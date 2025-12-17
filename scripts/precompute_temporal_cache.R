# Precompute temporal forecasts locally and save as an RDS cache.
# Run from repo root:
#   FAERS_DB_PATH=/path/to/faers.sqlite \
#   Rscript scripts/precompute_temporal_cache.R
# Optional: TEMPORAL_CACHE_OUT=/path/to/output.rds (default: data/temporal_cache.rds)

if (!requireNamespace("DBI", quietly = TRUE) ||
    !requireNamespace("RSQLite", quietly = TRUE) ||
    !requireNamespace("dplyr", quietly = TRUE) ||
    !requireNamespace("tibble", quietly = TRUE)) {
  stop("Please install DBI, RSQLite, dplyr, tibble before running.")
}

pkgs_needed <- c("forecast", "xgboost", "randomForest", "glmnet")
missing_pkgs <- pkgs_needed[!vapply(pkgs_needed, requireNamespace, logical(1), quietly = TRUE)]
if (length(missing_pkgs)) {
  stop("Install required packages: ", paste(missing_pkgs, collapse = ", "))
}

db_path <- Sys.getenv("FAERS_DB_PATH", unset = "data/faers.sqlite")
out_path <- Sys.getenv("TEMPORAL_CACHE_OUT", unset = "data/temporal_cache.rds")

if (!file.exists(db_path)) stop("Database not found at ", db_path)

con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
on.exit(DBI::dbDisconnect(con), add = TRUE)

next_yq <- function(last_year, last_quarter, h = 4) {
  steps <- seq_len(h)
  tibble::tibble(
    year    = last_year + (last_quarter + steps - 1) %/% 4,
    quarter = ((last_quarter + steps - 1) %% 4) + 1
  ) |>
    dplyr::mutate(year_quarter = paste0(year, "Q", quarter))
}

is_flat <- function(x, tol = 1e-6) {
  x <- as.numeric(x)
  if (length(x) <= 1 || any(!is.finite(x))) return(FALSE)
  rng <- max(x) - min(x)
  mean_abs <- mean(abs(x), na.rm = TRUE)
  rng < tol * max(1, mean_abs)
}

get_top10_drugs <- function(con) {
  sql <- "
    SELECT target_generic, COUNT(*) AS n
    FROM cohort_analytic
    GROUP BY target_generic
    ORDER BY n DESC
    LIMIT 10
  "
  df <- DBI::dbGetQuery(con, sql)
  if (nrow(df) == 0) return(character(0))
  df$target_generic
}

build_ts_and_forecasts <- function(drug_name, con, h = 4) {
  sql <- "
    SELECT source_quarter, COUNT(*) AS n
    FROM cohort_analytic
    WHERE target_generic = ?
    GROUP BY source_quarter
    ORDER BY source_quarter
  "
  df <- DBI::dbGetQuery(con, sql, params = list(drug_name))
  if (nrow(df) < 4) {
    return(list(ok = FALSE, reason = "Not enough quarters for modeling", observed = df, combined = NULL, fc_table = NULL))
  }

  df <- df |>
    dplyr::mutate(
      year    = as.integer(substr(source_quarter, 1, 4)),
      quarter = as.integer(substr(source_quarter, 6, 6))
    ) |>
    dplyr::arrange(year, quarter)

  y <- df$n
  yq_fc <- next_yq(tail(df$year, 1), tail(df$quarter, 1), h = h) |>
    dplyr::mutate(
      t        = max(seq_len(nrow(df))) + dplyr::row_number(),
      q_factor = as.integer(factor(quarter, levels = 1:4))
    )

  df_feats <- df |>
    dplyr::mutate(
      t        = dplyr::row_number(),
      q_factor = as.integer(factor(quarter, levels = 1:4))
    )
  X_train <- as.matrix(df_feats[, c("t", "q_factor")])
  y_train <- df_feats$n
  X_future <- as.matrix(yq_fc[, c("t", "q_factor")])

  df_obs <- df |>
    dplyr::transmute(
      year, quarter,
      year_quarter = source_quarter,
      kind = "Observed",
      model = "Observed",
      value = n
    )

  df_fc_arima <- df_fc_xgb <- df_fc_rf <- df_fc_en <- NULL

  # ARIMA (log1p)
  ts_y <- stats::ts(log1p(y), frequency = 4)
  fit_arima <- try(forecast::auto.arima(ts_y, stepwise = FALSE, approximation = FALSE), silent = TRUE)
  if (!inherits(fit_arima, "try-error")) {
    fc <- forecast::forecast(fit_arima, h = h)
    pred <- pmax(0, expm1(as.numeric(fc$mean)))
    if (!is_flat(pred)) {
      df_fc_arima <- tibble::tibble(
        year         = yq_fc$year,
        quarter      = yq_fc$quarter,
        year_quarter = yq_fc$year_quarter,
        kind         = "Forecast",
        model        = "ARIMA",
        value        = pred
      )
    }
  }

  # XGBoost
  dtrain <- xgboost::xgb.DMatrix(data = X_train, label = y_train)
  params <- list(objective = "reg:squarederror", max_depth = 3, eta = 0.2, subsample = 0.8, nthread = 1)
  fit_xgb <- try(
    xgboost::xgb.train(params = params, data = dtrain, nrounds = 80, verbose = 0),
    silent = TRUE
  )
  if (!inherits(fit_xgb, "try-error")) {
    pred_xgb <- try(predict(fit_xgb, newdata = X_future), silent = TRUE)
    if (!inherits(pred_xgb, "try-error") && !is_flat(pred_xgb)) {
      df_fc_xgb <- tibble::tibble(
        year         = yq_fc$year,
        quarter      = yq_fc$quarter,
        year_quarter = yq_fc$year_quarter,
        kind         = "Forecast",
        model        = "XGBoost",
        value        = as.numeric(pred_xgb)
      )
    }
  }

  # Random Forest
  df_rf_train <- data.frame(y = y_train, t = df_feats$t, q_factor = factor(df_feats$q_factor))
  fit_rf <- try(randomForest::randomForest(y ~ t + q_factor, data = df_rf_train), silent = TRUE)
  if (!inherits(fit_rf, "try-error")) {
    df_rf_future <- data.frame(t = yq_fc$t, q_factor = factor(yq_fc$q_factor, levels = levels(df_rf_train$q_factor)))
    pred_rf <- try(predict(fit_rf, newdata = df_rf_future), silent = TRUE)
    if (!inherits(pred_rf, "try-error") && !is_flat(pred_rf)) {
      df_fc_rf <- tibble::tibble(
        year         = yq_fc$year,
        quarter      = yq_fc$quarter,
        year_quarter = yq_fc$year_quarter,
        kind         = "Forecast",
        model        = "RF",
        value        = as.numeric(pred_rf)
      )
    }
  }

  # Elastic Net
  fit_en <- try(glmnet::cv.glmnet(X_train, y_train, alpha = 0.5), silent = TRUE)
  if (!inherits(fit_en, "try-error")) {
    pred_en <- try(as.numeric(stats::predict(fit_en, newx = X_future, s = "lambda.min")), silent = TRUE)
    if (!inherits(pred_en, "try-error") && !is_flat(pred_en)) {
      df_fc_en <- tibble::tibble(
        year         = yq_fc$year,
        quarter      = yq_fc$quarter,
        year_quarter = yq_fc$year_quarter,
        kind         = "Forecast",
        model        = "ENet",
        value        = pred_en
      )
    }
  }

  df_fc_all <- dplyr::bind_rows(df_fc_arima, df_fc_xgb, df_fc_rf, df_fc_en)
  df_all    <- dplyr::bind_rows(df_obs, df_fc_all)

  fc_table <- NULL
  if (!is.null(df_fc_all) && nrow(df_fc_all) > 0) {
    fc_table <- df_fc_all |>
      dplyr::select(year_quarter, model, value) |>
      tidyr::pivot_wider(names_from = model, values_from = value) |>
      dplyr::arrange(year_quarter)
  }

  list(ok = TRUE, observed = df, combined = df_all, fc_table = fc_table)
}

message("Querying top drugs...")
drugs <- get_top10_drugs(con)
if (!length(drugs)) stop("No drugs found.")

cache <- list()
for (drug in drugs) {
  message("Computing forecasts for: ", drug)
  cache[[drug]] <- build_ts_and_forecasts(drug, con, h = 4)
}

dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
saveRDS(cache, out_path)
message("Saved cache to ", out_path)
