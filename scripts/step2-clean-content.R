# Step 2: Content/type cleaning for FAERS tables (2018–2024).
# Input:  RDS files from step1 (column names already standardized).
# Output: Cleaned RDS files under data/processed/step2_clean_content,
#         with trimmed strings, normalized dates/numerics, and recoded categoricals.

suppressPackageStartupMessages({
  if (!requireNamespace("stringr", quietly = TRUE)) {
    stop("Package 'stringr' is required. Install with install.packages('stringr').")
  }
  if (!requireNamespace("lubridate", quietly = TRUE)) {
    stop("Package 'lubridate' is required. Install with install.packages('lubridate').")
  }
})

library(stringr)
library(lubridate)

in_dir <- file.path("data", "processed", "step1_clean_names")
out_dir <- file.path("data", "processed", "step2_clean_content")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# --- helpers -----------------------------------------------------------------

trim_and_squish_chars <- function(df) {
  char_cols <- names(df)[vapply(df, is.character, logical(1))]
  if (length(char_cols) == 0) return(df)
  df[char_cols] <- lapply(df[char_cols], function(x) {
    x <- stringr::str_trim(x)
    stringr::str_squish(x)
  })
  df
}

lowercase_fields <- function(df, fields) {
  keep <- intersect(fields, names(df))
  if (length(keep) == 0) return(df)
  df[keep] <- lapply(df[keep], tolower)
  df
}

parse_dates <- function(x) {
  # Most FAERS dates are YYYYMMDD; fallback tries other formats.
  x <- stringr::str_trim(x)
  x[x == ""] <- NA_character_
  dt <- suppressWarnings(lubridate::ymd(x))
  # Bound dates to sensible window
  bad <- dt < as.Date("1900-01-01") | dt > Sys.Date()
  dt[bad] <- NA
  dt
}

coerce_numeric <- function(x, min_val = -Inf, max_val = Inf) {
  x <- stringr::str_replace_all(x, "[^0-9.+-]", "")
  out <- suppressWarnings(as.numeric(x))
  out[out < min_val | out > max_val] <- NA_real_
  out
}

recode_sex <- function(x) {
  x <- toupper(stringr::str_trim(x))
  dplyr::case_when(
    x %in% c("M", "MALE", "MAN") ~ "Male",
    x %in% c("F", "FEMALE", "WOMAN") ~ "Female",
    TRUE ~ "Unknown"
  )
}

standardize_categoricals <- function(df) {
  if ("sex" %in% names(df)) df$sex <- recode_sex(df$sex)
  for (col in c("role_cod", "outc_cod", "age_cod", "wt_cod", "age_grp")) {
    if (col %in% names(df)) df[[col]] <- toupper(df[[col]])
  }
  df
}

clean_dates_by_table <- function(df, table_hint) {
  date_cols <- switch(
    table_hint,
    demo = c("event_dt", "rept_dt", "init_fda_dt", "fda_dt", "mfr_dt", "birth_dt", "death_dt"),
    ther = c("start_dt", "end_dt", "dur"),
    drug = c("start_dt", "end_dt"),
    reac = character(0),
    outc = character(0),
    indi = character(0),
    rpsr = character(0),
    character(0)
  )
  date_cols <- intersect(date_cols, names(df))
  for (col in date_cols) df[[col]] <- parse_dates(df[[col]])
  df
}

clean_numeric_by_table <- function(df, table_hint) {
  # Age and weight are the main numeric fields we bound.
  num_specs <- list(
    demo = list(
      age = c(0, 120),
      age_yr = c(0, 120),
      wt = c(0, 300)
    ),
    ther = list(),
    drug = list(dose_amt = c(0, Inf)),
    reac = list(),
    outc = list(),
    indi = list(),
    rpsr = list()
  )

  specs <- num_specs[[table_hint]]
  if (is.null(specs)) specs <- list()

  for (col in names(specs)) {
    if (col %in% names(df)) {
      bounds <- specs[[col]]
      df[[col]] <- coerce_numeric(df[[col]], min_val = bounds[1], max_val = bounds[2])
    }
  }
  df
}

dedupe_cases <- function(df) {
  # Keep latest case_version per caseid if both exist; otherwise return as-is.
  if (all(c("caseid", "case_version") %in% names(df))) {
    ord <- order(df$caseid, df$case_version, na.last = TRUE, decreasing = TRUE)
    df <- df[ord, ]
    df <- df[!duplicated(df$caseid), ]
  }
  df
}

infer_table_hint <- function(path) {
  nm <- tolower(basename(path))
  # Use file name to guess table type.
  if (grepl("demo", nm)) return("demo")
  if (grepl("drug", nm)) return("drug")
  if (grepl("reac", nm)) return("reac")
  if (grepl("outc", nm)) return("outc")
  if (grepl("indi", nm)) return("indi")
  if (grepl("ther", nm)) return("ther")
  if (grepl("rpsr", nm)) return("rpsr")
  "other"
}

# --- pipeline ----------------------------------------------------------------

process_one <- function(path, root_in = in_dir, root_out = out_dir) {
  rel <- sub(paste0("^", normalizePath(root_in, mustWork = TRUE)), "", normalizePath(path, mustWork = TRUE))
  rel <- sub("^/", "", rel)
  out_path <- file.path(root_out, rel)

  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  message("Cleaning content: ", path)

  df <- readRDS(path)
  hint <- infer_table_hint(path)

  df <- trim_and_squish_chars(df)
  df <- lowercase_fields(df, fields = c("drugname", "prod_ai", "pt", "indi_pt"))
  df <- clean_dates_by_table(df, hint)
  df <- clean_numeric_by_table(df, hint)
  df <- standardize_categoricals(df)
  df <- dedupe_cases(df)  # idempotent; keeps latest version if columns exist

  saveRDS(df, out_path)
  invisible(out_path)
}

find_rds_files <- function(root = in_dir) {
  list.files(root, pattern = "\\.rds$", recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
}

run <- function() {
  files <- find_rds_files()
  message("Found ", length(files), " RDS files to clean (content/type).")
  lapply(files, process_one)
  message("Done. Cleaned files written to: ", out_dir)
}

if (sys.nframe() == 0) {
  run()
}
