# Step 3: Deduplicate FAERS data (merge all quarters, keep latest case version).
# Input:  RDS files from step2 (cleaned content) under data/processed/step2_clean_content.
# Output: Deduplicated domain tables under data/processed/step3_dedup:
#         - demo_dedup.rds (one row per case)
#         - valid_cases.rds (caseid/primaryid to keep)
#         - drug/reac/outc/indi/ther/rpsr deduped & aligned to valid cases.

suppressPackageStartupMessages({
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required. Install with install.packages('dplyr').")
  }
  if (!requireNamespace("purrr", quietly = TRUE)) {
    stop("Package 'purrr' is required. Install with install.packages('purrr').")
  }
  if (!requireNamespace("stringr", quietly = TRUE)) {
    stop("Package 'stringr' is required. Install with install.packages('stringr').")
  }
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package 'DBI' is required. Install with install.packages('DBI').")
  }
  if (!requireNamespace("RSQLite", quietly = TRUE)) {
    stop("Package 'RSQLite' is required. Install with install.packages('RSQLite').")
  }
  has_dt <- requireNamespace("data.table", quietly = TRUE)
})

library(dplyr)
library(purrr)
library(stringr)
library(DBI)
library(RSQLite)

in_dir <- file.path("data", "processed", "step2_clean_content")
out_dir <- file.path("data", "processed", "step3_dedup")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
db_path <- file.path("data", "faers.sqlite")
dir.create(dirname(db_path), recursive = TRUE, showWarnings = FALSE)

domains <- c("demo", "drug", "reac", "outc", "indi", "ther", "rpsr")

# Extract quarter tag from path (e.g., faers_ascii_2018Q1 -> 2018Q1)
extract_quarter <- function(path) {
  m <- stringr::str_match(path, "faers_ascii_([0-9]{4}Q[1-4])")[, 2]
  ifelse(is.na(m), NA_character_, m)
}

find_domain_files <- function(domain, root = in_dir, years = 2019:2021) {
  all <- list.files(root, pattern = paste0(domain, ".*\\.rds$"), recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
  year_tag <- paste0(years, "Q[1-4]")
  keep_pattern <- paste(year_tag, collapse = "|")
  all[stringr::str_detect(all, keep_pattern)]
}

read_domain <- function(domain) {
  files <- find_domain_files(domain)
  if (length(files) == 0) return(NULL)
  purrr::map_dfr(files, function(f) {
    df <- readRDS(f)
    df$source_quarter <- extract_quarter(f)
    # Soften type conflicts across quarters: coerce non-character to character before binding
    df[] <- lapply(df, function(col) if (is.character(col)) col else as.character(col))
    df
  })
}

# Deduplicate DEMO-like table: keep latest case_version per caseid
dedup_demo <- function(df) {
  if (!"caseid" %in% names(df)) {
    warning("No caseid column found in DEMO; skipping dedup on caseid.")
    return(df)
  }

  if (!"case_version" %in% names(df)) {
    df$case_version <- NA_integer_
  }

  if (has_dt) {
    dt <- data.table::as.data.table(df)
    dt[, case_version := suppressWarnings(as.integer(case_version))]
    if ("fda_dt" %in% names(dt)) dt[, fda_dt := as.Date(fda_dt)]
    if ("rept_dt" %in% names(dt)) dt[, rept_dt := as.Date(rept_dt)]
    data.table::setorder(dt, caseid, -case_version, -fda_dt, -rept_dt, -primaryid)
    dt <- dt[!duplicated(caseid)]
    out <- as.data.frame(dt)
  } else {
    out <- df %>%
      mutate(
        case_version = suppressWarnings(as.integer(case_version)),
        fda_dt = if ("fda_dt" %in% names(df)) as.Date(fda_dt) else as.Date(NA),
        rept_dt = if ("rept_dt" %in% names(df)) as.Date(rept_dt) else as.Date(NA)
      ) %>%
      arrange(caseid, desc(case_version), desc(fda_dt), desc(rept_dt), desc(primaryid)) %>%
      group_by(caseid) %>%
      slice(1) %>%
      ungroup()
  }
  out
}

# Filter a domain table to valid cases, then drop fully duplicated rows
filter_and_dedup_domain <- function(df, valid_cases) {
  if (is.null(df) || nrow(df) == 0) return(df)

  # Match on caseid if present, else fall back to primaryid
  if ("caseid" %in% names(df) && "caseid" %in% names(valid_cases)) {
    df <- df %>% semi_join(valid_cases %>% select(caseid), by = "caseid")
  } else if ("primaryid" %in% names(df) && "primaryid" %in% names(valid_cases)) {
    df <- df %>% semi_join(valid_cases %>% select(primaryid), by = "primaryid")
  }

  df %>% distinct()
}

run <- function() {
  message("Reading DEMO to build case list (", in_dir, ")")
  demo_files <- find_domain_files("demo")
  if (length(demo_files) == 0) stop("No DEMO files found; cannot deduplicate cases.")

  # Pass 1: read only key columns to decide latest version per case
  read_keys <- function(f) {
    message("  [demo-keys] reading ", f)
    df <- readRDS(f)
    df$source_quarter <- extract_quarter(f)
    # Keep only key columns for deciding latest version
    keep <- intersect(names(df), c("caseid", "case_version", "fda_dt", "rept_dt", "primaryid", "source_quarter"))
    df <- df[, keep, drop = FALSE]
    df[] <- lapply(df, function(col) if (is.character(col)) col else as.character(col))
    df
  }

  if (has_dt) {
    demo_keys <- data.table::rbindlist(lapply(demo_files, read_keys), fill = TRUE)
    demo_keys <- as.data.frame(demo_keys)
  } else {
    demo_keys <- purrr::map_dfr(demo_files, read_keys)
  }

  message("Deduplicating DEMO keys (case-level)...")
  demo_key_dedup <- dedup_demo(demo_keys)
  message("  DEMO key dedup complete. Rows: ", nrow(demo_key_dedup))

  valid_cases <- demo_key_dedup %>% select(any_of(c("caseid", "primaryid", "case_version")))
  saveRDS(valid_cases, file.path(out_dir, "valid_cases.rds"))
  message("  Saved valid_cases.rds")

  # Pass 2: build full deduped DEMO by matching caseid (+case_version if available)
  message("Building full deduped DEMO...")
  match_case <- function(df) {
    if ("case_version" %in% names(df) && "case_version" %in% names(valid_cases)) {
      dplyr::semi_join(df, valid_cases, by = c("caseid", "case_version"))
    } else {
      dplyr::semi_join(df, valid_cases, by = "caseid")
    }
  }

  read_full_demo <- function(f) {
    message("  [demo-full] reading ", f)
    df <- readRDS(f)
    df$source_quarter <- extract_quarter(f)
    df[] <- lapply(df, function(col) if (is.character(col)) col else as.character(col))
    match_case(df)
  }

  if (has_dt) {
    demo_full <- data.table::rbindlist(lapply(demo_files, read_full_demo), fill = TRUE)
    demo_full <- as.data.frame(demo_full)
  } else {
    demo_full <- purrr::map_dfr(demo_files, read_full_demo)
  }

  # If duplicates remain, keep distinct
  demo_dedup <- demo_full %>% distinct()
  message("  DEMO dedup complete. Rows: ", nrow(demo_dedup))
  saveRDS(demo_dedup, file.path(out_dir, "demo_dedup.rds"))
  message("  Saved demo_dedup.rds")

  # Open SQLite connection
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Write demo to DB
  message("Writing DEMO to SQLite: ", db_path)
  DBI::dbExecute(con, "DROP TABLE IF EXISTS demo;")
  DBI::dbWriteTable(con, "demo", demo_dedup, overwrite = TRUE)
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_demo_caseid ON demo(caseid);")
  if ("primaryid" %in% names(demo_dedup)) {
    DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_demo_primaryid ON demo(primaryid);")
  }

  # Prepare vectors for fast filtering
  caseid_vec <- if ("caseid" %in% names(valid_cases)) unique(valid_cases$caseid) else character(0)
  primaryid_vec <- if ("primaryid" %in% names(valid_cases)) unique(valid_cases$primaryid) else character(0)

  # Helper to ensure consistent columns across files for a domain
  gather_columns <- function(files) {
    cols <- character(0)
    for (f in files) {
      nms <- names(readRDS(f))
      cols <- union(cols, nms)
    }
    cols
  }

  process_domain <- function(dom) {
    files <- find_domain_files(dom)
    if (length(files) == 0) {
      message("No files for domain: ", dom)
      return(NULL)
    }
    message("Processing domain: ", dom, " (", length(files), " files)")

    all_cols <- gather_columns(files)
    all_cols <- unique(c(all_cols, "source_quarter"))
    raw_table <- paste0(dom, "_raw")
    DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s;", raw_table))

    read_filter <- function(f) {
      message("  [", dom, "] reading ", f)
      df <- readRDS(f)
      df$source_quarter <- extract_quarter(f)
      df[] <- lapply(df, function(col) if (is.character(col)) col else as.character(col))

      if ("caseid" %in% names(df) && length(caseid_vec) > 0) {
        df <- df[df$caseid %in% caseid_vec, , drop = FALSE]
      } else if ("primaryid" %in% names(df) && length(primaryid_vec) > 0) {
        df <- df[df$primaryid %in% primaryid_vec, , drop = FALSE]
      }

      # Align columns to union
      missing <- setdiff(all_cols, names(df))
      if (length(missing) > 0) {
        df[missing] <- NA_character_
      }
      extra <- setdiff(names(df), all_cols)
      if (length(extra) > 0) df <- df[setdiff(names(df), extra)]
      df <- df[all_cols]

      df
    }

    # Stream each file into SQLite raw table
    first <- TRUE
    for (f in files) {
      df_chunk <- read_filter(f)
      df_chunk <- dplyr::distinct(df_chunk)
      if (first) {
        DBI::dbWriteTable(con, raw_table, df_chunk, overwrite = TRUE)
        first <- FALSE
      } else {
        DBI::dbWriteTable(con, raw_table, df_chunk, append = TRUE)
      }
    }

    # Deduplicate inside SQLite
    DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s;", dom))
    DBI::dbExecute(con, sprintf("CREATE TABLE %s AS SELECT DISTINCT * FROM %s;", dom, raw_table))
    DBI::dbExecute(con, sprintf("DROP TABLE IF EXISTS %s;", raw_table))

    # Indexes for faster joins
    if ("caseid" %in% all_cols) {
      DBI::dbExecute(con, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_caseid ON %s(caseid);", dom, dom))
    }
    if ("primaryid" %in% all_cols) {
      DBI::dbExecute(con, sprintf("CREATE INDEX IF NOT EXISTS idx_%s_primaryid ON %s(primaryid);", dom, dom))
    }

    message("  [", dom, "] done and stored in SQLite.")
  }

  purrr::walk(setdiff(domains, "demo"), process_domain)
  message("Done. Deduplicated outputs are under: ", out_dir)
  message("SQLite database written at: ", db_path)
}

if (sys.nframe() == 0) {
  run()
}
