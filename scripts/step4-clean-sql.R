# Step 4: In-database cleaning of anomalies on top of deduped SQLite (2019–2021).
# This avoids rerunning Step2/Step3; creates *_clean tables in data/faers.sqlite.

suppressPackageStartupMessages({
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package 'DBI' is required. Install with install.packages('DBI').")
  }
  if (!requireNamespace("RSQLite", quietly = TRUE)) {
    stop("Package 'RSQLite' is required. Install with install.packages('RSQLite').")
  }
})

library(DBI)
library(RSQLite)

db_path <- file.path("data", "faers.sqlite")

has_col <- function(con, tbl, col) {
  tolower(col) %in% tolower(DBI::dbListFields(con, tbl))
}

safe_update_date <- function(con, tbl, col, today) {
  if (has_col(con, tbl, col)) {
    DBI::dbExecute(con, sprintf("UPDATE %s SET %s = NULL WHERE %s < '1900-01-01' OR %s > '%s';", tbl, col, col, col, today))
  }
}

safe_update_numeric_bounds <- function(con, tbl, col, min_val, max_val) {
  if (has_col(con, tbl, col)) {
    DBI::dbExecute(con, sprintf("UPDATE %s SET %s = NULL WHERE CAST(%s AS REAL) < %s OR CAST(%s AS REAL) > %s;", tbl, col, col, min_val, col, max_val))
  }
}

create_index_if <- function(con, tbl, col, idx_name) {
  if (has_col(con, tbl, col)) {
    DBI::dbExecute(con, sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(%s);", idx_name, tbl, col))
  }
}

run <- function() {
  if (!file.exists(db_path)) stop("Database not found at ", db_path)
  con <- dbConnect(SQLite(), db_path)
  on.exit(dbDisconnect(con), add = TRUE)

  today <- format(Sys.Date(), "%Y-%m-%d")

  message("Cleaning DEMO -> demo_clean")
  dbExecute(con, "DROP TABLE IF EXISTS demo_clean;")
  dbExecute(con, "CREATE TABLE demo_clean AS SELECT * FROM demo;")
  for (col in c("event_dt", "rept_dt", "init_fda_dt", "fda_dt", "mfr_dt", "birth_dt", "death_dt")) {
    safe_update_date(con, "demo_clean", col, today)
  }
  if (has_col(con, "demo_clean", "event_dt") && has_col(con, "demo_clean", "death_dt")) {
    dbExecute(con, "UPDATE demo_clean SET death_dt = NULL WHERE event_dt IS NOT NULL AND death_dt IS NOT NULL AND event_dt > death_dt;")
  }
  safe_update_numeric_bounds(con, "demo_clean", "age", 0, 120)
  safe_update_numeric_bounds(con, "demo_clean", "age_yr", 0, 120)
  safe_update_numeric_bounds(con, "demo_clean", "wt", 0, 300)
  create_index_if(con, "demo_clean", "caseid", "idx_demo_clean_caseid")
  create_index_if(con, "demo_clean", "primaryid", "idx_demo_clean_primaryid")

  message("Cleaning DRUG -> drug_clean (drop empty drugname)")
  dbExecute(con, "DROP TABLE IF EXISTS drug_clean;")
  if (has_col(con, "drug", "drugname")) {
    dbExecute(con, "
      CREATE TABLE drug_clean AS
      SELECT *
      FROM drug
      WHERE TRIM(COALESCE(drugname,'')) <> ''
    ;")
  } else {
    dbExecute(con, "CREATE TABLE drug_clean AS SELECT * FROM drug;")
  }
  create_index_if(con, "drug_clean", "caseid", "idx_drug_clean_caseid")
  create_index_if(con, "drug_clean", "primaryid", "idx_drug_clean_primaryid")

  message("Cleaning REAC -> reac_clean")
  dbExecute(con, "DROP TABLE IF EXISTS reac_clean;")
  dbExecute(con, "CREATE TABLE reac_clean AS SELECT * FROM reac;")
  create_index_if(con, "reac_clean", "caseid", "idx_reac_clean_caseid")
  create_index_if(con, "reac_clean", "primaryid", "idx_reac_clean_primaryid")

  message("Cleaning OUTC -> outc_clean")
  dbExecute(con, "DROP TABLE IF EXISTS outc_clean;")
  dbExecute(con, "CREATE TABLE outc_clean AS SELECT * FROM outc;")
  create_index_if(con, "outc_clean", "caseid", "idx_outc_clean_caseid")
  create_index_if(con, "outc_clean", "primaryid", "idx_outc_clean_primaryid")

  message("Cleaning INDI -> indi_clean")
  dbExecute(con, "DROP TABLE IF EXISTS indi_clean;")
  dbExecute(con, "CREATE TABLE indi_clean AS SELECT * FROM indi;")
  create_index_if(con, "indi_clean", "caseid", "idx_indi_clean_caseid")
  create_index_if(con, "indi_clean", "primaryid", "idx_indi_clean_primaryid")

  message("Cleaning THER -> ther_clean (date bounds)")
  dbExecute(con, "DROP TABLE IF EXISTS ther_clean;")
  dbExecute(con, "CREATE TABLE ther_clean AS SELECT * FROM ther;")
  for (col in c("start_dt", "end_dt")) {
    safe_update_date(con, "ther_clean", col, today)
  }
  create_index_if(con, "ther_clean", "caseid", "idx_ther_clean_caseid")
  create_index_if(con, "ther_clean", "primaryid", "idx_ther_clean_primaryid")

  message("Cleaning RPSR -> rpsr_clean")
  dbExecute(con, "DROP TABLE IF EXISTS rpsr_clean;")
  dbExecute(con, "CREATE TABLE rpsr_clean AS SELECT * FROM rpsr;")
  create_index_if(con, "rpsr_clean", "caseid", "idx_rpsr_clean_caseid")
  create_index_if(con, "rpsr_clean", "primaryid", "idx_rpsr_clean_primaryid")

  message("Done. Clean tables: demo_clean, drug_clean, reac_clean, outc_clean, indi_clean, ther_clean, rpsr_clean")
}

if (sys.nframe() == 0) {
  run()
}
