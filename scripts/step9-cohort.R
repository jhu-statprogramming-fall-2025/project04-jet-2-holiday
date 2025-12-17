# Step 9: Build case-level cohort tables in SQLite.
# - Input: data/faers.sqlite with drug_normalized, demo_clean, reac_clean, outc_clean, reac_pseudo_soc
# - Output: cohort_index, cohort_demo, cohort_drug, cohort_reac, cohort_outc

suppressPackageStartupMessages({
  if (!requireNamespace("DBI", quietly = TRUE)) stop("Package 'DBI' is required.")
  if (!requireNamespace("RSQLite", quietly = TRUE)) stop("Package 'RSQLite' is required.")
})

library(DBI)
library(RSQLite)

db_path <- file.path("data", "faers.sqlite")

run <- function() {
  if (!file.exists(db_path)) stop("Database not found at ", db_path)
  con <- dbConnect(SQLite(), db_path)
  on.exit(dbDisconnect(con), add = TRUE)

  message("Dropping existing cohort tables if any...")
  dbExecute(con, "DROP TABLE IF EXISTS cohort_index;")
  dbExecute(con, "DROP TABLE IF EXISTS cohort_demo;")
  dbExecute(con, "DROP TABLE IF EXISTS cohort_drug;")
  dbExecute(con, "DROP TABLE IF EXISTS cohort_reac;")
  dbExecute(con, "DROP TABLE IF EXISTS cohort_outc;")

  message("Building cohort_index from drug_normalized ...")
  dbExecute(con, "
    CREATE TABLE cohort_index AS
    SELECT DISTINCT caseid, primaryid
    FROM drug_normalized
  ;")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_index_caseid ON cohort_index(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_index_primaryid ON cohort_index(primaryid);")

  message("Building cohort_demo ...")
  dbExecute(con, "
    CREATE TABLE cohort_demo AS
    SELECT d.*
    FROM demo_clean d
    JOIN cohort_index c
      ON d.caseid = c.caseid AND d.primaryid = c.primaryid
  ;")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_demo_caseid ON cohort_demo(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_demo_primaryid ON cohort_demo(primaryid);")

  message("Building cohort_drug (target drugs) ...")
  dbExecute(con, "
    CREATE TABLE cohort_drug AS
    SELECT n.*
    FROM drug_normalized n
    JOIN cohort_index c
      ON n.caseid = c.caseid AND n.primaryid = c.primaryid
  ;")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_drug_caseid ON cohort_drug(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_drug_primaryid ON cohort_drug(primaryid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_drug_target ON cohort_drug(target_generic);")

  message("Building cohort_reac (PT + pseudo SOC) ...")
  dbExecute(con, "
    CREATE TABLE cohort_reac AS
    SELECT r.caseid, r.primaryid, r.pt, p.pseudo_soc
    FROM reac_clean r
    JOIN reac_pseudo_soc p
      ON r.caseid = p.caseid AND r.primaryid = p.primaryid
    JOIN cohort_index c
      ON r.caseid = c.caseid AND r.primaryid = c.primaryid
  ;")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_reac_caseid ON cohort_reac(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_reac_primaryid ON cohort_reac(primaryid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_reac_soc ON cohort_reac(pseudo_soc);")

  message("Building cohort_outc ...")
  dbExecute(con, "
    CREATE TABLE cohort_outc AS
    SELECT o.*
    FROM outc_clean o
    JOIN cohort_index c
      ON o.caseid = c.caseid AND o.primaryid = c.primaryid
  ;")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_outc_caseid ON cohort_outc(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_outc_primaryid ON cohort_outc(primaryid);")

  message("Done. Tables created: cohort_index, cohort_demo, cohort_drug, cohort_reac, cohort_outc")
}

if (sys.nframe() == 0) run()
