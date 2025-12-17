# Step 11: Build analytic table by joining cohort_* tables.
# Output: cohort_analytic (one row per caseid/primaryid/drug/AE)

suppressPackageStartupMessages({
  if (!requireNamespace("DBI", quietly = TRUE)) stop("Package 'DBI' is required.")
  if (!requireNamespace("RSQLite", quietly = TRUE)) stop("Package 'RSQLite' is required.")
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
})

library(DBI)
library(RSQLite)
library(dplyr)

db_path <- file.path("data", "faers.sqlite")

run <- function() {
  if (!file.exists(db_path)) stop("Database not found at ", db_path)
  con <- dbConnect(SQLite(), db_path)
  on.exit(dbDisconnect(con), add = TRUE)

  idx  <- tbl(con, "cohort_index")
  dem  <- tbl(con, "cohort_demo") %>%
    select(caseid, primaryid, age, age_cod, sex, reporter_country, demo_source_quarter = source_quarter)
  drug <- tbl(con, "cohort_drug_final") %>%
    select(caseid, primaryid, target_generic, mech_class_final, atc_code_final, role_cod, drug_seq, drug_source_quarter = source_quarter)
  reac <- tbl(con, "cohort_reac") %>%
    select(caseid, primaryid, pt, pseudo_soc)
  outc <- tbl(con, "cohort_outc")

  message("Summarizing outcomes to flags ...")
  outc_wide <- outc %>%
    mutate(code = toupper(outc_cod)) %>%
    group_by(caseid, primaryid) %>%
    summarise(
      death = as.integer(max(code == "DE")),
      hosp = as.integer(max(code == "HO")),
      disability = as.integer(max(code == "DS")),
      lifethreat = as.integer(max(code == "LT")),
      other = as.integer(max(code == "OT")),
      congenital = as.integer(max(code == "CA")),
      required_intervention = as.integer(max(code == "RI")),
      .groups = "drop"
    )

  message("Joining cohort tables into cohort_analytic ...")
  analytic <- idx %>%
    left_join(dem,  by = c("caseid", "primaryid")) %>%
    left_join(drug, by = c("caseid", "primaryid")) %>%
    left_join(reac, by = c("caseid", "primaryid")) %>%
    left_join(outc_wide, by = c("caseid", "primaryid")) %>%
    mutate(source_quarter = coalesce(demo_source_quarter, drug_source_quarter)) %>%
    filter(!is.na(target_generic)) %>%
    select(
      caseid, primaryid,
      source_quarter,
      age, age_unit = age_cod, sex, reporter_country,
      target_generic, mech_class_final, atc_code_final,
      pt, pseudo_soc,
      death, hosp, disability, lifethreat, other, congenital, required_intervention
    )

  message("Writing cohort_analytic ...")
  dbExecute(con, "DROP TABLE IF EXISTS cohort_analytic;")
  dbWriteTable(con, "cohort_analytic", collect(analytic), overwrite = TRUE)
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_analytic_caseid ON cohort_analytic(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_analytic_primaryid ON cohort_analytic(primaryid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_analytic_drug ON cohort_analytic(target_generic);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_analytic_soc ON cohort_analytic(pseudo_soc);")

  message("Sanity checks:")
  total_n <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM cohort_analytic;")$n
  case_n  <- dbGetQuery(con, "SELECT COUNT(DISTINCT caseid) AS n FROM cohort_analytic;")$n
  soc_na  <- dbGetQuery(con, "SELECT COUNT(*) AS n FROM cohort_analytic WHERE pseudo_soc IS NULL;")$n
  message("Total rows: ", format(total_n, big.mark = ","))
  message("Distinct caseid: ", format(case_n, big.mark = ","))
  message("Rows with pseudo_soc = NULL: ", soc_na)
  message("Done. Table created: cohort_analytic")
}

if (sys.nframe() == 0) run()
