# Step 7: Final filter to target T2DM drugs (PS only).
# - Input: data/faers.sqlite with drug_normalized from Step 6
# - Output: drug_target_ps (only target generics, role PS), target_cases_ps

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

# 16 target T2DM drugs (generic)
target_generics <- c(
  "metformin",
  "semaglutide", "liraglutide", "dulaglutide", "exenatide", "tirzepatide",
  "empagliflozin", "dapagliflozin", "canagliflozin", "ertugliflozin",
  "sitagliptin", "saxagliptin", "linagliptin", "alogliptin",
  "insulin_glargine", "insulin_degludec"
)

run <- function() {
  if (!file.exists(db_path)) stop("Database not found at ", db_path)
  con <- dbConnect(SQLite(), db_path)
  on.exit(dbDisconnect(con), add = TRUE)

  message("Dropping old target PS tables if any...")
  dbExecute(con, "DROP TABLE IF EXISTS drug_target_ps;")
  dbExecute(con, "DROP TABLE IF EXISTS target_cases_ps;")

  tg_sql <- paste(sprintf("'%s'", target_generics), collapse = ",")
  message("Filtering drug_normalized for target generics and role PS...")
  dbExecute(con, sprintf("
    CREATE TABLE drug_target_ps AS
    SELECT *
    FROM drug_normalized
    WHERE lower(target_generic) IN (%s)
      AND upper(role_cod) = 'PS'
  ;", tg_sql))

  message("Indexing drug_target_ps...")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_drug_target_ps_caseid ON drug_target_ps(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_drug_target_ps_primaryid ON drug_target_ps(primaryid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_drug_target_ps_target ON drug_target_ps(target_generic);")

  message("Building target_cases_ps...")
  dbExecute(con, "
    CREATE TABLE target_cases_ps AS
    SELECT DISTINCT caseid, primaryid FROM drug_target_ps
  ;")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_target_cases_ps_caseid ON target_cases_ps(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_target_cases_ps_primaryid ON target_cases_ps(primaryid);")

  message("Done. Tables created: drug_target_ps, target_cases_ps")
}

if (sys.nframe() == 0) {
  run()
}
