# Step 5: Wide-screen target drug extraction in SQLite (2019–2021 clean tables).
# Uses stems/brand variants to flag potential matches in drug_clean and writes:
#   - drug_wide: matched drug records with generic/atc/class/pattern fields
#   - target_cases: distinct caseid/primaryid for matched drugs

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

drugs <- list(
  list(generic = "metformin", atc = "A10BA02", class = "Biguanide",
       patterns = c("metform", "glucoph", "fortamet", "glumetza", "riomet")),

  list(generic = "semaglutide", atc = "A10BJ06", class = "GLP1",
       patterns = c("semaglut", "ozemp", "rybels", "wegov")),
  list(generic = "liraglutide", atc = "A10BJ02", class = "GLP1",
       patterns = c("liraglut", "victoz", "saxend")),
  list(generic = "dulaglutide", atc = "A10BJ05", class = "GLP1",
       patterns = c("dulaglut", "trulic")),
  list(generic = "exenatide", atc = "A10BJ01", class = "GLP1",
        patterns = c("exenat", "byett", "bydure")),

  list(generic = "tirzepatide", atc = "A10BX23", class = "GIP_GLP1",
       patterns = c("tirzep", "mounjar", "zepbou", "zepbound")),

  list(generic = "empagliflozin", atc = "A10BK03", class = "SGLT2",
       patterns = c("empag", "jardi")),
  list(generic = "dapagliflozin", atc = "A10BK01", class = "SGLT2",
       patterns = c("dapag", "farx")),
  list(generic = "canagliflozin", atc = "A10BK02", class = "SGLT2",
       patterns = c("canag", "invok")),
  list(generic = "ertugliflozin", atc = "A10BK04", class = "SGLT2",
       patterns = c("ertug", "stegla")),
  # Class-level catch for any flozin spellings (do not map to a specific molecule)
  list(generic = "sglt2_class", atc = "A10BK", class = "SGLT2",
       patterns = c("flozin")),

  list(generic = "sitagliptin", atc = "A10BH01", class = "DPP4",
       patterns = c("sitaglipt", "januv")),
  list(generic = "saxagliptin", atc = "A10BH03", class = "DPP4",
       patterns = c("saxaglipt", "ongly")),
  list(generic = "linagliptin", atc = "A10BH05", class = "DPP4",
       patterns = c("linag", "linaglipt", "tradj")),
  list(generic = "alogliptin", atc = "A10BH04", class = "DPP4",
       patterns = c("aloglipt", "nesin", "nesina")),
  # Class-level catch for gliptin suffix (do not map to a specific molecule)
  list(generic = "dpp4_class", atc = "A10BH", class = "DPP4",
       patterns = c("gliptin")),

  list(generic = "insulin_glargine", atc = "A10AE04", class = "BasalInsulin",
       patterns = c("glarg", "lant", "tauj", "basag")),
  list(generic = "insulin_degludec", atc = "A10AE06", class = "BasalInsulin",
       patterns = c("deglud", "deglu", "tresib"))
)

build_union_query <- function() {
  parts <- lapply(drugs, function(d) {
    esc <- gsub("'", "''", d$patterns)
    pat_sql <- paste(sprintf("lower(drugname) LIKE '%%%s%%'", esc), collapse = " OR ")
    sprintf("
      SELECT
        d.*,
        lower(d.drugname) AS drugname_lc,
        '%s' AS generic_name,
        '%s' AS atc_code,
        '%s' AS mech_class
      FROM drug_clean d
      WHERE (%s)
    ", d$generic, d$atc, d$class, pat_sql)
  })
  paste(parts, collapse = " UNION ALL ")
}

run <- function() {
  if (!file.exists(db_path)) stop("Database not found at ", db_path)
  con <- dbConnect(SQLite(), db_path)
  on.exit(dbDisconnect(con), add = TRUE)

  message("Dropping old wide-screen tables if any...")
  dbExecute(con, "DROP TABLE IF EXISTS drug_wide;")
  dbExecute(con, "DROP TABLE IF EXISTS target_cases;")

  message("Building drug_wide with keyword stems...")
  union_sql <- build_union_query()
  dbExecute(con, sprintf("CREATE TABLE drug_wide AS %s;", union_sql))

  message("Deduplicating drug_wide...")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_drug_wide_caseid ON drug_wide(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_drug_wide_primaryid ON drug_wide(primaryid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_drug_wide_generic ON drug_wide(generic_name);")

  message("Building target_cases from drug_wide...")
  dbExecute(con, "
    CREATE TABLE target_cases AS
    SELECT DISTINCT caseid, primaryid FROM drug_wide
  ;")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_target_cases_caseid ON target_cases(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_target_cases_primaryid ON target_cases(primaryid);")

  message("Done. Tables created: drug_wide, target_cases")
}

if (sys.nframe() == 0) {
  run()
}
