# Step 10: Add ATC/mechanism classification to cohort_drug.
# - Input: data/faers.sqlite with cohort_drug
# - Output: drug_atc_mech (lookup) and cohort_drug_final with atc_code_final, mech_class_final

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

  message("Creating drug_atc_mech lookup ...")
  dbExecute(con, "DROP TABLE IF EXISTS drug_atc_mech;")
  dbExecute(con, "
    CREATE TABLE drug_atc_mech AS
    SELECT 'metformin'        AS target_generic, 'A10BA02' AS atc_code, 'Biguanide'     AS mech_class UNION ALL
    SELECT 'semaglutide',      'A10BJ06', 'GLP1' UNION ALL
    SELECT 'liraglutide',      'A10BJ02', 'GLP1' UNION ALL
    SELECT 'dulaglutide',      'A10BJ05', 'GLP1' UNION ALL
    SELECT 'exenatide',        'A10BJ01', 'GLP1' UNION ALL
    SELECT 'tirzepatide',      'A10BX23', 'GIP_GLP1' UNION ALL
    SELECT 'empagliflozin',    'A10BK03', 'SGLT2' UNION ALL
    SELECT 'dapagliflozin',    'A10BK01', 'SGLT2' UNION ALL
    SELECT 'canagliflozin',    'A10BK02', 'SGLT2' UNION ALL
    SELECT 'ertugliflozin',    'A10BK04', 'SGLT2' UNION ALL
    SELECT 'sitagliptin',      'A10BH01', 'DPP4' UNION ALL
    SELECT 'saxagliptin',      'A10BH03', 'DPP4' UNION ALL
    SELECT 'linagliptin',      'A10BH05', 'DPP4' UNION ALL
    SELECT 'alogliptin',       'A10BH04', 'DPP4' UNION ALL
    SELECT 'insulin_glargine', 'A10AE04', 'BasalInsulin' UNION ALL
    SELECT 'insulin_degludec', 'A10AE06', 'BasalInsulin'
  ;")
  dbExecute(con, "CREATE UNIQUE INDEX IF NOT EXISTS idx_drug_atc_mech_generic ON drug_atc_mech(target_generic);")

  message("Building cohort_drug_final ...")
  dbExecute(con, "DROP TABLE IF EXISTS cohort_drug_final;")
  dbExecute(con, "
    CREATE TABLE cohort_drug_final AS
    SELECT
      d.*,
      m.atc_code   AS atc_code_final,
      m.mech_class AS mech_class_final
    FROM cohort_drug d
    LEFT JOIN drug_atc_mech m
      ON d.target_generic = m.target_generic
  ;")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_drug_final_caseid ON cohort_drug_final(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_drug_final_primaryid ON cohort_drug_final(primaryid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_drug_final_target ON cohort_drug_final(target_generic);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cohort_drug_final_mech ON cohort_drug_final(mech_class_final);")

  message("Sanity check: unmatched target_generic ...")
  unmatched <- dbGetQuery(con, "
    SELECT target_generic, COUNT(*) AS n
    FROM cohort_drug_final
    WHERE mech_class_final IS NULL
    GROUP BY target_generic
  ;")
  if (nrow(unmatched)) {
    message("Unmatched entries:\n", paste(apply(unmatched, 1, paste, collapse = ": "), collapse = "\n"))
  } else {
    message("All target_generic matched to mechanism.")
  }

  message("Distribution by mech_class_final:")
  dist <- dbGetQuery(con, "
    SELECT mech_class_final, COUNT(*) AS n
    FROM cohort_drug_final
    GROUP BY mech_class_final
    ORDER BY n DESC
  ;")
  print(dist)

  message("Done. Tables created: drug_atc_mech, cohort_drug_final")
}

if (sys.nframe() == 0) run()
