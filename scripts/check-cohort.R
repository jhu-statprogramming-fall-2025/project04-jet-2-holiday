#!/usr/bin/env Rscript
# Cohort QA checks (lightweight, DB-backed; no full data pull)

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(dplyr)
})

db_path <- "data/faers.sqlite"
if (!file.exists(db_path)) stop("Database not found: ", db_path)
con <- dbConnect(SQLite(), db_path)
on.exit(dbDisconnect(con), add = TRUE)

cat("=== 0. Required tables ===\n")
required_tables <- c(
  "cohort_index",
  "cohort_demo",
  "cohort_drug_final",
  "cohort_reac",
  "cohort_outc",
  "cohort_analytic"
)
existing <- dbListTables(con)
missing_tables <- setdiff(required_tables, existing)
print(missing_tables) # 预期空

# Lazy tbl refs
tbl_ci   <- tbl(con, "cohort_index")
tbl_demo <- tbl(con, "cohort_demo")
tbl_drug <- tbl(con, "cohort_drug_final")
tbl_reac <- tbl(con, "cohort_reac")
tbl_outc <- tbl(con, "cohort_outc")
tbl_ca   <- tbl(con, "cohort_analytic")

cat("\n=== 1. Row / case counts ===\n")
counts <- list(
  cohort_index_rows      = tbl_ci %>% tally() %>% collect(),
  cohort_demo_rows       = tbl_demo %>% tally() %>% collect(),
  cohort_drug_rows       = tbl_drug %>% tally() %>% collect(),
  cohort_reac_rows       = tbl_reac %>% tally() %>% collect(),
  cohort_outc_rows       = tbl_outc %>% tally() %>% collect(),
  cohort_analytic_rows   = tbl_ca %>% tally() %>% collect(),
  cohort_index_caseid    = tbl_ci %>% summarise(n_caseid = n_distinct(caseid)) %>% collect(),
  cohort_analytic_caseid = tbl_ca %>% summarise(n_caseid = n_distinct(caseid)) %>% collect()
)
print(counts)

cat("\n=== 2. Key uniqueness & referential checks ===\n")
dup_ci <- tbl_ci %>%
  count(caseid, primaryid) %>%
  filter(n > 1) %>%
  tally() %>%
  collect()
cat("2.1 cohort_index duplicate keys (should be 0):\n"); print(dup_ci)

missing_reac <- tbl_reac %>% anti_join(tbl_ci, by = c("caseid", "primaryid")) %>% tally() %>% collect()
missing_drug <- tbl_drug %>% anti_join(tbl_ci, by = c("caseid", "primaryid")) %>% tally() %>% collect()
missing_outc <- tbl_outc %>% anti_join(tbl_ci, by = c("caseid", "primaryid")) %>% tally() %>% collect()
missing_ca   <- tbl_ca %>% anti_join(tbl_ci, by = c("caseid", "primaryid")) %>% tally() %>% collect()
cat("\n2.2 records not in cohort_index (should be 0):\n")
print(list(
  reac_not_in_index = missing_reac,
  drug_not_in_index = missing_drug,
  outc_not_in_index = missing_outc,
  analytic_not_in_index = missing_ca
))

no_drug_cases <- tbl_ci %>% anti_join(tbl_drug, by = c("caseid", "primaryid")) %>% tally() %>% collect()
no_reac_cases <- tbl_ci %>% anti_join(tbl_reac, by = c("caseid", "primaryid")) %>% tally() %>% collect()
cat("\n2.3 cases without drug/reac (should be 0 or very small):\n")
print(list(
  cases_without_drug = no_drug_cases,
  cases_without_reac = no_reac_cases
))

cat("\n=== 3. Domain checks ===\n")
expected_generics <- c(
  "metformin",
  "semaglutide", "liraglutide", "dulaglutide", "exenatide", "tirzepatide",
  "empagliflozin", "dapagliflozin", "canagliflozin", "ertugliflozin",
  "sitagliptin", "saxagliptin", "linagliptin", "alogliptin",
  "insulin_glargine", "insulin_degludec"
)

tg_values <- tbl_drug %>% distinct(target_generic) %>% arrange(target_generic) %>% collect()
cat("3.1 target_generic distinct:\n"); print(tg_values)
cat("Unexpected target_generic:\n"); print(setdiff(tg_values$target_generic, expected_generics))

mech_values <- tbl_drug %>% distinct(mech_class_final) %>% arrange(mech_class_final) %>% collect()
cat("\n3.2 mech_class_final distinct:\n"); print(mech_values)

soc_values <- tbl_reac %>% distinct(pseudo_soc) %>% arrange(pseudo_soc) %>% collect()
cat("\n3.3 pseudo_soc distinct:\n"); print(soc_values)
soc_dist <- tbl_reac %>% count(pseudo_soc) %>% collect() %>% arrange(desc(n))
cat("Top pseudo_soc counts:\n"); print(head(soc_dist, 15))
prop_general <- soc_dist %>% mutate(prop = n / sum(n)) %>% filter(pseudo_soc == "general_disorders")
cat("Proportion general_disorders:\n"); print(prop_general)

cat("\n=== 4. Role check (if available) ===\n")
if ("role_cod" %in% colnames(tbl_drug)) {
  role_dist <- tbl_drug %>% count(role_cod) %>% collect()
  print(role_dist)
} else {
  cat("Column role_cod not found in cohort_drug_final; skipped.\n")
}

cat("\n=== 5. Outcome flags (from cohort_analytic) ===\n")
flag_cols <- c("death", "hosp", "disability", "lifethreat", "other", "congenital", "required_intervention")
for (v in flag_cols) {
  if (v %in% colnames(tbl_ca)) {
    cat("\nFlag:", v, "\n")
    vals <- tbl_ca %>% count(.data[[v]]) %>% collect() %>% arrange(.data[[v]])
    print(vals)
  } else {
    cat("Flag", v, "not found in cohort_analytic; skipped.\n")
  }
}

cat("\n=== 6. RxNorm quality (if table exists) ===\n")
if ("drug_normalized" %in% existing) {
  tbl_norm <- tbl(con, "drug_normalized")
  norm_summary <- tbl_norm %>% summarise(
    n = n(),
    n_rxcui_na = sum(is.na(rxcui) | rxcui == ""),
    n_drugname = n_distinct(drugname)
  ) %>% collect()
  cat("drug_normalized summary:\n"); print(norm_summary)
  source_dist <- tbl_norm %>% count(source) %>% collect()
  print(source_dist)
} else {
  cat("drug_normalized not found; skipped.\n")
}

cat("\n=== Done ===\n")
