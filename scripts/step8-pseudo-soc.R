# Step 8: PT → Pseudo-SOC mapping (with exact top mapping + keyword fallback)
# Goal: use exact mapping for frequent PTs (data/pseudo_soc_map_top.csv), then keyword dictionary; fallback to general_disorders.

suppressPackageStartupMessages({
  library(DBI)
  library(RSQLite)
  library(dplyr)
  library(readr)
  library(stringr)
})

db_path <- file.path("data", "faers.sqlite")
map_path <- file.path("data", "pseudo_soc_map_top.csv")

clean_pt <- function(x) {
  x <- str_to_lower(str_trim(as.character(x)))
  gsub("[^a-z0-9 ]", "", x)
}

# Pseudo-SOC keyword dictionary (used after exact mapping)
pseudo_dict <- list(
  infections_infestations = c("infection", "pneumonia", "bronchitis", "sinusitis", "covid", "influenza", "herpes zoster", "cellulitis", "sepsis", "neutropenia", "febrile neutropenia", "urinary tract infection", "nasopharyngitis", "upper respiratory tract infection", "lower respiratory tract infection"),
  gastrointestinal_disorders = c("nausea", "vomit", "diarrhoea", "diarrhea", "abdominal", "stomatitis", "dyspepsia", "gastric", "gastro", "constipation", "flatulence", "haematochezia", "colitis", "gastrooesophageal reflux", "pancreat", "reflux", "digest"),
  skin_subcutaneous_disorders = c("rash", "pruritus", "erythema", "alopecia", "psoriasis", "skin", "urticaria", "dermat", "injection site", "blister", "dry skin", "exfoliation", "skin irritation", "discolouration"),
  musculoskeletal_disorders = c("arthralgia", "arthritis", "muscle", "myalgia", "musculoskeletal", "osteoporosis", "osteonecrosis", "bone", "joint", "back pain", "neck pain", "gait", "fracture", "bone density", "psoriatic arthropathy"),
  nervous_system_disorders = c("headache", "seizure", "convulsion", "neuropathy", "tremor", "syncope", "migraine", "cognitive", "vertigo", "amnesia", "hypoaesthesia", "paraesthesia", "coma", "confusional state", "memory impairment", "speech disorder", "disturbance in attention", "loss of consciousness"),
  psychiatric_disorders = c("anxiety", "depression", "depressed", "insomnia", "hallucination", "suicid", "stress", "nervousness", "agitation", "mental disorder", "anhedonia", "suicide attempt", "suicidal ideation", "depressed mood", "withdrawal syndrome"),
  respiratory_disorders = c("dyspnoea", "dyspnea", "cough", "wheezing", "asthma", "respiratory", "copd", "rhinorrhoea", "nasal congestion", "pleural effusion", "oxygen saturation decreased", "respiratory failure"),
  cardiac_disorders = c("myocardial infarction", "cardiac", "heart rate", "palpitations", "tachycardia", "bradycardia", "cardiac arrest", "atrial fibrillation", "arrhythmia", "cardiac failure", "chest pain", "chest discomfort"),
  vascular_disorders = c("hypertension", "hypotension", "thrombosis", "pulmonary embolism", "embolism", "haemorrhage", "hemorrhage", "deep vein thrombosis", "blood pressure", "oedema", "edema", "swelling", "blood pressure fluctuation"),
  renal_urinary_disorders = c("kidney", "renal", "nephro", "urinary", "end stage renal", "creatinine", "renal impairment", "renal failure"),
  hepatobiliary_disorders = c("hepatic", "liver", "alanine aminotransferase", "aspartate aminotransferase", "transaminase", "hepatic enzyme"),
  metabolism_nutrition_disorders = c("weight increased", "weight decreased", "dehydration", "diabetes", "glucose", "hypokalaemia", "hyponatraemia", "bone density decreased", "bone loss", "osteoporosis", "appetite", "metabolic"),
  neoplasms = c("cancer", "carcinoma", "neoplasm", "myeloma", "tumour", "tumor", "leukaemia", "leukemia", "malignant"),
  eye_disorders = c("cataract", "eye", "vision", "ocular", "blurred vision", "visual impairment", "eye irritation", "eye pain"),
  blood_lymphatic_disorders = c("anaemia", "anemia", "neutropenia", "thrombocytopenia", "pancytopenia", "blood count", "white blood cell count", "platelet count"),
  pregnancy_perinatal_conditions = c("pregnancy", "maternal exposure", "foetal exposure", "fetal exposure", "exposure during pregnancy"),
  immune_system_disorders = c("hypersensitivity", "anaphylactic", "infusion related reaction", "immune", "allerg"),
  injury_poisoning_complications = c("injury", "fracture", "overdose", "skeletal injury", "fall", "contusion", "surgery", "toxicity", "poison"),
  product_issues = c("product", "device", "dose", "underdose", "overdose", "wrong technique", "quality issue", "prescribing error", "substitution", "storage error", "interaction", "off label", "misuse", "abuse", "noncompliance", "treatment failure", "drug ineffective", "adverse drug reaction", "dose omission", "product use"),
  device_issues = c("device", "expulsion", "leakage", "malfunction"),
  general_disorders = character(0) # fallback
)

classify_by_dict <- function(pt_vec, already_assigned = NULL) {
  pt_clean <- str_to_lower(str_trim(pt_vec))
  out <- if (is.null(already_assigned)) rep(NA_character_, length(pt_clean)) else already_assigned
  to_fill <- which(is.na(out))
  if (!length(to_fill)) return(out)
  for (soc in names(pseudo_dict)) {
    keys <- pseudo_dict[[soc]]
    if (!length(keys)) next
    pattern <- paste(keys, collapse = "|")
    hit <- grepl(pattern, pt_clean[to_fill], ignore.case = TRUE)
    idx <- to_fill[hit & is.na(out[to_fill])]
    if (length(idx)) out[idx] <- soc
  }
  out
}

run <- function() {
  if (!file.exists(db_path)) stop("Database not found: ", db_path)
  if (!file.exists(map_path)) stop("Mapping file not found: ", map_path, ". Ensure data/pseudo_soc_map_top.csv exists.")

  message("Reading exact PT→pseudo_SOC mapping from: ", map_path)
  map_top <- readr::read_csv(map_path, col_types = cols(pt = col_character(), pseudo_soc = col_character())) |>
    mutate(pt = clean_pt(pt), pseudo_soc = str_to_lower(str_trim(pseudo_soc)))
  if (any(duplicated(map_top$pt))) {
    message("Warning: duplicated PT in mapping, keeping first occurrence.")
    map_top <- map_top[!duplicated(map_top$pt), ]
  }

  con <- dbConnect(SQLite(), db_path)
  on.exit(dbDisconnect(con), add = TRUE)

  if (!"reac_clean" %in% dbListTables(con)) stop("reac_clean not found in database.")

  message("Reading PTs from reac_clean ...")
  reac_df <- dbGetQuery(con, "SELECT caseid, primaryid, pt FROM reac_clean;")
  if (!nrow(reac_df)) stop("No rows in reac_clean.")

  message("Cleaning PT terms ...")
  reac_df <- reac_df |>
    mutate(pt_clean = clean_pt(pt))

  message("Applying exact PT→pseudo_SOC mapping (top list) ...")
  reac_df <- reac_df |>
    left_join(map_top, by = c("pt_clean" = "pt")) |>
    rename(pseudo_soc_exact = pseudo_soc)

  message("Classifying unmatched PTs by keyword dictionary ...")
  pseudo_soc_dict <- classify_by_dict(reac_df$pt_clean, already_assigned = NA_character_)
  reac_df <- reac_df |>
    mutate(pseudo_soc = dplyr::coalesce(pseudo_soc_exact, pseudo_soc_dict, "general_disorders"))

  message("Writing reac_pseudo_soc into SQLite ...")
  dbExecute(con, "DROP TABLE IF EXISTS reac_pseudo_soc;")
  dbWriteTable(con, "reac_pseudo_soc", reac_df |> select(caseid, primaryid, pt, pseudo_soc), overwrite = TRUE)
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_reac_pseudo_caseid ON reac_pseudo_soc(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_reac_pseudo_soc ON reac_pseudo_soc(pseudo_soc);")

  summary_df <- dbGetQuery(con, "SELECT pseudo_soc, COUNT(*) AS n FROM reac_pseudo_soc GROUP BY pseudo_soc ORDER BY n DESC;")
  total_n <- sum(summary_df$n)
  general_n <- summary_df$n[summary_df$pseudo_soc == "general_disorders"]
  message("Total rows: ", format(total_n, big.mark = ","))
  if (length(general_n) == 1L) {
    perc <- round(100 * general_n / total_n, 1)
    message("general_disorders rows: ", format(general_n, big.mark = ","), " (", perc, "%)")
  }
  message("Top 15 pseudo_soc by count:")
  print(head(summary_df, 15))
  message("Done. Table created: reac_pseudo_soc")
}

if (sys.nframe() == 0) run()
