# Step 6: RxNorm-based drug name normalization into SQLite.
# - Input: data/faers.sqlite (expects drug_wide from Step5, and clean tables)
# - Output: drug_normalized, target_cases_norm written into the same SQLite DB
# Note: Requires network access to RxNorm; uses a local cache to avoid repeat lookups.

suppressPackageStartupMessages({
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package 'DBI' is required. Install with install.packages('DBI').")
  }
  if (!requireNamespace("RSQLite", quietly = TRUE)) {
    stop("Package 'RSQLite' is required. Install with install.packages('RSQLite').")
  }
  if (!requireNamespace("httr", quietly = TRUE)) {
    stop("Package 'httr' is required. Install with install.packages('httr').")
  }
  if (!requireNamespace("jsonlite", quietly = TRUE)) {
    stop("Package 'jsonlite' is required. Install with install.packages('jsonlite').")
  }
  if (!requireNamespace("xml2", quietly = TRUE)) {
    stop("Package 'xml2' is required. Install with install.packages('xml2').")
  }
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required. Install with install.packages('dplyr').")
  }
  if (!requireNamespace("purrr", quietly = TRUE)) {
    stop("Package 'purrr' is required. Install with install.packages('purrr').")
  }
  if (!requireNamespace("tidyr", quietly = TRUE)) {
    stop("Package 'tidyr' is required. Install with install.packages('tidyr').")
  }
})

library(DBI)
library(RSQLite)
library(httr)
library(jsonlite)
library(dplyr)
library(purrr)
library(tidyr)

db_path <- file.path("data", "faers.sqlite")
cache_dir <- file.path("data", "cache")
dir.create(cache_dir, recursive = TRUE, showWarnings = FALSE)
cache_file <- file.path(cache_dir, "rxnorm_cache.rds")

target_map <- tibble::tribble(
  ~generic_key,       ~target_generic,      ~atc_code,  ~mech_class,
  "metformin",        "metformin",          "A10BA02",  "Biguanide",
  "semaglutide",      "semaglutide",        "A10BJ06",  "GLP1",
  "liraglutide",      "liraglutide",        "A10BJ02",  "GLP1",
  "dulaglutide",      "dulaglutide",        "A10BJ05",  "GLP1",
  "exenatide",        "exenatide",          "A10BJ01",  "GLP1",
  "tirzepatide",      "tirzepatide",        "A10BX23",  "GIP_GLP1",
  "empagliflozin",    "empagliflozin",      "A10BK03",  "SGLT2",
  "dapagliflozin",    "dapagliflozin",      "A10BK01",  "SGLT2",
  "canagliflozin",    "canagliflozin",      "A10BK02",  "SGLT2",
  "ertugliflozin",    "ertugliflozin",      "A10BK04",  "SGLT2",
  "sitagliptin",      "sitagliptin",        "A10BH01",  "DPP4",
  "saxagliptin",      "saxagliptin",        "A10BH03",  "DPP4",
  "linagliptin",      "linagliptin",        "A10BH05",  "DPP4",
  "alogliptin",       "alogliptin",         "A10BH04",  "DPP4",
  "insulin glargine", "insulin_glargine",   "A10AE04",  "BasalInsulin",
  "insulin degludec", "insulin_degludec",   "A10AE06",  "BasalInsulin"
)

brand_map <- tibble::tribble(
  ~brand_key,    ~target_generic,
  "ozempic",     "semaglutide",
  "rybelsus",    "semaglutide",
  "wegovy",      "semaglutide",
  "victoza",     "liraglutide",
  "saxenda",     "liraglutide",
  "trulicity",   "dulaglutide",
  "byetta",      "exenatide",
  "bydureon",    "exenatide",
  "mounjaro",    "tirzepatide",
  "zepbound",    "tirzepatide",
  "jardiance",   "empagliflozin",
  "farxiga",     "dapagliflozin",
  "invokana",    "canagliflozin",
  "steglatro",   "ertugliflozin",
  "januvia",     "sitagliptin",
  "onglyza",     "saxagliptin",
  "tradjenta",   "linagliptin",
  "nesina",      "alogliptin",
  "lantus",      "insulin_glargine",
  "toujeo",      "insulin_glargine",
  "basaglar",    "insulin_glargine",
  "tresiba",     "insulin_degludec",
  "glucophage",  "metformin",
  "fortamet",    "metformin",
  "glumetza",    "metformin",
  "riomet",      "metformin"
)

load_cache <- function() {
  if (file.exists(cache_file)) readRDS(cache_file) else list()
}

save_cache <- function(x) {
  saveRDS(x, cache_file)
}

clean_name <- function(x) {
  x <- as.character(x)
  x <- tolower(x)
  x <- gsub("[[:punct:]]", " ", x)
  x <- gsub("\\b\\d+(mg|mcg|g|ml|%|iu|units)?\\b", " ", x)
  x <- gsub("\\b(injection|inj|pen|vial|syringe|tab|tablet|cap|capsule|solution|soln|suspension|susp|dose|u-100|u100|u-200|u200|sc|subcutaneous|im)\\b", " ", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

rxnorm_root <- function(parsed) {
  if (is.null(parsed) || inherits(parsed, "error")) return(NULL)
  if (!is.null(parsed$rxnormdata)) parsed$rxnormdata else parsed
}

rxnorm_exact <- function(term) {
  url <- sprintf("https://rxnav.nlm.nih.gov/REST/rxcui?name=%s", URLencode(term))
  res <- tryCatch(httr::GET(url, httr::accept("application/xml"), timeout(10)), error = identity)
  if (inherits(res, "error") || httr::status_code(res) != 200) return(NA_character_)
  doc <- tryCatch(httr::content(res, "text", encoding = "UTF-8"), error = identity)
  parsed <- tryCatch(xml2::as_list(xml2::read_xml(doc)), error = identity)
  if (inherits(parsed, "error")) return(NA_character_)
  root <- rxnorm_root(parsed)
  if (is.null(root)) return(NA_character_)
  rx <- unlist(root$idGroup$rxnormId)
  if (length(rx) == 0) rx <- unlist(root$rxcuiIdList$rxcui)
  if (length(rx)) rx[[1]] else NA_character_
}


rxnorm_approx <- function(term) {
  url <- sprintf("https://rxnav.nlm.nih.gov/REST/approximateTerm?term=%s&maxEntries=5", URLencode(term))
  res <- tryCatch(httr::GET(url, httr::accept("application/xml"), timeout(10)), error = identity)
  if (inherits(res, "error")) return(NA_character_)
  if (httr::status_code(res) != 200) return(NA_character_)
  doc <- tryCatch(httr::content(res, "text", encoding = "UTF-8"), error = identity)
  if (inherits(doc, "error")) return(NA_character_)
  parsed <- tryCatch(xml2::as_list(xml2::read_xml(doc)), error = identity)
  if (inherits(parsed, "error")) return(NA_character_)
  root <- rxnorm_root(parsed)
  if (is.null(root)) return(NA_character_)
  cand <- root$approximateGroup$candidate
  if (is.null(cand)) return(NA_character_)
  # cand could be list of candidates; take first rxcui
  rc <- unlist(lapply(cand, function(x) x$rxcui))
  if (length(rc)) rc[[1]] else NA_character_
}

rxnorm_generic <- function(rxcui) {
  if (is.na(rxcui)) return(NA_character_)
  url <- sprintf("https://rxnav.nlm.nih.gov/REST/rxcui/%s/property?propName=GENERIC_NAME", rxcui)
  res <- tryCatch(httr::GET(url, httr::accept("application/xml"), timeout(10)), error = identity)
  if (inherits(res, "error")) return(NA_character_)
  if (httr::status_code(res) != 200) return(NA_character_)
  doc <- tryCatch(httr::content(res, "text", encoding = "UTF-8"), error = identity)
  if (inherits(doc, "error")) return(NA_character_)
  parsed <- tryCatch(xml2::as_list(xml2::read_xml(doc)), error = identity)
  if (inherits(parsed, "error")) return(NA_character_)
  root <- rxnorm_root(parsed)
  if (is.null(root)) return(NA_character_)
  val <- unlist(root$propConceptGroup$propConcept$propValue)
  if (length(val) == 0) {
    val <- unlist(root$propertyConceptList$propertyConcept$propValue)
  }
  if (length(val)) val[[1]] else NA_character_
}

rxnorm_name <- function(rxcui) {
  if (is.na(rxcui)) return(NA_character_)
  url <- sprintf("https://rxnav.nlm.nih.gov/REST/rxcui/%s/property?propName=NAME", rxcui)
  res <- tryCatch(httr::GET(url, httr::accept("application/xml"), timeout(10)), error = identity)
  if (inherits(res, "error")) return(NA_character_)
  if (httr::status_code(res) != 200) return(NA_character_)
  doc <- tryCatch(httr::content(res, "text", encoding = "UTF-8"), error = identity)
  if (inherits(doc, "error")) return(NA_character_)
  parsed <- tryCatch(xml2::as_list(xml2::read_xml(doc)), error = identity)
  if (inherits(parsed, "error")) return(NA_character_)
  root <- rxnorm_root(parsed)
  if (is.null(root)) return(NA_character_)
  val <- unlist(root$propConceptGroup$propConcept$propValue)
  if (length(val) == 0) {
    val <- unlist(root$propertyConceptList$propertyConcept$propValue)
  }
  if (length(val)) val[[1]] else NA_character_
}

lookup_rxnorm <- function(term, cache) {
  if (!is.null(cache[[term]])) return(cache[[term]])
  out <- list(
    rxcui = NA_character_,
    generic = NA_character_,
    name_rx = NA_character_,
    source = "rxnorm_none"
  )
  rxcui <- rxnorm_exact(term)
  source <- "rxnorm_exact"
  if (is.na(rxcui)) {
    rxcui <- rxnorm_approx(term)
    source <- "rxnorm_approx"
  }
  generic <- rxnorm_generic(rxcui)
  name_rx <- rxnorm_name(rxcui)
  if (length(generic) == 0 || is.na(generic)) {
    generic <- NA_character_
  } else {
    generic <- generic[[1]]
  }
  if (length(name_rx) == 0 || is.na(name_rx)) {
    name_rx <- NA_character_
  } else {
    name_rx <- name_rx[[1]]
  }
  out <- list(
    rxcui = rxcui,
    generic = tolower(generic),
    name_rx = tolower(name_rx),
    source = source
  )
  cache[[term]] <- out
  save_cache(cache)
  out
}

map_to_target <- function(term_list) {
  purrr::map(term_list, function(tt) {
    tt <- unique(trimws(tolower(unlist(tt))))
    tt <- tt[tt != ""]
    if (!length(tt)) return(NULL)
    purrr::map_dfr(tt, function(tok) {
      # Exact match only (full token or split by space); avoids over-broad substring matches
      tokens <- unique(c(tok, unlist(strsplit(tok, "\\s+"))))
      m <- target_map %>% filter(generic_key %in% tokens)
      if (nrow(m) == 0) {
        b <- brand_map %>% filter(brand_key %in% tokens)
        if (nrow(b)) m <- target_map %>% filter(target_generic %in% b$target_generic)
      }
      if (nrow(m) == 0) return(NULL)
      m
    }) %>% distinct()
  })
}

run <- function() {
  if (!file.exists(db_path)) stop("Database not found at ", db_path)
  con <- dbConnect(SQLite(), db_path)
  on.exit(dbDisconnect(con), add = TRUE)

  message("Reading distinct drugname from drug_wide ...")
  dn_df <- dbGetQuery(con, "SELECT DISTINCT drugname FROM drug_wide;")
  dn_df$drugname_clean <- vapply(dn_df$drugname, clean_name, character(1))

  sample_frac <- as.numeric(Sys.getenv("RXNORM_SAMPLE_FRAC", "1"))
  if (!is.na(sample_frac) && sample_frac > 0 && sample_frac < 1) {
    n_keep <- max(1, floor(nrow(dn_df) * sample_frac))
    dn_df <- dn_df[sample.int(nrow(dn_df), n_keep), , drop = FALSE]
    message(sprintf("Sampling %.1f%% of names for test run (%d rows)", sample_frac * 100, nrow(dn_df)))
  }

  cache <- load_cache()
  message("Querying RxNorm (this may take time; uses cache when available)...")
  pb <- txtProgressBar(min = 0, max = nrow(dn_df), style = 3)
  lookups <- vector("list", nrow(dn_df))
  for (i in seq_len(nrow(dn_df))) {
    lookups[[i]] <- lookup_rxnorm(dn_df$drugname_clean[i], cache = cache)
    if (i %% 50 == 0 || i == nrow(dn_df)) setTxtProgressBar(pb, i)
  }
  close(pb)
  `%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
  lookup_df <- dplyr::bind_cols(dn_df, tibble::tibble(
    rxcui = purrr::map_chr(lookups, ~ .x$rxcui %||% NA_character_),
    generic_rx = purrr::map_chr(lookups, ~ .x$generic %||% NA_character_),
    name_rx = purrr::map_chr(lookups, ~ .x$name_rx %||% NA_character_),
    rx_source = purrr::map_chr(lookups, ~ .x$source %||% NA_character_)
  ))

  message("Mapping RxNorm generic to target drugs...")
  term_list <- purrr::pmap(list(lookup_df$generic_rx, lookup_df$name_rx, lookup_df$drugname_clean), function(a, b, c) {
    unique(c(a, b, c))
  })
  mapped_list <- map_to_target(term_list)
  # Expand rows for multi-component products
  expanded <- purrr::map2_dfr(seq_along(mapped_list), mapped_list, function(i, m) {
    if (is.null(m)) return(NULL)
    cbind(lookup_df[rep(i, nrow(m)), c("drugname", "drugname_clean", "rxcui", "generic_rx", "rx_source")], m)
  })

  if (nrow(expanded) == 0) stop("RxNorm returned no mappable generics; check network or adjust target_map.")

  normalized <- expanded %>%
    distinct(drugname, target_generic, .keep_all = TRUE) %>%
    filter(!is.na(rxcui), rxcui != "") %>%  # drop rows with missing rxcui
    mutate(source = rx_source) %>%
    select(drugname, drugname_clean, rxcui, generic_standard = generic_rx,
           target_generic, atc_code, mech_class, source)

  if (nrow(normalized) == 0) stop("No normalized drugs; check RxNorm/network.")

  message("Joining normalized mapping back to drug_wide ...")
  drug_wide <- dbGetQuery(con, "SELECT * FROM drug_wide;")
  drug_norm <- drug_wide %>%
    left_join(normalized, by = c("drugname" = "drugname")) %>%
    filter(!is.na(target_generic))

  message("Writing drug_normalized and target_cases_norm ...")
  dbExecute(con, "DROP TABLE IF EXISTS drug_normalized;")
  dbWriteTable(con, "drug_normalized", drug_norm, overwrite = TRUE)
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_drug_normalized_caseid ON drug_normalized(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_drug_normalized_primaryid ON drug_normalized(primaryid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_drug_normalized_target ON drug_normalized(target_generic);")

  dbExecute(con, "DROP TABLE IF EXISTS target_cases_norm;")
  dbExecute(con, "
    CREATE TABLE target_cases_norm AS
    SELECT DISTINCT caseid, primaryid FROM drug_normalized
  ;")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_target_cases_norm_caseid ON target_cases_norm(caseid);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_target_cases_norm_primaryid ON target_cases_norm(primaryid);")

  message("Done. Tables created: drug_normalized, target_cases_norm")
}

if (sys.nframe() == 0) {
  run()
}
