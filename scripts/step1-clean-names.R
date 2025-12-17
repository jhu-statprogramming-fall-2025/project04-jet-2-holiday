# Step 1: Column-name cleaning for FAERS TXT files (2018–2024).
# Reads all TXT files under data/raw/faers/unzipped, cleans column names,
# and writes out RDS files with standardized names for downstream steps.

suppressPackageStartupMessages({
  has_dt <- requireNamespace("data.table", quietly = TRUE)
})

# Make output directory for cleaned objects
out_dir <- file.path("data", "processed", "step1_clean_names")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# Column-name cleaner: lower case, underscores, no symbols/spaces
clean_colnames <- function(nms) {
  nms <- tolower(nms)
  nms <- trimws(nms)
  nms <- gsub("[^a-z0-9]+", "_", nms)       # replace any non-alnum with _
  nms <- gsub("_+", "_", nms)               # collapse repeats
  nms <- gsub("^_+|_+$", "", nms)           # trim leading/trailing _
  nms
}

# Read a FAERS TXT file and clean column names
read_faers_file <- function(path, sep = "$") {
  if (has_dt) {
    df <- data.table::fread(
      path,
      sep = sep,
      quote = "",
      na.strings = c("", "NA"),
      encoding = "UTF-8",
      data.table = FALSE,
      showProgress = FALSE
    )
  } else {
    df <- utils::read.table(
      path,
      sep = sep,
      quote = "",
      header = TRUE,
      na.strings = c("", "NA"),
      comment.char = "",
      stringsAsFactors = FALSE,
      check.names = FALSE
    )
  }

  names(df) <- clean_colnames(names(df))
  df
}

# Process one TXT: read -> clean names -> write RDS mirror under out_dir
process_one <- function(path, root_in = file.path("data", "raw", "faers", "unzipped"), root_out = out_dir) {
  rel <- sub(paste0("^", normalizePath(root_in, mustWork = TRUE)), "", normalizePath(path, mustWork = TRUE))
  rel <- sub("^/", "", rel)
  out_path <- file.path(root_out, sub("\\.txt$", ".rds", rel, ignore.case = TRUE))

  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  message("Processing: ", path)
  df <- read_faers_file(path)
  saveRDS(df, out_path)
  invisible(out_path)
}

# Discover all TXT files under the unzipped tree
find_txt_files <- function(root = file.path("data", "raw", "faers", "unzipped")) {
  list.files(root, pattern = "\\.txt$", recursive = TRUE, full.names = TRUE, ignore.case = TRUE)
}

# Main: iterate all TXT files and clean column names
run <- function() {
  files <- find_txt_files()
  message("Found ", length(files), " TXT files to process.")
  lapply(files, process_one)
  message("Done. Cleaned RDS files are under: ", out_dir)
}

# Execute when run via Rscript
if (sys.nframe() == 0) {
  run()
}
