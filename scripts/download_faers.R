# Automated downloader for FAERS quarterly ASCII ZIP files (2018-2024).
# Writes archives into data/raw/faers.

suppressPackageStartupMessages({
  if (!requireNamespace("glue", quietly = TRUE)) {
    stop("Package 'glue' is required. Install with install.packages('glue').")
  }
  if (!requireNamespace("purrr", quietly = TRUE)) {
    stop("Package 'purrr' is required. Install with install.packages('purrr').")
  }
})

library(glue)
library(purrr)

faers_dir <- file.path("data", "raw", "faers")
dir.create(faers_dir, recursive = TRUE, showWarnings = FALSE)

years <- 2018:2024
quarters <- 1:4

base_url <- "https://fis.fda.gov/content/Exports/faers_ascii_{year}Q{quarter}.zip"

options(timeout = max(600, getOption("timeout", 60)))

download_one <- function(year, quarter, dest_dir = faers_dir, max_attempts = 3, wait_secs = 5) {
  url <- glue(base_url, year = year, quarter = quarter)
  dest_file <- file.path(dest_dir, glue("faers_ascii_{year}Q{quarter}.zip", year = year, quarter = quarter))

  if (file.exists(dest_file)) {
    message("Skipping existing file: ", dest_file)
    return(invisible(dest_file))
  }

  method <- if (capabilities("libcurl")) "libcurl" else "auto"
  extra <- if (method %in% c("curl", "wget")) "--retry 3 --retry-delay 2 -L" else NULL

  message("Downloading ", url)
  for (attempt in seq_len(max_attempts)) {
    ok <- tryCatch(
      {
        utils::download.file(url, destfile = dest_file, mode = "wb", quiet = FALSE, method = method, extra = extra)
        TRUE
      },
      error = function(e) {
        message("Attempt ", attempt, " failed: ", conditionMessage(e))
        FALSE
      },
      warning = function(w) {
        message("Attempt ", attempt, " warning: ", conditionMessage(w))
        FALSE
      }
    )

    if (ok) {
      message("Saved -> ", dest_file)
      return(invisible(dest_file))
    }

    if (attempt < max_attempts) {
      message("Retrying in ", wait_secs, "s ...")
      Sys.sleep(wait_secs)
    }
  }

  message("Giving up on ", url)
  invisible(NULL)
}

download_all <- function(years, quarters) {
  suppressPackageStartupMessages({
    if (!requireNamespace("tidyr", quietly = TRUE)) {
      stop("Package 'tidyr' is required. Install with install.packages('tidyr').")
    }
  })

  grid <- tidyr::expand_grid(year = years, quarter = quarters)
  purrr::pwalk(grid, download_one)
}

download_all(years, quarters)
