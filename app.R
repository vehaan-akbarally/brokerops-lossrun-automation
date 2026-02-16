# ---------------------------------------------------------
# BrokerOps AI - Loss-Run & Bordereaux Automation (Stable)
# (Privacy-by-default • DQ • PDF/Table OCR • MVP-UX)
# ---------------------------------------------------------

# Safe env handling (optional local load; on shinyapps, .Renviron is auto-read)
if (file.exists(".Renviron")) readRenviron(".Renviron")

APP_VERSION <- "0.6.2-pilots"  # bumped

# ---------- Env helpers (trim + safe parse) ----------
env_str <- function(name, unset = "") trimws(Sys.getenv(name, unset = unset))
env_int <- function(name, unset) {
  raw <- trimws(Sys.getenv(name, unset = as.character(unset)))
  x <- suppressWarnings(as.integer(raw))
  if (is.na(x)) as.integer(unset) else x
}
env_num <- function(name, unset) {
  raw <- trimws(Sys.getenv(name, unset = as.character(unset)))
  x <- suppressWarnings(as.numeric(raw))
  if (is.na(x)) as.numeric(unset) else x
}
env_bool <- function(name, unset = FALSE) {
  v <- tolower(trimws(Sys.getenv(name, unset = if (unset) "true" else "false")))
  v %in% c("1","true","t","yes","y","on")
}

# ---- Environment / Debug options ----
APP_ENV <- env_str("APP_ENV", "production")
is_prod <- identical(APP_ENV, "production")

# Shiny upload cap BEFORE request body is read (env-driven; default 500MB)
MAX_UPLOAD_MB_ENV <- env_num("MAX_UPLOAD_MB", 500)         # MB
RUN_TIMEOUT_SEC   <- env_int("RUN_TIMEOUT_SEC", 90)
READ_TIMEOUT_SEC  <- env_int("READ_TIMEOUT_SEC", 45)
CAP_MAX_PDF_PAGES <- env_int("MAX_PDF_PAGES", 500)
DB_PATH           <- env_str("DB_PATH", "")                # set to persist (e.g. "/srv/brokerops.sqlite")

options(
  shiny.fullstacktrace = !is_prod,   # full traces only in dev
  shiny.sanitize.errors = is_prod,   # sanitize in prod
  brokerops.locale_profile = env_str("LOCALE_PROFILE", "AU"),
  shiny.maxRequestSize = MAX_UPLOAD_MB_ENV * 1024^2
)

# ---- Packages ----
suppressPackageStartupMessages({
  library(shiny)
  library(shinythemes)
  library(shinyjs)
  library(DT)
  library(readr)
  library(readxl)
  library(writexl)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(purrr)
  library(lubridate)
  library(httr)
  library(jsonlite)
  library(glue)
  library(htmltools)
  library(tibble)
  library(pdftools)
  library(digest)
  library(promises)
  library(future)
  library(zip)
  library(withr)
  library(R.utils)
  library(DBI)
  library(RSQLite)
  # NOTE: tesseract used if installed
})

# ---- Concurrency plan (auto-fallback on hosted envs) ----
workers <- max(1L, env_int("WORKERS", 2))
plan_name <- if (nzchar(Sys.getenv("RSC_INSTANCE")) || nzchar(Sys.getenv("SHINYAPPS_URL"))) "sequential" else "multisession"
if (identical(plan_name, "multisession")) {
  future::plan(multisession, workers = workers)
} else {
  future::plan(sequential)
}

# Optional extras
has_tabulizer <- requireNamespace("tabulizer", quietly = TRUE)
has_filelock  <- requireNamespace("filelock", quietly = TRUE)
has_arrow     <- requireNamespace("arrow", quietly = TRUE)

# ---------- Config ----------
ALLOWED_CURRENCIES <- unique(c("AUD","USD","NZD","GBP","EUR","CAD","SGD"))
AI_RATE_PER_MIN   <- env_int("AI_RATE_PER_MINUTE", 3)
AI_RATE_PER_SESS  <- env_int("AI_RATE_PER_SESSION", 10)

LOCALES <- list(
  AU = list(date_orders = c("d/m/Y","Y-m-d","d-b-Y","d.m.Y","d/m/y","Y/m/d"),
            decimal_mark = ".", big_mark = ","),
  US = list(date_orders = c("m/d/Y","Y-m-d","b d Y","m.d.Y","m/d/y","Y/m/d"),
            decimal_mark = ".", big_mark = ",")
)
LOCALE_DEFAULT <- getOption("brokerops.locale_profile", "AU")

# NULL-coalescer
`%||%` <- function(a, b) if (is.null(a)) b else a
or_null <- function(a, b) if (is.null(a)) b else a

# ---------- Audit log (PII-safe) ----------
AUDIT_FILE <- env_str("AUDIT_FILE", "")
HIDE_FILENAMES <- env_bool("HIDE_FILENAMES", TRUE)
AUDIT_SALT <- env_str("AUDIT_SALT", "")

scrub_name <- function(x) {
  if (!HIDE_FILENAMES) return(x)
  if (nzchar(AUDIT_SALT)) return(digest::hmac(AUDIT_SALT, x, algo = "sha256"))
  "redacted"
}

# ---- SQLite (optional) ----
db_available <- function() nzchar(DB_PATH)
db_connect <- function() { if (!db_available()) return(NULL); DBI::dbConnect(RSQLite::SQLite(), DB_PATH) }

db_init <- function(){
  con <- db_connect(); if (is.null(con)) return(invisible(FALSE))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, "PRAGMA journal_mode=WAL")
  DBI::dbExecute(con, "CREATE TABLE IF NOT EXISTS runs(
      run_id TEXT PRIMARY KEY, tenant_id TEXT, ts_utc TEXT,
      n_rows INTEGER, n_claims INTEGER, total_paid REAL, total_incurred REAL)")
  DBI::dbExecute(con, "CREATE TABLE IF NOT EXISTS issues(
      run_id TEXT, row INTEGER, claim_id TEXT, issue TEXT, detail TEXT)")
  DBI::dbExecute(con, "CREATE TABLE IF NOT EXISTS saved_mappings(
      tenant_id TEXT, counterparty TEXT, mapping_json TEXT, ts_utc TEXT,
      PRIMARY KEY(tenant_id, counterparty))")
  DBI::dbExecute(con, "CREATE TABLE IF NOT EXISTS audit_log(
      ts_utc TEXT, event TEXT, detail_json TEXT, app_version TEXT)")
  invisible(TRUE)
}

db_prune_30d <- function(){
  con <- db_connect(); if (is.null(con)) return(invisible(FALSE))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, "DELETE FROM runs WHERE datetime(ts_utc) < datetime('now','-30 days')")
  DBI::dbExecute(con, "DELETE FROM issues WHERE run_id NOT IN (SELECT run_id FROM runs)")
  DBI::dbExecute(con, "DELETE FROM audit_log WHERE datetime(ts_utc) < datetime('now','-30 days')")
  invisible(TRUE)
}

db_delete_all <- function(){
  con <- db_connect(); if (is.null(con)) return(invisible(FALSE))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, "DELETE FROM runs")
  DBI::dbExecute(con, "DELETE FROM issues")
  DBI::dbExecute(con, "DELETE FROM saved_mappings")
  DBI::dbExecute(con, "DELETE FROM audit_log")
  invisible(TRUE)
}

db_save_run <- function(run_id, tenant_id, Q, issues_df){
  con <- db_connect(); if (is.null(con)) return(invisible(FALSE))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con,
                 "INSERT OR REPLACE INTO runs(run_id,tenant_id,ts_utc,n_rows,n_claims,total_paid,total_incurred)
     VALUES(?,?,?,?,?,?,?)",
                 params = list(run_id, tenant_id, format(Sys.time(), "%FT%TZ", tz="UTC"),
                               Q$n_rows %||% NA, Q$n_claims %||% NA, Q$total_paid %||% NA, Q$total_incurred %||% NA))
  if (!is.null(issues_df) && nrow(issues_df) > 0) {
    to_write <- issues_df %>% mutate(run_id = run_id) %>%
      select(run_id, row, claim_id, issue, detail)
    DBI::dbWriteTable(con, "issues", to_write, append = TRUE)
  }
  invisible(TRUE)
}

db_list_saved_counterparties <- function(tenant_id){
  con <- db_connect(); if (is.null(con)) return(character())
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  res <- tryCatch(DBI::dbGetQuery(con, "SELECT counterparty FROM saved_mappings WHERE tenant_id = ? ORDER BY counterparty",
                                  params = list(tenant_id)), error = function(e) data.frame())
  unique(res$counterparty %||% character())
}

db_save_mapping <- function(tenant_id, counterparty, mapping_list){
  con <- db_connect(); if (is.null(con)) return(invisible(FALSE))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con,
                 "INSERT OR REPLACE INTO saved_mappings(tenant_id,counterparty,mapping_json,ts_utc)
     VALUES(?,?,?,datetime('now'))",
                 params = list(tenant_id, counterparty, jsonlite::toJSON(mapping_list, auto_unbox = TRUE)))
  invisible(TRUE)
}

db_load_mapping <- function(tenant_id, counterparty){
  con <- db_connect(); if (is.null(con)) return(NULL)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  j <- tryCatch(DBI::dbGetQuery(con,
                                "SELECT mapping_json FROM saved_mappings WHERE tenant_id = ? AND counterparty = ?",
                                params = list(tenant_id, counterparty))$mapping_json[1], error=function(e) NA_character_)
  if (!is.na(j) && nzchar(j)) tryCatch(jsonlite::fromJSON(j), error=function(e) NULL) else NULL
}

# audit() now mirrors to DB too (if available)
audit <- function(event, detail = list()){
  if (nzchar(AUDIT_FILE)) {
    rec <- list(ts = format(Sys.time(), "%FT%TZ", tz = "UTC"),
                event = event, detail = detail, app_version = APP_VERSION)
    line <- jsonlite::toJSON(rec, auto_unbox=TRUE)
    dir.create(dirname(AUDIT_FILE), recursive = TRUE, showWarnings = FALSE)
    if (has_filelock) {
      lock_path <- paste0(AUDIT_FILE, ".lock")
      lock <- filelock::lock(lock_path, timeout = 5000)
      on.exit(try(filelock::unlock(lock), silent=TRUE), add = TRUE)
      cat(line, "\n", file = AUDIT_FILE, append = TRUE)
    } else {
      cat(line, "\n", file = AUDIT_FILE, append = TRUE)
    }
  }
  # mirror to DB
  if (db_available()) {
    con <- db_connect(); if (!is.null(con)) {
      on.exit(DBI::dbDisconnect(con), add = TRUE)
      DBI::dbExecute(con,
                     "INSERT INTO audit_log(ts_utc,event,detail_json,app_version) VALUES(?,?,?,?)",
                     params = list(format(Sys.time(), "%FT%TZ", tz="UTC"), event,
                                   jsonlite::toJSON(detail, auto_unbox = TRUE), APP_VERSION))
    }
  }
}

# ---------- Pseudonymization salt ----------
TENANT_ID       <- env_str("TENANT_ID", "")
MASK_MASTER_KEY <- env_str("MASK_MASTER_KEY", "")
MASK_SALT       <- env_str("MASK_SALT", "")
MASK_SALT_SOURCE <- "ephemeral"
if (!nzchar(MASK_SALT)) {
  if (nzchar(MASK_MASTER_KEY) && nzchar(TENANT_ID)) {
    MASK_SALT <- paste0("v1|", digest::hmac(MASK_MASTER_KEY, TENANT_ID, algo = "sha256"))
    MASK_SALT_SOURCE <- "tenant"
  } else {
    MASK_SALT <- paste0("v1|", digest::digest(runif(1), algo = "sha256"))
    MASK_SALT_SOURCE <- "ephemeral"
  }
} else {
  MASK_SALT_SOURCE <- "env"
}

# --- Optional access-code gate for pilots ---
ACCESS_CODE <- env_str("ACCESS_CODE", "")

# ---------- Cloud OCR Config (BYO keys) ----------
OCR_PROVIDER          <- tolower(env_str("OCR_PROVIDER", ""))
AZURE_DOCINT_ENDPOINT <- env_str("AZURE_DOCINT_ENDPOINT", "")
AZURE_DOCINT_KEY      <- env_str("AZURE_DOCINT_KEY", "")
AZURE_REGION          <- env_str("AZURE_REGION", "")  # informational only

rtrim_slash <- function(x) sub("/+$", "", x)
cloud_ocr_configured <- function(provider) {
  switch(tolower(provider),
         "azure" = nzchar(AZURE_DOCINT_ENDPOINT) && nzchar(AZURE_DOCINT_KEY),
         FALSE)
}

# ---- OCR backend registry (pluggable) ----
OCR_BACKENDS <- new.env(parent = emptyenv())
register_ocr <- function(name, fn, configured_fn = function() TRUE) {
  assign(paste0("fn_", tolower(name)), fn, OCR_BACKENDS)
  assign(paste0("ok_", tolower(name)), configured_fn, OCR_BACKENDS)
}
get_ocr_fn <- function(name) get0(paste0("fn_", tolower(name)), OCR_BACKENDS, ifnotfound = NULL)
get_ocr_ok <- function(name) get0(paste0("ok_", tolower(name)), OCR_BACKENDS, ifnotfound = function() FALSE)

# ---- Azure Document Intelligence (prebuilt-layout) with cache ----
azure_layout_analyze <- function(pdf_path,
                                 endpoint = AZURE_DOCINT_ENDPOINT,
                                 key = AZURE_DOCINT_KEY,
                                 api_version = "2023-07-31",
                                 max_wait_sec = 120) {
  if (!nzchar(endpoint) || !nzchar(key)) stop("Azure Document Intelligence not configured.")
  # cache by file digest
  cache_dir <- file.path(tempdir(), "brokerops_ocr_cache")
  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  file_hash <- digest::digest(file = pdf_path, algo = "sha256")
  cache_file <- file.path(cache_dir, paste0(file_hash, ".json"))
  if (file.exists(cache_file)) {
    j <- tryCatch(jsonlite::read_json(cache_file, simplifyVector = FALSE), error = function(e) NULL)
    if (!is.null(j)) return(j)
  }
  
  url <- paste0(rtrim_slash(endpoint), "/formrecognizer/documentModels/prebuilt-layout:analyze?api-version=", api_version)
  size <- tryCatch(file.info(pdf_path)$size, error = function(e) NA_real_)
  if (is.na(size) || size <= 0) stop("PDF file not found or empty for Azure OCR.")
  
  bin <- readBin(pdf_path, what = "raw", n = size)
  resp <- httr::POST(
    url,
    httr::add_headers(`Ocp-Apim-Subscription-Key` = key, `Content-Type` = "application/pdf"),
    body = bin, encode = "raw",
    httr::timeout(20)
  )
  if (httr::status_code(resp) %/% 100 != 2) {
    stop(sprintf("Azure analyze POST failed (%s)", httr::status_code(resp)))
  }
  op_loc <- httr::headers(resp)[["operation-location"]]
  if (is.null(op_loc)) stop("Azure: missing operation-location header.")
  
  t0 <- Sys.time()
  delay <- 0.8
  repeat {
    Sys.sleep(delay)
    res <- httr::GET(op_loc, httr::add_headers(`Ocp-Apim-Subscription-Key` = key), httr::timeout(20))
    code <- httr::status_code(res)
    if (code %/% 100 != 2) stop(sprintf("Azure analyze GET failed (%s)", code))
    j  <- httr::content(res, as = "parsed", type = "application/json", encoding = "UTF-8")
    st <- tolower(or_null(j$status, ""))
    if (identical(st, "succeeded")) {
      try({ jsonlite::write_json(j, cache_file, auto_unbox = TRUE, pretty = FALSE) }, silent = TRUE)
      return(j)
    }
    if (identical(st, "failed")) stop("Azure analysis failed.")
    if (as.numeric(difftime(Sys.time(), t0, units = "secs")) > max_wait_sec) stop("Azure analysis timed out.")
    delay <- min(5, delay * 1.5 + runif(1, 0, 0.25))
  }
}

azure_tables_to_df <- function(analysis_json) {
  ar <- analysis_json$analyzeResult
  if (is.null(ar) || is.null(ar$tables) || !length(ar$tables)) return(NULL)
  dfs <- lapply(ar$tables, function(tb) {
    nr <- or_null(tb$rowCount, 0); nc <- or_null(tb$columnCount, 0)
    if (nr == 0 || nc == 0) return(NULL)
    mat <- matrix("", nrow = nr, ncol = nc)
    cells <- tb$cells
    if (!length(cells)) return(NULL)
    for (cell in cells) {
      r <- as.integer(cell$rowIndex) + 1L
      c <- as.integer(cell$columnIndex) + 1L
      val <- or_null(cell$content, or_null(cell$text, ""))
      rs <- max(1L, as.integer(or_null(cell$rowSpan, 1L)))
      cs <- max(1L, as.integer(or_null(cell$columnSpan, 1L)))
      for (rr in r:(r + rs - 1L)) for (cc in c:(c + cs - 1L)) {
        if (rr <= nr && cc <= nc && !nzchar(mat[rr, cc])) mat[rr, cc] <- val
      }
    }
    df <- as.data.frame(mat, stringsAsFactors = FALSE, check.names = FALSE)
    pg <- NA_integer_
    if (!is.null(tb$boundingRegions) && length(tb$boundingRegions) && !is.null(tb$boundingRegions[[1]]$pageNumber)) {
      pg <- as.integer(tb$boundingRegions[[1]]$pageNumber)
    }
    df$..source_page.. <- pg
    df
  })
  dfs <- dfs[!vapply(dfs, is.null, logical(1))]
  if (!length(dfs)) return(NULL)
  df <- dplyr::bind_rows(dfs)
  names(df) <- make.names(names(df), unique = TRUE)
  df
}

register_ocr(
  "azure",
  function(path){
    az <- tryCatch(azure_layout_analyze(path), error = function(e){ message("Azure OCR error: ", e$message); NULL })
    if (is.null(az)) return(NULL)
    tryCatch(azure_tables_to_df(az), error = function(e){ message("Azure parse error: ", e$message); NULL })
  },
  function(){ cloud_ocr_configured("azure") }
)

# ---------- Helpers (locale-aware) ----------
normalize_names <- function(x) {
  x <- stringr::str_replace_all(x, "[^A-Za-z0-9]+", "_")
  x <- stringr::str_replace_all(x, "_+", "_")
  x <- stringr::str_replace_all(x, "^_|_$", "")
  tolower(x)
}
norm_id <- function(x) tolower(trimws(as.character(x)))

file_sha256 <- function(path) digest::digest(file = path, algo = "sha256")

# --- Magic-byte sniffing to block mislabeled uploads ---
read_magic <- function(path, n = 8L) {
  raw <- tryCatch(readBin(path, what = "raw", n = n), error = function(e) raw(0))
  paste0(as.integer(raw), collapse = " ")
}
is_pdf_magic <- function(path) {
  hdr <- tryCatch(readChar(path, nchars = 5L, useBytes = TRUE), error = function(e) "")
  startsWith(hdr, "%PDF-")
}
is_zip_magic <- function(path) {
  raw <- tryCatch(readBin(path, what = "raw", n = 2), error = function(e) raw(0))
  length(raw) >= 2 && raw[1] == as.raw(0x50) && raw[2] == as.raw(0x4B)
}
is_ole_magic <- function(path) {
  raw <- tryCatch(readBin(path, what = "raw", n = 8), error = function(e) raw(0))
  ident <- as.integer(raw)
  all(ident[1:8] == c(0xD0,0xCF,0x11,0xE0,0xA1,0xB1,0x1A,0xE1))
}

pdf_sanity_check <- function(path, max_pages = 500L) {
  info <- tryCatch(pdftools::pdf_info(path), error = function(e) NULL)
  if (is.null(info)) return(list(ok = FALSE, reason = "Unreadable PDF"))
  if (isTRUE(info$encrypted)) return(list(ok = FALSE, reason = "Encrypted/password-protected PDF"))
  pages <- or_null(info$pages, NA_integer_)
  if (!is.na(pages) && pages > max_pages) return(list(ok = FALSE, reason = paste("Too many pages (>", max_pages, ")")))
  list(ok = TRUE, pages = pages)
}

pdf_has_selectable_text <- function(path, min_chars = 400L) {
  txt <- tryCatch(pdftools::pdf_text(path), error = function(e) character())
  if (!length(txt)) return(FALSE)
  s <- paste(head(txt, 2), collapse = "\n")
  nz <- nchar(gsub("\\s+", "", s))
  nz >= min_chars
}

guess_schema <- function(cols) {
  ncols <- normalize_names(cols)
  pick <- function(cands) { hit <- which(ncols %in% cands)[1]; if (is.na(hit)) "" else cols[hit] }
  list(
    claim_id      = pick(c("claim_id","claimnumber","claim_no","claim","claim_num")),
    loss_date     = pick(c("loss_date","date_of_loss","incident_date","occurrence_date","lossdt","lossdate")),
    report_date   = pick(c("report_date","reported_date","notif_date","date_reported","reported")),
    paid          = pick(c("paid","paid_amount","amount_paid","total_paid","paid_total")),
    incurred      = pick(c("incurred","incurred_amount","total_incurred","reserve_plus_paid","paid_incurred")),
    outstanding   = pick(c("outstanding","case_outstanding","case_reserve","reserve","outstanding_amount")),
    currency      = pick(c("currency","curr","ccy")),
    policy_number = pick(c("policy_number","policynumber","policy_no","policy","policyid")),
    status        = pick(c("status","claim_status","open_closed","state")),
    claimant      = pick(c("claimant","insured_name","name","customer","insured"))
  )
}

parse_date_safe <- function(x, loc) {
  if (inherits(x, "Date")) return(x)
  if (inherits(x, "POSIXt")) return(as.Date(x))
  if (is.numeric(x)) {
    ser <- as.numeric(x)
    ok <- !is.na(ser) & ser > 20000 & ser < 60000
    out <- rep(as.Date(NA), length(ser))
    out[ok] <- as.Date(ser[ok], origin = "1899-12-30")
    return(out)
  }
  orders <- loc$date_orders
  y <- suppressWarnings(lubridate::parse_date_time(x, orders = orders))
  as.Date(y)
}

clean_num_locale <- function(z, loc) {
  s <- as.character(z)
  s <- gsub("\u2212", "-", s, fixed = TRUE)
  if (nzchar(loc$big_mark)) s <- gsub(paste0("\\", loc$big_mark), "", s)
  s <- gsub("[\\$\\s\\(\\)]", "", s)
  s <- gsub("[^0-9\\.-]", "", s)
  neg_paren <- grepl("^\\s*\\(.*\\)\\s*$", as.character(z))
  out <- suppressWarnings(as.numeric(s))
  out[neg_paren & !is.na(out)] <- -abs(out[neg_paren & !is.na(out)])
  out
}

mask_pii <- function(df){
  if ("claimant" %in% names(df)) {
    df$claimant <- stringr::str_replace_all(as.character(df$claimant), "[A-Za-z]", "x")
  }
  if ("policy_number" %in% names(df)) {
    df$policy_number <- vapply(
      as.character(df$policy_number),
      function(x) digest::hmac(MASK_SALT, x, algo = "sha256"),
      FUN.VALUE = character(1)
    )
  }
  df
}

promote_header_if <- function(df, enabled = FALSE){
  if (!isTRUE(enabled)) return(df)
  if (!is.data.frame(df) || nrow(df) < 2) return(df)
  hdr <- as.character(df[1, , drop = TRUE])
  non_empty <- sum(hdr != "" & !is.na(hdr))
  if (non_empty >= max(2, floor(0.6 * ncol(df)))) {
    names(df) <- make.names(trimws(hdr), unique = TRUE)
    df <- df[-1, , drop = FALSE]
  }
  df
}

# ---- Parse cache for PDFs (RDS by file hash) ----
read_pdf_from_cache <- function(path, cache_key){
  cache_dir <- file.path(tempdir(), "brokerops_parse_cache")
  file <- file.path(cache_dir, paste0(cache_key, ".rds"))
  if (file.exists(file)) {
    out <- tryCatch(readRDS(file), error = function(e) NULL)
    if (!is.null(out)) return(out)
  }
  NULL
}
write_pdf_to_cache <- function(obj, cache_key){
  cache_dir <- file.path(tempdir(), "brokerops_parse_cache")
  dir.create(cache_dir, showWarnings = FALSE, recursive = TRUE)
  file <- file.path(cache_dir, paste0(cache_key, ".rds"))
  try(saveRDS(obj, file = file), silent = TRUE)
}

# ---- Read uploaded (with optional Cloud OCR) ----
read_uploaded_one <- function(path, ext, use_cloud_ocr = FALSE, ocr_provider = "azure") {
  ext <- tolower(ext)
  if (ext == "csv") {
    if (is_zip_magic(path) || is_pdf_magic(path)) stop("File looks like ZIP/PDF, not CSV.")
    return(R.utils::withTimeout({
      if (requireNamespace("vroom", quietly = TRUE)) {
        vroom::vroom(path, altrep = TRUE, show_col_types = FALSE)
      } else {
        readr::read_csv(path, show_col_types = FALSE)
      }
    }, timeout = READ_TIMEOUT_SEC, onTimeout = "error"))
  }
  if (ext %in% c("xlsx","xls")) {
    if (ext == "xlsx" && !(is_zip_magic(path))) stop("XLSX file is not a ZIP container.")
    if (ext == "xls"  && !(is_ole_magic(path) || is_zip_magic(path))) stop("XLS file does not have expected OLE/ZIP signature.")
    return(R.utils::withTimeout(readxl::read_excel(path), timeout = READ_TIMEOUT_SEC, onTimeout = "error"))
  }
  if (ext == "pdf") {
    if (!is_pdf_magic(path)) stop("Not a valid PDF header.")
    # parse cache key considers whether OCR is requested
    cache_key <- paste0(file_sha256(path), "_ocr_", as.integer(isTRUE(use_cloud_ocr)))
    cached <- read_pdf_from_cache(path, cache_key)
    if (!is.null(cached)) return(cached)
    # 1) Tabulizer page-by-page
    if (has_tabulizer) {
      np <- tryCatch(pdftools::pdf_info(path)$pages, error = function(e) 0L)
      if (np > 0) {
        dfs <- list()
        for (pg in seq_len(np)) {
          tabs <- tryCatch(R.utils::withTimeout(
            tabulizer::extract_tables(path, pages = pg, guess = TRUE),
            timeout = READ_TIMEOUT_SEC, onTimeout = "silent"), error = function(e) NULL)
          if (!is.null(tabs) && length(tabs) > 0) {
            for (tb in tabs) {
              df <- as.data.frame(tb, stringsAsFactors = FALSE)
              if (ncol(df) >= 3) {
                if (nrow(df) > 1) {
                  header_guess <- df[1, , drop = FALSE]
                  non_empty <- sum(header_guess != "" & !is.na(header_guess))
                  if (non_empty >= max(2, floor(0.6 * ncol(df)))) {
                    names(df) <- as.character(unlist(header_guess)); df <- df[-1, , drop = FALSE]
                  }
                }
                names(df) <- make.names(names(df), unique = TRUE)
                df$..source_page.. <- pg
                dfs[[length(dfs) + 1]] <- df
              }
            }
          }
        }
        dfs <- dfs[!vapply(dfs, is.null, logical(1))]
        if (length(dfs)) {
          out <- dplyr::bind_rows(dfs)
          write_pdf_to_cache(out, cache_key)
          return(out)
        }
      }
    }
    # 2) Skip OCR if selectable text is present
    if (isTRUE(use_cloud_ocr) && pdf_has_selectable_text(path)) {
      df_attr <- tibble::tibble(text = unlist(strsplit(paste(tryCatch(pdftools::pdf_text(path), error=function(e) ""), collapse="\n"), "\n", fixed=TRUE)))
      attr(df_attr, "ocr_skipped") <- TRUE
      write_pdf_to_cache(df_attr, cache_key)
      return(df_attr)
    }
    # 3) Cloud OCR
    if (isTRUE(use_cloud_ocr)) {
      prov <- tolower(or_null(ocr_provider, OCR_PROVIDER))
      if (get_ocr_ok(prov)()) {
        df_ocr <- tryCatch(get_ocr_fn(prov)(path), error = function(e){ message("OCR backend error: ", e$message); NULL })
        if (!is.null(df_ocr) && is.data.frame(df_ocr) && ncol(df_ocr) >= 3) {
          write_pdf_to_cache(df_ocr, cache_key)
          return(df_ocr)
        }
      }
    }
    # 4) Selectable text (no OCR)
    txt <- tryCatch(pdftools::pdf_text(path), error = function(e) character())
    if (length(txt) && any(nzchar(txt))) {
      lines <- unlist(strsplit(txt, "\n"))
      out <- tibble::tibble(text = lines)
      write_pdf_to_cache(out, cache_key)
      return(out)
    }
    # 5) Tesseract fallback
    npages <- tryCatch(pdftools::pdf_info(path)$pages, error = function(e) 0L)
    if (npages > 0 && requireNamespace("tesseract", quietly = TRUE)) {
      imgs <- pdftools::pdf_convert(path, dpi = 200)
      ocr_engine <- tesseract::tesseract("eng")
      dfs <- list()
      for (i in seq_along(imgs)) {
        ocr_txt <- tryCatch(tesseract::ocr(imgs[i], engine = ocr_engine), error = function(e) "")
        lines <- unlist(strsplit(ocr_txt, "\n"))
        if (length(lines)) dfs[[length(dfs)+1]] <- tibble::tibble(text = lines, ..source_page.. = i)
      }
      unlink(imgs, force = TRUE)
      if (length(dfs)) {
        out <- dplyr::bind_rows(dfs)
        write_pdf_to_cache(out, cache_key)
        return(out)
      }
    }
    stop("PDF could not be parsed (no tables/text/OCR available).")
  }
  stop("Unsupported file type. Upload CSV/XLSX/PDF.")
}

standardize_df <- function(df, map, source_file = NA_character_, loc) {
  std <- tibble::tibble(
    claim_id      = if (map$claim_id      != "") as.character(df[[map$claim_id]]) else NA_character_,
    loss_date     = if (map$loss_date     != "") df[[map$loss_date]] else NA,
    report_date   = if (map$report_date   != "") df[[map$report_date]] else NA,
    paid          = if (map$paid          != "") df[[map$paid]] else NA,
    incurred      = if (map$incurred      != "") df[[map$incurred]] else NA,
    outstanding   = if (map$outstanding   != "") df[[map$outstanding]] else NA,
    currency      = if (map$currency      != "") df[[map$currency]] else NA,
    policy_number = if (map$policy_number != "") df[[map$policy_number]] else NA,
    status        = if (map$status        != "") df[[map$status]] else NA,
    claimant      = if (map$claimant      != "") df[[map$claimant]] else NA,
    source_file   = source_file
  )
  std$source_page <- if ("..source_page.." %in% names(df)) as.integer(df[["..source_page.."]]) else NA_integer_
  std$loss_date   <- parse_date_safe(std$loss_date, loc)
  std$report_date <- parse_date_safe(std$report_date, loc)
  std$paid        <- clean_num_locale(std$paid, loc)
  std$incurred    <- clean_num_locale(std$incurred, loc)
  std$outstanding <- clean_num_locale(std$outstanding, loc)
  synth_idx <- which(is.na(std$incurred) & !is.na(std$paid) & !is.na(std$outstanding))
  if (length(synth_idx)) std$incurred[synth_idx] <- std$paid[synth_idx] + std$outstanding[synth_idx]
  std$currency      <- toupper(trimws(as.character(std$currency)))
  std$policy_number <- as.character(std$policy_number)
  std$status        <- as.character(std$status)
  std$claimant      <- as.character(std$claimant)
  # per-tenant stamp
  std$tenant_id     <- TENANT_ID
  unmasked <- std
  std <- mask_pii(std)
  attr(std, "unmasked") <- unmasked
  attr(std, "original") <- df
  std
}

# ---------- Data Quality Summary ----------
.format_num <- function(x) { if (is.null(x) || is.na(x)) return("NA"); format(round(as.numeric(x),2), big.mark=",", scientific=FALSE) }
.decimal_consistency <- function(x) { z <- suppressWarnings(as.numeric(x)); z <- z[!is.na(z)]; if (!length(z)) return(TRUE); all(abs(z - round(z, 2)) < 1e-9) }
.exact_row_dups <- function(std) {
  key <- apply(cbind(
    norm_id(std$claim_id),
    as.character(std$loss_date),
    as.character(std$report_date),
    round(coalesce(std$paid, 0), 2),
    round(coalesce(std$incurred, 0), 2),
    round(coalesce(std$outstanding, 0), 2),
    toupper(coalesce(std$currency, "")),
    norm_id(std$policy_number),
    norm_id(std$status),
    norm_id(std$claimant)
  ), 1, paste, collapse="|")
  which(duplicated(key))
}

build_quality_summary <- function(std_df, policy_start, policy_end){
  n_rows   <- nrow(std_df)
  n_claims <- dplyr::n_distinct(std_df$claim_id)
  clm <- std_df %>% filter(!is.na(claim_id) & nzchar(claim_id)) %>%
    group_by(claim_id) %>%
    summarise(paid_c = sum(coalesce(paid, 0), na.rm=TRUE),
              incurred_c = sum(coalesce(incurred, 0), na.rm=TRUE),
              os_c = sum(coalesce(incurred, 0) - coalesce(paid, 0), na.rm=TRUE), .groups="drop")
  total_paid_sum     <- sum(coalesce(std_df$paid, 0), na.rm=TRUE)
  total_incurred_sum <- sum(coalesce(std_df$incurred, 0), na.rm=TRUE)
  severity_claim <- if (n_claims > 0) total_paid_sum / n_claims else NA_real_
  tau <- 0.01
  open_n      <- sum(clm$os_c >  tau, na.rm=TRUE)
  closed_n    <- sum(clm$os_c <= tau, na.rm=TRUE)
  open_os     <- sum(clm$os_c[clm$os_c > tau], na.rm=TRUE)
  closed_paid <- sum(clm$paid_c[clm$os_c <= tau], na.rm=TRUE)
  neg_paid_n   <- sum(!is.na(std_df$paid) & std_df$paid < 0)
  neg_paid_sum <- sum(std_df$paid[!is.na(std_df$paid) & std_df$paid < 0], na.rm=TRUE)
  neg_os_n     <- sum(!is.na(std_df$outstanding) & std_df$outstanding < 0)
  neg_os_sum   <- sum(std_df$outstanding[!is.na(std_df$outstanding) & std_df$outstanding < 0], na.rm=TRUE)
  zero_claims  <- sum(clm$paid_c == 0 & clm$incurred_c == 0, na.rm=TRUE)
  inc_lt_rows <- which(!is.na(std_df$incurred) & !is.na(std_df$paid) & (std_df$incurred + 1e-9) < std_df$paid)
  inc_lt_rows_n    <- length(inc_lt_rows)
  inc_lt_claims_n  <- length(unique(std_df$claim_id[inc_lt_rows]))
  inc_lt_variance  <- sum((std_df$paid[inc_lt_rows] - std_df$incurred[inc_lt_rows]), na.rm=TRUE)
  os_vec <- if ("outstanding" %in% names(std_df)) coalesce(std_df$outstanding, 0) else coalesce(std_df$incurred,0) - coalesce(std_df$paid,0)
  recon_rows <- which(!is.na(std_df$paid) & !is.na(std_df$incurred) & !is.na(os_vec) & abs(std_df$incurred - (std_df$paid + os_vec)) > 0.01)
  recon_rows_n    <- length(recon_rows)
  recon_abs_delta <- sum(abs(std_df$incurred[recon_rows] - (std_df$paid[recon_rows] + os_vec[recon_rows])), na.rm=TRUE)
  outlier_k <- 1.5
  paid_pos  <- std_df$paid[!is.na(std_df$paid) & std_df$paid >= 0]
  outliers_n <- 0; top_loss <- NA; q3 <- NA; iqr <- NA
  if (length(paid_pos) >= 5) {
    qs  <- stats::quantile(paid_pos, probs=c(0.25,0.75), na.rm=TRUE)
    iqr <- qs[2]-qs[1]; q3 <- qs[2]
    fence <- q3 + outlier_k*iqr
    idx <- which(!is.na(std_df$paid) & std_df$paid > fence)
    outliers_n <- length(idx)
    top_loss   <- if (outliers_n) max(std_df$paid[idx], na.rm=TRUE) else NA
  }
  unparse_loss          <- sum(is.na(std_df$loss_date))
  unparse_report        <- sum(is.na(std_df$report_date))
  report_before_loss    <- sum(!is.na(std_df$loss_date) & !is.na(std_df$report_date) & std_df$report_date < std_df$loss_date)
  pol_term_set <- (!is.null(policy_start) && !is.null(policy_end) && !is.na(policy_start) && !is.na(policy_end))
  outside_term_n <- NA_integer_; outside_term_pct <- NA_real_
  if (pol_term_set) {
    outside_term_n   <- sum(!is.na(std_df$loss_date) & (std_df$loss_date < as.Date(policy_start) | std_df$loss_date > as.Date(policy_end)))
    outside_term_pct <- if (n_rows) round(100 * outside_term_n / n_rows, 1) else NA_real_
  }
  currencies   <- sort(unique(na.omit(std_df$currency)))
  currency_ok  <- all(currencies %in% ALLOWED_CURRENCIES)
  decimals_ok  <- .decimal_consistency(std_df$paid) && .decimal_consistency(std_df$incurred)
  exact_dup_n            <- length(.exact_row_dups(std_df))
  dup_claimid_lossdate_n <- sum(duplicated(paste(std_df$claim_id, std_df$loss_date)))
  req_missing <- list(
    claim_id      = sum(is.na(std_df$claim_id)      | !nzchar(std_df$claim_id)),
    loss_date     = sum(is.na(std_df$loss_date)),
    report_date   = sum(is.na(std_df$report_date)),
    policy_number = sum(is.na(std_df$policy_number) | !nzchar(std_df$policy_number)),
    currency      = sum(is.na(std_df$currency)      | !nzchar(std_df$currency)),
    status        = if ("status" %in% names(std_df)) sum(is.na(std_df$status) | !nzchar(std_df$status)) else NA_integer_
  )
  ds_gap <- abs(total_incurred_sum - (total_paid_sum + sum(coalesce(os_vec,0), na.rm=TRUE)))
  ds_tol <- max(100, 0.001 * max(1, total_incurred_sum))
  ds_ok  <- ds_gap <= ds_tol
  list(
    n_rows=n_rows, n_claims=n_claims,
    total_paid=total_paid_sum, total_incurred=total_incurred_sum, severity_claim=severity_claim,
    open_n=open_n, closed_n=closed_n, open_os=open_os, closed_paid=closed_paid,
    neg_paid_n=neg_paid_n, neg_paid_sum=neg_paid_sum, neg_os_n=neg_os_n, neg_os_sum=neg_os_sum, zero_claims=zero_claims,
    inc_lt_rows_n=inc_lt_rows_n, inc_lt_claims_n=inc_lt_claims_n, inc_lt_variance=inc_lt_variance,
    recon_rows_n=recon_rows_n, recon_abs_delta=recon_abs_delta,
    outlier_k=outlier_k, outliers_n=outliers_n, top_loss=top_loss, iqr=iqr, q3=q3,
    unparse_loss=unparse_loss, unparse_report=unparse_report, report_before_loss=report_before_loss,
    pol_term_set=pol_term_set, policy_start=policy_start, policy_end=policy_end,
    outside_term_n=outside_term_n, outside_term_pct=outside_term_pct,
    currencies=currencies, currency_ok=currency_ok, decimals_ok=decimals_ok,
    exact_dup_n=exact_dup_n, dup_claimid_lossdate_n=dup_claimid_lossdate_n,
    req_missing=req_missing, dataset_recon_ok=ds_ok, dataset_recon_gap=ds_gap, dataset_recon_tol=ds_tol
  )
}

# ---------- Row-level validator ----------
validate_claims <- function(std_df, cfg, policy_start = NULL, policy_end = NULL) {
  issues <- tibble::tibble(row = integer(), claim_id = character(), issue = character(), detail = character())
  add_issue <- function(rows, msg, detail = "") {
    tibble::tibble(row = rows, claim_id = std_df$claim_id[rows], issue = msg, detail = detail)
  }
  n <- nrow(std_df)
  if (n == 0) return(list(issues = issues, metrics = list()))
  
  if (isTRUE(cfg$rule_required_fields)) {
    req_missing_rows <- which(is.na(std_df$claim_id) | std_df$claim_id == "" |
                                is.na(std_df$loss_date) | is.na(std_df$report_date))
    if (length(req_missing_rows)) issues <- bind_rows(issues, add_issue(req_missing_rows, "Missing required fields", "claim_id/loss_date/report_date"))
  }
  if (isTRUE(cfg$rule_negative_values)) {
    if (any(std_df$paid < 0, na.rm = TRUE))
      issues <- bind_rows(issues, add_issue(which(!is.na(std_df$paid) & std_df$paid < 0), "Negative paid", "paid < 0"))
    if (any(std_df$incurred < 0, na.rm = TRUE))
      issues <- bind_rows(issues, add_issue(which(!is.na(std_df$incurred) & std_df$incurred < 0), "Negative incurred", "incurred < 0"))
    if (any(std_df$outstanding < 0, na.rm = TRUE))
      issues <- bind_rows(issues, add_issue(which(!is.na(std_df$outstanding) & std_df$outstanding < 0), "Negative outstanding", "outstanding < 0"))
  }
  if (isTRUE(cfg$rule_incurred_lt_paid)) {
    bad_ratio <- which(!is.na(std_df$incurred) & !is.na(std_df$paid) & (std_df$incurred + 1e-9) < std_df$paid)
    if (length(bad_ratio)) issues <- bind_rows(issues, add_issue(bad_ratio, "Incurred less than Paid", "incurred < paid"))
  }
  if (isTRUE(cfg$rule_reconcile)) {
    recon_idx <- which(!is.na(std_df$paid) & !is.na(std_df$outstanding) & !is.na(std_df$incurred) &
                         abs(std_df$incurred - (std_df$paid + std_df$outstanding)) > 0.01)
    if (length(recon_idx)) issues <- bind_rows(issues, add_issue(recon_idx, "Incurred != Paid + Outstanding", "reconciliation mismatch"))
  }
  if (isTRUE(cfg$rule_currency_list)) {
    bad_ccy <- which(!is.na(std_df$currency) & !(std_df$currency %in% ALLOWED_CURRENCIES))
    if (length(bad_ccy)) issues <- bind_rows(issues, add_issue(bad_ccy, "Unrecognised currency", paste("Allowed:", paste(ALLOWED_CURRENCIES, collapse=", "))))
  }
  if (isTRUE(cfg$rule_duplicates)) {
    dup_idx <- which(duplicated(paste(norm_id(std_df$claim_id), std_df$loss_date)))
    if (length(dup_idx)) issues <- bind_rows(issues, add_issue(dup_idx, "Potential duplicate claim rows", "duplicate claim_id + loss_date"))
  }
  if (isTRUE(cfg$rule_date_order)) {
    bad_dates <- which(!is.na(std_df$loss_date) & !is.na(std_df$report_date) & std_df$loss_date > std_df$report_date)
    if (length(bad_dates)) issues <- bind_rows(issues, add_issue(bad_dates, "Loss date after report date", "loss_date > report_date"))
  }
  if (isTRUE(cfg$rule_policy_term) && !is.null(policy_start) && !is.null(policy_end) && !is.na(policy_start) && !is.na(policy_end)) {
    out_of_term <- which(!is.na(std_df$loss_date) & (std_df$loss_date < as.Date(policy_start) | std_df$loss_date > as.Date(policy_end)))
    if (length(out_of_term)) issues <- bind_rows(issues, add_issue(out_of_term, "Loss date outside policy term", paste("Policy:", policy_start, "to", policy_end)))
  }
  if (isTRUE(cfg$rule_missing_policy)) {
    miss_pol <- which(is.na(std_df$policy_number) | std_df$policy_number == "")
    if (length(miss_pol)) issues <- bind_rows(issues, add_issue(miss_pol, "Missing policy number", ""))
  }
  if (isTRUE(cfg$rule_missing_currency)) {
    miss_ccy <- which(is.na(std_df$currency) | std_df$currency == "")
    if (length(miss_ccy)) issues <- bind_rows(issues, add_issue(miss_ccy, "Missing currency", ""))
  }
  if (isTRUE(cfg$rule_paid_outliers)) {
    paid_ok <- std_df$paid[!is.na(std_df$paid)]
    if (length(paid_ok) >= 5) {
      q <- stats::quantile(paid_ok, probs = c(0.25, 0.75), na.rm = TRUE)
      iqr <- q[2] - q[1]; mult <- or_null(cfg$iqr_multiplier, 1.5)
      upper <- q[2] + mult * iqr; lower <- max(0, q[1] - mult * iqr)
      outl <- which(!is.na(std_df$paid) & (std_df$paid > upper | std_df$paid < lower))
      if (length(outl)) issues <- bind_rows(issues, add_issue(outl, "Paid outlier", glue::glue("IQR fence: [{round(lower,2)}..{round(upper,2)}] (mult={mult})")))
    }
  }
  if (isTRUE(cfg$rule_high_ratio)) {
    thr <- or_null(cfg$ratio_threshold, 2)
    high_ratio <- which(!is.na(std_df$incurred) & !is.na(std_df$paid) & std_df$paid > 0 & (std_df$incurred/std_df$paid) > thr)
    if (length(high_ratio)) issues <- bind_rows(issues, add_issue(high_ratio, "High incurred-to-paid ratio", paste0("ratio > ", thr)))
  }
  if (isTRUE(cfg$rule_missing_claimant)) {
    miss_claimant <- which(is.na(std_df$claimant) | std_df$claimant == "")
    if (length(miss_claimant)) issues <- bind_rows(issues, add_issue(miss_claimant, "Missing claimant/insured name", ""))
  }
  if (isTRUE(cfg$rule_zero_rows)) {
    zero_amounts <- which(!is.na(std_df$paid) & !is.na(std_df$incurred) & std_df$paid == 0 & std_df$incurred == 0)
    if (length(zero_amounts)) issues <- bind_rows(issues, add_issue(zero_amounts, "Zero paid and incurred", ""))
  }
  if (isTRUE(cfg$rule_parse_dates)) {
    bad_loss_parse <- which(is.na(std_df$loss_date))
    bad_rep_parse  <- which(is.na(std_df$report_date))
    if (length(bad_loss_parse)) issues <- bind_rows(issues, add_issue(bad_loss_parse, "Unparseable loss_date", "check date format"))
    if (length(bad_rep_parse))  issues <- bind_rows(issues, add_issue(bad_rep_parse, "Unparseable report_date", "check date format"))
  }
  if (isTRUE(cfg$rule_multi_rows_per_claim)) {
    multi_rows <- std_df %>% mutate(rn = row_number()) %>% group_by(claim_id) %>%
      summarise(n = n(), rows = paste(rn, collapse = ", "), .groups = "drop") %>% filter(n > 1)
    if (nrow(multi_rows) > 0) {
      for (i in seq_len(nrow(multi_rows))) {
        rows <- as.integer(strsplit(multi_rows$rows[i], ", ")[[1]])
        issues <- bind_rows(issues, add_issue(rows, "Multiple rows for claim_id", paste0("n=", multi_rows$n[i])))
      }
    }
  }
  metrics <- list(
    n_rows = nrow(std_df),
    n_claims = dplyr::n_distinct(std_df$claim_id),
    total_paid = sum(std_df$paid, na.rm = TRUE),
    total_incurred = sum(std_df$incurred, na.rm = TRUE),
    avg_severity = mean(std_df$paid, na.rm = TRUE),
    missing_required = nrow(dplyr::filter(issues, issue == "Missing required fields")),
    duplicates = nrow(dplyr::filter(issues, issue == "Potential duplicate claim rows")),
    outliers = nrow(dplyr::filter(issues, issue == "Paid outlier"))
  )
  list(issues = issues, metrics = metrics)
}

# ---- OpenAI Responses API helpers with simple rate-limit state ----
extract_responses_text <- function(parsed){
  if (!is.null(parsed$output)) {
    texts <- c()
    for (msg in parsed$output) {
      if (!is.null(msg$content)) {
        for (c in msg$content) {
          if (!is.null(c$text)) texts <- c(texts, as.character(c$text))
          else if (!is.null(c$body)) texts <- c(texts, as.character(c$body))
        }
      }
    }
    if (length(texts)) return(paste(texts, collapse = "\n"))
  }
  if (!is.null(parsed$output_text)) return(as.character(parsed$output_text))
  if (!is.null(parsed$choices) && length(parsed$choices) >= 1){
    ch <- parsed$choices[[1]]
    if (!is.null(ch$message$content)) return(as.character(ch$message$content))
    if (!is.null(ch$text)) return(as.character(ch$text))
  }
  paste(capture.output(str(parsed)), collapse = "\n")
}

OPENAI_API_KEY <- env_str("OPENAI_API_KEY", "")

generate_gpt_explainer <- function(Q) {
  key <- OPENAI_API_KEY
  if (!nzchar(key)) return("OpenAI key not found. Set it via .Renviron (OPENAI_API_KEY=sk-...) or Sys.setenv before running.")
  pol_line <- if (isTRUE(Q$pol_term_set))
    glue("Policy term: {format(as.Date(Q$policy_start))} to {format(as.Date(Q$policy_end))}.")
  else "Policy term: not specified."
  prompt <- glue(
    "Summarize this loss-run/bordereaux for an underwriter in 5 short bullets.\nBe explicit about the math and include counts and dollars.\n\nVolume & severity:\n- {Q$n_claims} unique claims across {Q$n_rows} rows.\n- Severity = total paid ÷ unique claims = ${.format_num(Q$severity_claim)}.\n\nOpen vs. closed:\n- Open: {Q$open_n} claims; Outstanding (incurred − paid) = ${.format_num(Q$open_os)}.\n- Closed: {Q$closed_n} claims; Paid on closed = ${.format_num(Q$closed_paid)}.\n\nAnomalies (quantified):\n- Incurred < Paid: {Q$inc_lt_rows_n} rows / {Q$inc_lt_claims_n} claims; total variance = ${.format_num(Q$inc_lt_variance)}.\n- Reconciliation mismatches (incurred vs paid + OS): {Q$recon_rows_n} rows; total |delta| = ${.format_num(Q$recon_abs_delta)}.\n\nData quality:\n- Negatives — paid {Q$neg_paid_n} (sum ${.format_num(Q$neg_paid_sum)}), outstanding {Q$neg_os_n} (sum ${.format_num(Q$neg_os_sum)}); zero-amount claims {Q$zero_claims}.\n- Dates — unparseable loss {Q$unparse_loss}, unparseable report {Q$unparse_report}; report<loss {Q$report_before_loss}; {if (Q$pol_term_set) glue('outside policy term {Q$outside_term_n} rows ({Q$outside_term_pct}%).') else 'policy term not provided.'}\n- Currency — set [{paste(Q$currencies, collapse=', ')}]; allowed={Q$currency_ok}; decimals consistent (≤2)={Q$decimals_ok}.\n- Duplicates — exact row {Q$exact_dup_n}; claim_id+loss_date {Q$dup_claimid_lossdate_n}.\n- Dataset sanity: sum(incurred) vs sum(paid + OS) → {if (Q$dataset_recon_ok) 'OK' else paste0('gap $', .format_num(Q$dataset_recon_gap))}.\n\n{pol_line}\n\nReturn only the 5 bullets."
  )
  body <- list(model = "gpt-4o-mini", input = as.character(prompt), temperature = 0.2)
  resp <- tryCatch({
    httr::RETRY("POST","https://api.openai.com/v1/responses",
                httr::add_headers(Authorization = paste("Bearer", key), `Content-Type` = "application/json", `User-Agent` = "BrokerOpsAI/1.0"),
                body=jsonlite::toJSON(body, auto_unbox=TRUE), times=3, pause_min=1, terminate_on=c(400,401,403,404),
                httr::timeout(20))
  }, error=function(e) e)
  if (inherits(resp,"error")) return(paste("Network error:", resp$message))
  txt <- httr::content(resp, as="text", encoding="UTF-8")
  parsed <- tryCatch(jsonlite::fromJSON(txt, simplifyVector=FALSE), error=function(e) NULL)
  if (is.null(parsed)) return(substr(txt,1,400))
  if (httr::status_code(resp) >= 400) {
    msg <- tryCatch(parsed$error$message, error=function(e) substr(txt,1,200))
    return(paste("OpenAI error:", msg))
  }
  out <- tryCatch(extract_responses_text(parsed), error=function(e) substr(txt,1,400))
  if (!nzchar(out)) out <- "(AI explanation unavailable.)"
  out
}

# ---- Simple pivots & triangles ----
make_pivots <- function(std) {
  std %>% mutate(ay = lubridate::year(loss_date)) %>%
    group_by(ay, currency) %>%
    summarise(
      rows        = n(),
      claims      = n_distinct(claim_id),
      paid        = sum(paid, na.rm = TRUE),
      incurred    = sum(incurred, na.rm = TRUE),
      outstanding = sum(outstanding, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(desc(ay), currency)
}
make_triangle_counts <- function(std, max_dev_months = 36L) {
  df <- std %>% filter(!is.na(loss_date), !is.na(report_date), !is.na(claim_id), nzchar(claim_id)) %>%
    mutate(ay = lubridate::year(loss_date),
           dev_mon = pmax(0L, as.integer(lubridate::interval(
             lubridate::floor_date(loss_date, "month"),
             lubridate::floor_date(report_date, "month")
           ) %/% months(1)))) %>% filter(dev_mon <= max_dev_months) %>% distinct(claim_id, ay, dev_mon)
  df %>% count(ay, dev_mon, name = "n_claims") %>% tidyr::pivot_wider(names_from = dev_mon, values_from = n_claims, values_fill = 0) %>% arrange(ay)
}
make_triangle_amounts <- function(std, field = c("paid","incurred"), max_dev_months = 36L) {
  field <- match.arg(field)
  df <- std %>% filter(!is.na(loss_date), !is.na(report_date)) %>%
    mutate(ay = lubridate::year(loss_date),
           dev_mon = pmax(0L, as.integer(lubridate::interval(
             lubridate::floor_date(loss_date, "month"),
             lubridate::floor_date(report_date, "month")
           ) %/% months(1)))) %>% filter(dev_mon <= max_dev_months)
  df %>% group_by(ay, dev_mon) %>% summarise(val = sum(.data[[field]], na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(names_from = dev_mon, values_from = val, values_fill = 0) %>% arrange(ay)
}

# ---- Export format presets (lightweight, indicative) ----
format_date_for <- function(d, style = c("iso","au")) {
  style <- match.arg(style)
  if (style == "iso") return(format(as.Date(d), "%Y-%m-%d"))
  format(as.Date(d), "%d/%m/%Y")
}
transform_export <- function(std, format = c("standard_csv","ebix_v1","lloyds_v5_2","sunrise_basic"), locale = "AU") {
  fmt <- match.arg(format)
  out <- std
  # ensure run_id exists for stamping
  if (!"run_id" %in% names(out)) out$run_id <- NA_character_
  if (!"tenant_id" %in% names(out)) out$tenant_id <- TENANT_ID
  
  if (fmt == "standard_csv") {
    out <- out %>% select(run_id, tenant_id, claim_id, loss_date, report_date, paid, incurred, outstanding, currency, policy_number, status, claimant, source_file, source_page)
    out$loss_date   <- format_date_for(out$loss_date, "iso")
    out$report_date <- format_date_for(out$report_date, "iso")
    names(out) <- c("run_id","tenant_id","claim_id","loss_date","report_date","paid","incurred","outstanding","currency","policy_number","status","claimant_masked","source_file","source_page")
    return(out)
  }
  if (fmt == "ebix_v1") {
    out <- out %>% transmute(
      ClaimNumber = claim_id,
      DateOfLoss = format_date_for(loss_date, "au"),
      DateReported = format_date_for(report_date, "au"),
      Paid = round(paid %||% NA_real_, 2),
      Incurred = round(incurred %||% NA_real_, 2),
      Outstanding = round(outstanding %||% (incurred - paid), 2),
      Currency = currency,
      PolicyNumber = policy_number,
      Status = status,
      ClaimantMasked = claimant
    )
    return(out)
  }
  if (fmt == "lloyds_v5_2") {
    out <- out %>% transmute(
      Claim_Reference = claim_id,
      Loss_Date = format_date_for(loss_date, "iso"),
      Report_Date = format_date_for(report_date, "iso"),
      Paid_To_Date = round(paid %||% NA_real_, 2),
      Incurred_To_Date = round(incurred %||% NA_real_, 2),
      Outstanding_Reserves = round((incurred - paid) %||% NA_real_, 2),
      Currency = currency,
      Policy_Number = policy_number
    )
    return(out)
  }
  if (fmt == "sunrise_basic") {
    out <- out %>% transmute(
      ClaimID = claim_id,
      LossDate = format_date_for(loss_date, "au"),
      ReportDate = format_date_for(report_date, "au"),
      Paid = round(paid %||% NA_real_, 2),
      Incurred = round(incurred %||% NA_real_, 2),
      OS = round(outstanding %||% (incurred - paid), 2),
      CCY = currency,
      PolicyNo = policy_number
    )
    return(out)
  }
  std
}

# ---- HTML report helper ----
make_html_report <- function(Q, issues, piv, tri_counts, tri_paid, tri_incurred, ai_text, run_id){
  summary_txt <- glue::glue(
    "Rows: {Q$n_rows}\nUnique claims: {Q$n_claims}\nSeverity (total paid ÷ unique claims): ${.format_num(Q$severity_claim)}\nTotals — Paid: ${.format_num(Q$total_paid)} | Incurred: ${.format_num(Q$total_incurred)}\nOpen vs Closed — Open {Q$open_n} (OS ${.format_num(Q$open_os)}); Closed {Q$closed_n} (Paid ${.format_num(Q$closed_paid)})\nAnomalies — Incurred<Paid: {Q$inc_lt_rows_n} rows / {Q$inc_lt_claims_n} claims (variance ${.format_num(Q$inc_lt_variance)}); Recon mismatches: {Q$recon_rows_n} rows (|delta| ${.format_num(Q$recon_abs_delta)})\nOutliers — IQR rule (paid > Q3 + {Q$outlier_k}×IQR); n={Q$outliers_n}; top loss ${.format_num(Q$top_loss)}\nNegatives — paid {Q$neg_paid_n} (sum ${.format_num(Q$neg_paid_sum)}); outstanding {Q$neg_os_n} (sum ${.format_num(Q$neg_os_sum)}); zero-amount claims {Q$zero_claims}\nDates — unparseable loss {Q$unparse_loss}, unparseable report {Q$unparse_report}; report<loss {Q$report_before_loss}; {if (Q$pol_term_set) glue('outside policy term {Q$outside_term_n} rows ({Q$outside_term_pct}%).') else 'policy term not provided.'}\nCurrency — set [{paste(Q$currencies, collapse=', ')}]; allowed={Q$currency_ok}; decimals≤2={Q$decimals_ok}\nDuplicates — exact row {Q$exact_dup_n}; claim_id+loss_date {Q$dup_claimid_lossdate_n}\nRequired fields missing — claim_id={Q$req_missing$claim_id}, loss_date={Q$req_missing$loss_date}, report_date={Q$req_missing$report_date}, policy_number={Q$req_missing$policy_number}, currency={Q$req_missing$currency}{if(!is.na(Q$req_missing$status)) glue(', status={Q$req_missing$status}') else ''}.\nDataset sanity — sum(incurred) vs sum(paid + OS): {if (Q$dataset_recon_ok) paste0('OK (tol $', .format_num(Q$dataset_recon_tol), ')') else paste0('gap $', .format_num(Q$dataset_recon_gap), ' (tol $', .format_num(Q$dataset_recon_tol), ')')}"
  )
  issues_str <- if (!is.null(issues) && nrow(issues)>0) paste(capture.output(print(head(issues,30))), collapse="\n") else "No issues detected by current rules."
  piv_str <- if (!is.null(piv) && nrow(piv)>0) paste(capture.output(print(piv)), collapse="\n") else "(no pivots)"
  tri_counts_str <- if (!is.null(tri_counts) && nrow(tri_counts)>0) paste(capture.output(print(tri_counts)), collapse="\n") else "(no counts triangle)"
  tri_paid_str   <- if (!is.null(tri_paid)   && nrow(tri_paid)>0)   paste(capture.output(print(tri_paid)), collapse="\n") else "(no paid triangle)"
  tri_incurred_str <- if (!is.null(tri_incurred) && nrow(tri_incurred)>0) paste(capture.output(print(tri_incurred)), collapse="\n") else "(no incurred triangle)"
  ai_str <- or_null(ai_text, "AI explanation unavailable.")
  paste0(
    "<!DOCTYPE html><html><head><meta charset='utf-8'><title>BrokerOps AI Report</title>",
    "<style>body{font-family:Arial,Helvetica,sans-serif;padding:24px}h2,h3{margin:0.6em 0}pre{white-space:pre-wrap;background:#f7f7f7;padding:12px;border-radius:6px}</style>",
    "</head><body><h2>BrokerOps AI — Data Quality Report</h2>",
    "<p><em>Educational/analytics only — not financial product advice.</em></p>",
    "<h3>Summary</h3><pre>", htmltools::htmlEscape(summary_txt), "</pre>",
    "<h3>Top Issues</h3><pre>", htmltools::htmlEscape(issues_str), "</pre>",
    "<h3>Pivots</h3><pre>", htmltools::htmlEscape(piv_str), "</pre>",
    "<h3>Lag Triangle — Counts</h3><pre>", htmltools::htmlEscape(tri_counts_str), "</pre>",
    "<h3>Lag Triangle — Paid</h3><pre>", htmltools::htmlEscape(tri_paid_str), "</pre>",
    "<h3>Lag Triangle — Incurred</h3><pre>", htmltools::htmlEscape(tri_incurred_str), "</pre>",
    "<h3>AI Explanation</h3><pre>", htmltools::htmlEscape(ai_str), "</pre>",
    "<p style='margin-top:1em;color:#777'>Generated by BrokerOps AI v", APP_VERSION, " • Run ID ", or_null(run_id, "N/A"), ".</p>",
    "</body></html>"
  )
}

# ---------- UI ----------
app_ui <- fluidPage(
  theme = shinytheme("flatly"),
  useShinyjs(),
  titlePanel("BrokerOps AI - Loss-Run & Bordereaux (Stable)"),
  uiOutput("busy_banner"),
  tags$small(
    "Educational/analytics only — not financial product advice. ",
    "Files are processed in-memory and not stored by default. ",
    HTML("PII is anonymized by default (names are masked; policy numbers are <code>HMAC</code>-hashed with a per-run/tenant salt). "),
    "If 'AI Explanation' is enabled, only aggregate metrics (no rows/PII) are sent to OpenAI."
  ),
  br(),
  sidebarLayout(
    sidebarPanel(
      # Quick Start / Help
      tags$div(
        style="background:#f6fbff;border:1px solid #bfe1ff;padding:10px;border-radius:8px;margin-bottom:10px",
        tags$b("Quick start:"),
        tags$ul(
          tags$li("Upload loss runs as CSV/XLSX, or PDFs (tables work best)."),
          tags$li("Check the auto-detected column mapping; adjust if needed."),
          tags$li("Optional: tick 'First row is header' for messy tables."),
          tags$li("Click ", tags$b("Run Validation"), " → review Issues, Summary & downloads.")
        ),
        fluidRow(
          column(6, downloadButton("download_sample", "Sample loss-run CSV", class="btn btn-sm btn-light w-100")),
          column(6, downloadButton("download_sample_mapping", "Sample mapping JSON", class="btn btn-sm btn-light w-100"))
        )
      ),
      
      h5("Upload loss-run/bordereaux (CSV, XLSX, PDF)"),
      fileInput("files", NULL, accept = c(".csv",".xlsx",".xls",".pdf"), multiple = TRUE, buttonLabel = "Choose files"),
      checkboxInput("promote_header", "Treat first row as header (for messy PDFs/CSVs)", FALSE),
      tags$hr(),
      
      h5("Policy Term"),
      dateInput("policy_start","Policy start", value = NA),
      dateInput("policy_end","Policy end", value = NA),
      tags$hr(),
      
      h5("Validation Rules"),
      checkboxInput("show_advanced", "Show advanced settings", FALSE),
      fluidRow(
        column(6,
               checkboxInput("rule_required_fields", "Required fields", TRUE),
               checkboxInput("rule_negative_values", "No negative values", TRUE),
               checkboxInput("rule_incurred_lt_paid", "Incurred ≥ Paid", TRUE),
               checkboxInput("rule_reconcile", "Incurred = Paid + Outstanding", TRUE),
               checkboxInput("rule_currency_list", "Allowed currencies", TRUE)
        ),
        column(6,
               conditionalPanel(
                 "input.show_advanced",
                 checkboxInput("rule_duplicates", "Duplicate rows", TRUE),
                 checkboxInput("rule_date_order", "Loss date ≤ Report date", TRUE),
                 checkboxInput("rule_missing_policy", "Policy number present", TRUE),
                 checkboxInput("rule_missing_currency", "Currency present", TRUE),
                 checkboxInput("rule_missing_claimant", "Claimant present", TRUE)
               )
        )
      ),
      conditionalPanel(
        "input.show_advanced",
        fluidRow(
          column(6,
                 checkboxInput("rule_paid_outliers", "Paid outliers (IQR)", TRUE),
                 numericInput("iqr_multiplier", "IQR multiplier", value = 1.5, min = 0.5, max = 5, step = 0.1)
          ),
          column(6,
                 checkboxInput("rule_high_ratio", "High incurred/paid ratio", TRUE),
                 numericInput("ratio_threshold", "Ratio threshold", value = 2, min = 1, max = 10, step = 0.1)
          )
        ),
        checkboxInput("rule_zero_rows", "Flag zero paid & incurred", TRUE),
        checkboxInput("rule_parse_dates", "Flag unparseable dates", TRUE),
        checkboxInput("rule_policy_term", "Flag outside policy term", TRUE),
        checkboxInput("rule_multi_rows_per_claim", "Info: multiple rows for claim_id", TRUE)
      ),
      tags$hr(),
      
      uiOutput("salt_banner"),
      
      h5("Options"),
      checkboxInput("lloyds_mode", "Lloyd's mode (CRS v5.2 presets)", FALSE),
      selectInput("locale_profile", "Locale profile", choices = c("AU","US"), selected = LOCALE_DEFAULT),
      checkboxInput("enable_ai", "Enable AI Explanation (aggregates only; never rows)", FALSE),
      conditionalPanel(
        "input.enable_ai",
        div(style="background:#ffefef;border:1px solid #e7b0b0;color:#5a0000;padding:10px;border-radius:6px;margin-top:6px",
            HTML("<b>External AI is ON.</b> Only aggregate counts/sums/dates are sent.<br/>No claimant names or policy numbers are ever sent."))
      ),
      checkboxInput("download_unmasked", "Allow unmasked values in downloads (this session)", FALSE),
      tags$small(HTML("Privacy-by-default: names are masked; policy numbers are HMAC hashed with a per-run or per-tenant salt. Unmasking is opt-in per download.")),
      tags$hr(),
      
      h5("Cloud OCR / Table Extraction"),
      checkboxInput("use_cloud_ocr", "Use Cloud OCR for PDFs (sends files to your provider)", FALSE),
      conditionalPanel(
        "input.use_cloud_ocr",
        selectInput("ocr_provider", "Provider", choices = c("azure"), selected = or_null(OCR_PROVIDER, "azure")),
        div(style="background:#fff7e6;border:1px solid #ffd591;padding:8px;border-radius:6px;margin-top:6px",
            htmltools::HTML("We will upload your PDF to the selected provider for OCR/table extraction. <b>No masking can be applied before upload</b>. Use only with permission.")
        ),
        uiOutput("ocr_status")
      ),
      tags$hr(),
      
      # Admin caps (session)
      conditionalPanel(
        "input.show_advanced",
        h5("Admin & Data"),
        numericInput("max_upload_mb", "Max upload size (MB)", value = MAX_UPLOAD_MB_ENV, min = 5, max = 1000, step = 5),
        numericInput("max_pdf_pages", "Max PDF pages", value = CAP_MAX_PDF_PAGES, min = 10, max = 2000, step = 10),
        actionButton("delete_all_data", "Delete ALL stored data", class = "btn btn-danger w-100")
      ),
      tags$hr(),
      
      h5("Column Mapping"),
      uiOutput("mapping_ui"),
      fileInput("upload_mapping", "Load mapping (.json)", accept = ".json"),
      downloadButton("download_mapping", "Save current mapping", class = "btn btn-light w-100"),
      br(), br(),
      textInput("counterparty_id", "Counterparty ID (for saved mapping)", placeholder = "e.g. ACME_UK_2024"),
      fluidRow(
        column(6, actionButton("save_mapping_db", "Save mapping to DB", class="btn btn-sm btn-outline-secondary w-100")),
        column(6, actionButton("load_mapping_db", "Load mapping from DB", class="btn btn-sm btn-outline-secondary w-100"))
      ),
      uiOutput("saved_mappings_ui"),
      
      tags$hr(),
      
      h5("Presets (rules)"),
      fluidRow(
        column(4, actionButton("preset_underwriter", "Underwriter pack", class="btn btn-sm btn-outline-secondary w-100")),
        column(4, actionButton("preset_claims", "Claims QA pack", class="btn btn-sm btn-outline-secondary w-100")),
        column(4, actionButton("preset_lloyds", "Lloyd's DA pack", class="btn btn-sm btn-outline-secondary w-100"))
      ),
      tags$small("Applies sensible rule defaults and thresholds."),
      
      br(), br(),
      actionButton("run", "Run Validation", class = "btn btn-primary w-100"),
      br(), br(),
      actionButton("test_openai", "Test OpenAI", class = "btn btn-secondary w-100"),
      br(), verbatimTextOutput("openai_status"),
      tags$hr(),
      
      h5("Export"),
      selectInput("export_format", "Export format",
                  choices = c("standard_csv","ebix_v1","lloyds_v5_2","sunrise_basic"),
                  selected = "standard_csv"),
      
      downloadButton("download_csv", "Download Clean CSV", class = "btn btn-success w-100"),
      br(), br(),
      downloadButton("download_xlsx", "Download Clean Excel", class = "btn btn-success w-100"),
      br(), br(),
      downloadButton("download_report", "Download HTML Report", class = "btn btn-info w-100"),
      br(), br(),
      downloadButton("download_audit_pack", "Download Audit Pack (ZIP)", class = "btn btn-dark w-100"),
      br(), br(),
      
      checkboxInput("filter_preview_issues", "Filter Preview to selected Issues rows", FALSE),
      tags$small("Tip: select rows in Issues; the Preview filters to those rows when ticked."),
      tags$hr(),
      uiOutput("run_info"),
      tags$small(paste0("Version ", APP_VERSION, " • Minimal diagnostics logged."))
    ),
    mainPanel(
      tabsetPanel(
        tabPanel("Preview", DTOutput("preview")),
        tabPanel("Issues", DTOutput("issues_tbl")),
        tabPanel("Summary", verbatimTextOutput("summary_txt")),
        tabPanel("Pivots", DTOutput("pivots_tbl")),
        tabPanel("Lag Triangle (counts)", DTOutput("triangle_counts_tbl")),
        tabPanel("Lag Triangle (paid)", DTOutput("triangle_paid_tbl")),
        tabPanel("Lag Triangle (incurred)", DTOutput("triangle_incurred_tbl")),
        tabPanel("AI Explanation", verbatimTextOutput("ai_txt"))
      )
    )
  )
)

ui <- fluidPage(uiOutput("gate_or_app"))

# ---------- Server ----------
server <- function(input, output, session) {
  message("App starting. Version: ", APP_VERSION, " | mode=", APP_ENV)
  if (db_available()) { db_init(); db_prune_30d() }
  
  rv <- reactiveValues(
    access_ok = !nzchar(ACCESS_CODE),
    raw_list = list(), raw = NULL, std = NULL, std_unmasked = NULL, map = NULL,
    issues = NULL, metrics = NULL, ai_text = NULL, piv = NULL,
    tri_counts = NULL, tri_paid = NULL, tri_incurred = NULL, Q = NULL,
    run_id = NULL, ocr_skipped_files = character(),
    ai_call_times = numeric(0),  # rate-limit stamp store
    run_busy = FALSE,
    saved_counterparties = character()
  )
  
  # load saved counterparties for dropdown
  refresh_saved_counterparties <- function(){
    if (db_available()) {
      rv$saved_counterparties <- db_list_saved_counterparties(TENANT_ID)
    } else {
      rv$saved_counterparties <- character()
    }
  }
  refresh_saved_counterparties()
  
  # Busy banner
  output$busy_banner <- renderUI({
    if (!isTRUE(rv$run_busy)) return(NULL)
    div(style="background:#fff7e6;border:1px solid #ffd591;padding:10px;border-radius:8px;margin:8px 0",
        strong("Running validation… "), "Large files may take a moment. A timeout is enforced.")
  })
  
  # Per-session locale (no globals)
  loc_rv <- reactiveVal(LOCALES[[LOCALE_DEFAULT]])
  observe({
    req(input$locale_profile)
    if (input$locale_profile %in% names(LOCALES)) {
      loc_rv(LOCALES[[input$locale_profile]])
      options(brokerops.locale_profile = input$locale_profile)
    }
  })
  
  # Lloyd's mode = presets + export format
  observeEvent(input$lloyds_mode, {
    if (isTRUE(input$lloyds_mode)) {
      updateCheckboxInput(session, "rule_required_fields", TRUE)
      updateCheckboxInput(session, "rule_currency_list", TRUE)
      updateCheckboxInput(session, "rule_parse_dates", TRUE)
      updateCheckboxInput(session, "rule_policy_term", TRUE)
      updateCheckboxInput(session, "rule_date_order", TRUE)
      updateSelectInput(session, "export_format", selected = "lloyds_v5_2")
    }
  }, ignoreInit = TRUE)
  
  # Gate UI
  output$gate_or_app <- renderUI({
    if (isTRUE(rv$access_ok)) return(app_ui)
    tagList(
      br(), br(),
      div(style="max-width:420px;margin:0 auto",
          h3("Enter Access Code"),
          passwordInput("code", NULL, placeholder = "Access code"),
          actionButton("enter", "Enter", class="btn btn-primary"),
          tags$small(style="display:block;margin-top:8px;color:#666","Access code required for pilot access.")
      )
    )
  })
  observeEvent(input$enter, {
    rv$access_ok <- identical(trimws(input$code), ACCESS_CODE)
    if (!rv$access_ok) showNotification("Wrong code", type="error")
  })
  
  # Cloud OCR status badge
  output$ocr_status <- renderUI({
    prov <- tolower(or_null(input$ocr_provider, OCR_PROVIDER))
    ok <- get_ocr_ok(prov)()
    msg <- if (ok) sprintf("Provider '%s' is configured.", prov) else sprintf("Provider '%s' is NOT configured.", prov)
    col <- if (ok) "#2e7d32" else "#c62828"
    region_text <- if (nzchar(AZURE_REGION) && prov == "azure") paste0(" • region: ", AZURE_REGION) else ""
    tags$small(style = paste0("color:", col), paste0(msg, region_text))
  })
  
  # Salt banner
  output$salt_banner <- renderUI({
    if (identical(MASK_SALT_SOURCE, "ephemeral")) {
      div(style="background:#fff3cd;border:1px solid #ffeeba;padding:8px;border-radius:6px;margin-bottom:8px",
          HTML("<b>Heads-up:</b> using a temporary masking salt this session. Hashes won’t match across runs."))
    } else NULL
  })
  
  # Sample downloads
  output$download_sample <- downloadHandler(
    filename = function() "sample_loss_run.csv",
    content  = function(file) readr::write_csv(tibble::tibble(
      run_id        = NA_character_,
      tenant_id     = TENANT_ID,
      claim_id      = c("C-1001","C-1002","C-1003"),
      loss_date     = as.Date(c("2023-01-05","2023-03-18","2023-06-02")),
      report_date   = as.Date(c("2023-01-10","2023-03-20","2023-06-06")),
      paid          = c(1200, 0, 7500),
      incurred      = c(1500, 500, 9000),
      outstanding   = c(300, 500, 1500),
      currency      = c("AUD","AUD","AUD"),
      policy_number = c("POL123","POL456","POL789"),
      status        = c("Open","Closed","Open"),
      claimant      = c("Jane Doe","Acme Pty Ltd","John Smith")
    ), file, na = "")
  )
  output$download_sample_mapping <- downloadHandler(
    filename = function() "sample_mapping.json",
    content  = function(file) writeLines(jsonlite::toJSON(list(
      claim_id="claim_id", loss_date="loss_date", report_date="report_date",
      paid="paid", incurred="incurred", outstanding="outstanding",
      currency="currency", policy_number="policy_number", status="status", claimant="claimant"
    ), auto_unbox=TRUE, pretty=TRUE), file)
  )
  
  # Unmasking confirmation
  observeEvent(input$download_unmasked, {
    if (isTRUE(input$download_unmasked)) {
      showModal(modalDialog(
        title = "Confirm unmasked downloads",
        HTML("Unmasked exports may include personal data (claimant names, policy numbers). 
              Only download if you have permission and a secure storage location."),
        footer = tagList(
          actionButton("cancel_unmask", "Cancel"),
          actionButton("confirm_unmask", "I understand", class = "btn btn-danger")
        ),
        easyClose = FALSE
      ))
    }
  })
  observeEvent(input$cancel_unmask, { updateCheckboxInput(session, "download_unmasked", value = FALSE); removeModal() })
  observeEvent(input$confirm_unmask, { removeModal() })
  
  # Apply Presets
  observeEvent(input$preset_underwriter, {
    updateCheckboxInput(session, "rule_required_fields", TRUE)
    updateCheckboxInput(session, "rule_negative_values", TRUE)
    updateCheckboxInput(session, "rule_incurred_lt_paid", TRUE)
    updateCheckboxInput(session, "rule_reconcile", TRUE)
    updateCheckboxInput(session, "rule_currency_list", TRUE)
    updateCheckboxInput(session, "rule_date_order", TRUE)
    updateCheckboxInput(session, "rule_paid_outliers", TRUE)
    updateNumericInput(session, "iqr_multiplier", value = 1.5)
    updateCheckboxInput(session, "rule_high_ratio", TRUE)
    updateNumericInput(session, "ratio_threshold", value = 2)
  })
  observeEvent(input$preset_claims, {
    updateCheckboxInput(session, "rule_required_fields", TRUE)
    updateCheckboxInput(session, "rule_negative_values", TRUE)
    updateCheckboxInput(session, "rule_reconcile", TRUE)
    updateCheckboxInput(session, "rule_zero_rows", TRUE)
    updateCheckboxInput(session, "rule_duplicates", TRUE)
    updateCheckboxInput(session, "rule_missing_policy", TRUE)
    updateCheckboxInput(session, "rule_missing_currency", TRUE)
  })
  observeEvent(input$preset_lloyds, {
    updateCheckboxInput(session, "rule_required_fields", TRUE)
    updateCheckboxInput(session, "rule_currency_list", TRUE)
    updateCheckboxInput(session, "rule_parse_dates", TRUE)
    updateCheckboxInput(session, "rule_policy_term", TRUE)
    updateCheckboxInput(session, "rule_date_order", TRUE)
    updateSelectInput(session, "export_format", selected = "lloyds_v5_2")
  })
  
  # Mapping UI
  output$mapping_ui <- renderUI({
    req(rv$raw)
    cols <- names(rv$raw)
    m <- or_null(rv$map, guess_schema(cols))
    tagList(
      selectInput("map_claim_id","claim_id", choices = c("", cols), selected = m$claim_id),
      selectInput("map_loss_date","loss_date", choices = c("", cols), selected = m$loss_date),
      selectInput("map_report_date","report_date", choices = c("", cols), selected = m$report_date),
      selectInput("map_paid","paid", choices = c("", cols), selected = m$paid),
      selectInput("map_incurred","incurred", choices = c("", cols), selected = m$incurred),
      selectInput("map_outstanding","outstanding", choices = c("", cols), selected = m$outstanding),
      selectInput("map_currency","currency", choices = c("", cols), selected = m$currency),
      selectInput("map_policy_number","policy_number", choices = c("", cols), selected = m$policy_number),
      selectInput("map_status","status", choices = c("", cols), selected = m$status),
      selectInput("map_claimant","claimant", choices = c("", cols), selected = m$claimant)
    )
  })
  
  # Saved mappings UI
  output$saved_mappings_ui <- renderUI({
    if (!db_available()) return(tags$small("Saved mappings require DB_PATH to be set."))
    if (!length(rv$saved_counterparties)) return(tags$small("No saved mappings yet."))
    selectInput("saved_counterparty", "Saved counterparties", choices = rv$saved_counterparties)
  })
  
  observeEvent(input$save_mapping_db, {
    req(db_available(), rv$raw)
    cp <- trimws(input$counterparty_id); if (!nzchar(cp)) { showNotification("Enter a Counterparty ID.", type="warning"); return() }
    m <- or_null(rv$map, guess_schema(names(rv$raw)))
    db_save_mapping(TENANT_ID, cp, m)
    refresh_saved_counterparties()
    showNotification("Mapping saved.", type="message")
  })
  observeEvent(input$load_mapping_db, {
    req(db_available())
    cp <- if (nzchar(input$counterparty_id)) input$counterparty_id else input$saved_counterparty
    cp <- trimws(cp); if (!nzchar(cp)) { showNotification("Specify which counterparty to load.", type="warning"); return() }
    m <- db_load_mapping(TENANT_ID, cp)
    if (is.null(m)) { showNotification("No saved mapping found for that counterparty.", type="error"); return() }
    rv$map <- m
    showNotification(paste("Loaded mapping for", cp), type="message")
  })
  
  output$download_mapping <- downloadHandler(
    filename = function() "column_mapping.json",
    content  = function(file) {
      m <- or_null(rv$map, guess_schema(names(rv$raw)))
      writeLines(jsonlite::toJSON(m, auto_unbox = TRUE, pretty = TRUE), file)
    }
  )
  observeEvent(input$upload_mapping, {
    req(input$upload_mapping)
    m <- tryCatch(jsonlite::fromJSON(readr::read_file(input$upload_mapping$datapath)),
                  error = function(e) NULL)
    if (is.null(m)) { showNotification("Invalid mapping file.", type = "error"); return(NULL) }
    rv$map <- m
    showNotification("Mapping loaded.", type = "message")
  })
  
  # Uploads (with progress, magic-byte checks, header promote, OCR skip notices + read timeout)
  output$openai_status <- renderText("")  # init
  
  observeEvent(input$files, {
    try({
      req(input$files)
      message("Upload received: ", paste(input$files$name, collapse = " | "))
      
      if (isTRUE(input$use_cloud_ocr) && !get_ocr_ok(tolower(or_null(input$ocr_provider, OCR_PROVIDER)))()) {
        showNotification("Cloud OCR is enabled but the provider is not configured. Check environment variables.", type = "error", duration = 8)
      }
      
      exts <- tolower(tools::file_ext(input$files$name))
      bad  <- !exts %in% c("csv","xlsx","xls","pdf")
      if (any(bad)) {
        showNotification(
          paste("Unsupported file(s):", paste(unique(input$files$name[bad]), collapse=", "),
                " — upload CSV/XLSX/PDF only."),
          type = "error", duration = 8
        )
      }
      files <- input$files[!bad, , drop = FALSE]
      if (nrow(files) == 0) { showNotification("No valid files to read (CSV/XLSX/PDF only).", type = "error", duration = 6); return(NULL) }
      
      dfs <- list()
      ocr_skipped <- character()
      withProgress(message = "Reading files...", value = 0, {
        n <- nrow(files)
        for (i in seq_len(n)) {
          f   <- files[i, ]
          ext <- tolower(tools::file_ext(f$name))
          detail <- paste0("(", i, "/", n, ") ", f$name)
          incProgress(0, detail = detail)
          message("Reading file: ", f$name, " (", ext, ")")
          
          # Global size guard (post body already capped by options)
          MAX_MB <- or_null(input$max_upload_mb, MAX_UPLOAD_MB_ENV)
          sz <- tryCatch(file.info(f$datapath)$size, error = function(e) 0)
          if (!is.na(sz) && sz > MAX_MB * 1024^2) {
            showNotification(paste("Skipped", f$name, "— file too large (>", MAX_MB, "MB)."), type = "warning", duration = 8)
            incProgress(1/n); next
          }
          
          # Magic signature checks
          sig_ok <- TRUE
          if (ext == "pdf" && !is_pdf_magic(f$datapath)) sig_ok <- FALSE
          if (ext == "xlsx" && !is_zip_magic(f$datapath)) sig_ok <- FALSE
          if (ext == "xls" && !(is_ole_magic(f$datapath) || is_zip_magic(f$datapath))) sig_ok <- FALSE
          if (ext == "csv" && (is_zip_magic(f$datapath) || is_pdf_magic(f$datapath))) sig_ok <- FALSE
          if (!sig_ok) {
            showNotification(paste("Skipped", f$name, "— file signature does not match the extension."), type="error", duration=8)
            incProgress(1/n); next
          }
          
          # PDF sanity checks
          if (identical(ext, "pdf")) {
            chk <- pdf_sanity_check(f$datapath, max_pages = or_null(input$max_pdf_pages, CAP_MAX_PDF_PAGES))
            if (!isTRUE(chk$ok)) {
              showNotification(paste("Skipped", f$name, "—", chk$reason), type = "error", duration = 8)
              incProgress(1/n); next
            }
          }
          
          # Cloud OCR guardrails
          if (ext == "pdf" && isTRUE(input$use_cloud_ocr)) {
            if (!get_ocr_ok(tolower(or_null(input$ocr_provider, OCR_PROVIDER)))()) {
              showNotification("Cloud OCR is ON but not configured. Skipping this PDF.", type="error", duration=8)
              incProgress(1/n); next
            }
            szpdf <- tryCatch(file.info(f$datapath)$size, error = function(e) 0)
            if (!is.na(szpdf) && szpdf > 45 * 1024^2) {
              showNotification(paste("Skipped", f$name, "— PDF too large for OCR (>45MB)."), type="warning", duration=8)
              incProgress(1/n); next
            }
          }
          
          df <- tryCatch(
            R.utils::withTimeout(
              read_uploaded_one(
                f$datapath, ext,
                use_cloud_ocr = isTRUE(input$use_cloud_ocr),
                ocr_provider  = or_null(input$ocr_provider, OCR_PROVIDER)
              ),
              timeout = READ_TIMEOUT_SEC, onTimeout = "error"
            ),
            error = function(e) {
              showNotification(paste("Failed to read", f$name, ":", e$message), type = "error", duration = 8)
              message("READ ERROR for ", f$name, ": ", e$message)
              NULL
            }
          )
          if (!is.null(df)) {
            df <- promote_header_if(df, isTRUE(input$promote_header))
            df$..source_file.. <- f$name
            if (isTRUE(attr(df, "ocr_skipped"))) ocr_skipped <- c(ocr_skipped, f$name)
            dfs[[f$name]] <- df
          }
          incProgress(1/n)
        }
      })
      
      if (!length(dfs)) { showNotification("All files failed to load.", type = "error", duration = 6); return(NULL) }
      if (length(ocr_skipped)) {
        showNotification(paste0("Cloud OCR was ON but skipped for ", length(ocr_skipped), " PDF(s) because selectable text was detected: ",
                                paste(unique(ocr_skipped), collapse = ", ")), type = "message", duration = 10)
      }
      
      first_name   <- names(dfs)[1]
      rv$raw_list  <- dfs
      rv$raw       <- dfs[[first_name]]
      rv$map       <- guess_schema(names(rv$raw))
      rv$ocr_skipped_files <- unique(ocr_skipped)
      message("Loaded OK: ", paste(names(dfs), collapse = " | "))
    }, silent = TRUE)
  })
  
  # Tables
  output$preview <- renderDT({
    req(rv$std)
    std <- rv$std
    if (isTRUE(input$filter_preview_issues) && !is.null(input$issues_tbl_rows_selected) && length(input$issues_tbl_rows_selected) > 0) {
      sel <- input$issues_tbl_rows_selected
      rows_to_show <- unique(rv$issues$row[sel])
      rows_to_show <- rows_to_show[rows_to_show >= 1 & rows_to_show <= nrow(std)]
      if (length(rows_to_show)) std <- std[rows_to_show, , drop = FALSE]
    }
    datatable(std, options = list(scrollX = TRUE, pageLength = 25, fixedHeader = TRUE, deferRender = TRUE, processing = TRUE),
              rownames = FALSE, selection = "none")
  }, server = TRUE)
  
  output$issues_tbl <- renderDT({
    req(rv$issues)
    datatable(rv$issues, options = list(scrollX = TRUE, pageLength = 25, fixedHeader = TRUE, deferRender = TRUE, processing = TRUE),
              rownames = FALSE, selection = "multiple")
  }, server = TRUE)
  
  output$pivots_tbl <- renderDT({ req(rv$piv); datatable(rv$piv, options = list(scrollX=TRUE, pageLength=25, fixedHeader=TRUE, deferRender=TRUE, processing=TRUE), rownames=FALSE) }, server = TRUE)
  output$triangle_counts_tbl <- renderDT({ req(rv$tri_counts); datatable(rv$tri_counts, options = list(scrollX=TRUE, pageLength=25, fixedHeader=TRUE, deferRender=TRUE, processing=TRUE), rownames=FALSE) }, server = TRUE)
  output$triangle_paid_tbl   <- renderDT({ req(rv$tri_paid);   datatable(rv$tri_paid, options = list(scrollX=TRUE, pageLength=25, fixedHeader=TRUE, deferRender=TRUE, processing=TRUE), rownames=FALSE) }, server = TRUE)
  output$triangle_incurred_tbl <- renderDT({ req(rv$tri_incurred); datatable(rv$tri_incurred, options = list(scrollX=TRUE, pageLength=25, fixedHeader=TRUE, deferRender=TRUE, processing=TRUE), rownames=FALSE) }, server = TRUE)
  
  output$summary_txt <- renderText({
    req(rv$Q); Q <- rv$Q
    glue::glue(
      "Rows: {Q$n_rows}\n" ,
      "Unique claims: {Q$n_claims}\n",
      "Severity (total paid ÷ unique claims): ${.format_num(Q$severity_claim)}\n",
      "Totals — Paid: ${.format_num(Q$total_paid)} | Incurred: ${.format_num(Q$total_incurred)}\n",
      "Open vs Closed — Open {Q$open_n} (OS ${.format_num(Q$open_os)}); Closed {Q$closed_n} (Paid ${.format_num(Q$closed_paid)})\n",
      "Anomalies — Incurred<Paid: {Q$inc_lt_rows_n} rows / {Q$inc_lt_claims_n} claims (variance ${.format_num(Q$inc_lt_variance)}); Recon mismatches: {Q$recon_rows_n} rows (|delta| ${.format_num(Q$recon_abs_delta)})\n",
      "Outliers — IQR rule (paid > Q3 + {Q$outlier_k}×IQR); n={Q$outliers_n}; top loss ${.format_num(Q$top_loss)}\n",
      "Negatives — paid {Q$neg_paid_n} (sum ${.format_num(Q$neg_paid_sum)}); outstanding {Q$neg_os_n} (sum ${.format_num(Q$neg_os_sum)}); zero-amount claims {Q$zero_claims}\n",
      "Dates — unparseable loss {Q$unparse_loss}, unparseable report {Q$unparse_report}; report<loss {Q$report_before_loss}; ",
      if (Q$pol_term_set) glue('outside policy term {Q$outside_term_n} rows ({Q$outside_term_pct}%).') else 'policy term not provided.', "\n",
      "Currency — set [", paste(Q$currencies, collapse=', '), "]; allowed=", Q$currency_ok, "; decimals≤2=", Q$decimals_ok, "\n",
      "Duplicates — exact row ", Q$exact_dup_n, "; claim_id+loss_date ", Q$dup_claimid_lossdate_n, "\n",
      "Required fields missing — claim_id=", Q$req_missing$claim_id, ", loss_date=", Q$req_missing$loss_date, ", report_date=", Q$req_missing$report_date, ", policy_number=", Q$req_missing$policy_number, ", currency=", Q$req_missing$currency,
      if(!is.na(Q$req_missing$status)) glue(", status={Q$req_missing$status}") else '', ".\n",
      "Dataset sanity — sum(incurred) vs sum(paid + OS): ", if (Q$dataset_recon_ok) glue('OK (tol ${.format_num(Q$dataset_recon_tol)})') else glue('gap ${.format_num(Q$dataset_recon_gap)} (tol ${.format_num(Q$dataset_recon_tol)})')
    )
  })
  
  output$ai_txt <- renderText({ or_null(rv$ai_text, "") })
  output$run_info <- renderUI({ if (is.null(rv$run_id)) return(NULL); tags$div(style="margin-top:6px;color:#666", paste("Run ID:", rv$run_id)) })
  
  # OpenAI test with rate-limit
  observeEvent(input$test_openai, {
    now <- as.numeric(Sys.time())
    recent <- rv$ai_call_times[rv$ai_call_times > (now - 60)]
    if (length(recent) >= AI_RATE_PER_MIN || length(rv$ai_call_times) >= AI_RATE_PER_SESS) {
      output$openai_status <- renderText("AI test rate-limited. Please try again later.")
      return()
    }
    rv$ai_call_times <- c(recent, now)
    key <- OPENAI_API_KEY
    if (!nzchar(key)) { output$openai_status <- renderText("No OPENAI_API_KEY set. Add it via .Renviron or Sys.setenv then redeploy."); return() }
    body <- list(model = "gpt-4o-mini",
                 input = list(list(role="system", content="You are helpful."), list(role="user", content="Reply with: OK (OpenAI key works)")))
    resp <- tryCatch({
      httr::RETRY("POST", "https://api.openai.com/v1/responses",
                  httr::add_headers(Authorization = paste("Bearer",key), `Content-Type` = "application/json", `User-Agent` = "BrokerOpsAI/1.0"),
                  body=jsonlite::toJSON(body, auto_unbox=TRUE), times=3, pause_min=1, terminate_on=c(400,401,403,404), httr::timeout(20))
    }, error=function(e) e)
    if (inherits(resp,"error")) { output$openai_status <- renderText(paste("Network error:", resp$message)); return() }
    txt <- httr::content(resp, as="text", encoding="UTF-8")
    parsed <- tryCatch(jsonlite::fromJSON(txt, simplifyVector=FALSE), error=function(e) NULL)
    if (is.null(parsed)) { output$openai_status <- renderText(substr(txt,1,200)); return() }
    if (httr::status_code(resp)>=400){
      msg <- tryCatch(parsed$error$message, error=function(e) substr(txt,1,200))
      output$openai_status <- renderText(paste("OpenAI error:", msg)); return()
    }
    out <- tryCatch(extract_responses_text(parsed), error=function(e) substr(txt,1,200))
    output$openai_status <- renderText(out)
  })
  
  # Delete all data
  observeEvent(input$delete_all_data, {
    showModal(modalDialog(
      title = "Confirm deletion",
      "This will permanently delete saved runs, issues, mappings, and audit_log from the local DB.",
      footer = tagList(
        modalButton("Cancel"),
        actionButton("confirm_delete_all", "Delete everything", class = "btn btn-danger")
      )
    ))
  })
  observeEvent(input$confirm_delete_all, {
    removeModal()
    if (db_available()) {
      db_delete_all()
      refresh_saved_counterparties()
      showNotification("All stored data deleted.", type = "warning", duration = 6)
    } else {
      showNotification("No DB configured; nothing to delete.", type = "message")
    }
  })
  
  # Background run
  observeEvent(input$run, {
    try({
      req(length(rv$raw_list) > 0)
      run_id <- digest::digest(paste(Sys.time(), sample.int(1e9,1)))
      rv$run_busy <- TRUE
      shinyjs::disable("run"); shinyjs::disable("test_openai")
      
      raw_list      <- rv$raw_list
      map           <- list(
        claim_id = input$map_claim_id, loss_date = input$map_loss_date, report_date = input$map_report_date,
        paid = input$map_paid, incurred = input$map_incurred, outstanding = input$map_outstanding,
        currency = input$map_currency, policy_number = input$map_policy_number, status = input$map_status, claimant = input$map_claimant
      )
      pol_start     <- input$policy_start
      pol_end       <- input$policy_end
      cfg           <- list(
        rule_required_fields = input$rule_required_fields,
        rule_negative_values = input$rule_negative_values,
        rule_incurred_lt_paid = input$rule_incurred_lt_paid,
        rule_reconcile = input$rule_reconcile,
        rule_currency_list = input$rule_currency_list,
        rule_duplicates = input$rule_duplicates,
        rule_date_order = input$rule_date_order,
        rule_missing_policy = input$rule_missing_policy,
        rule_missing_currency = input$rule_missing_currency,
        rule_missing_claimant = input$rule_missing_claimant,
        rule_paid_outliers = input$rule_paid_outliers,
        iqr_multiplier = input$iqr_multiplier,
        rule_high_ratio = input$rule_high_ratio,
        ratio_threshold = input$ratio_threshold,
        rule_zero_rows = input$rule_zero_rows,
        rule_parse_dates = input$rule_parse_dates,
        rule_policy_term = input$rule_policy_term,
        rule_multi_rows_per_claim = input$rule_multi_rows_per_claim
      )
      locale        <- loc_rv()
      # Rate-limit gate for AI
      enable_ai <- isTRUE(input$enable_ai) && nzchar(OPENAI_API_KEY)
      if (enable_ai) {
        now <- as.numeric(Sys.time()); recent <- rv$ai_call_times[rv$ai_call_times > (now - 60)]
        if (length(recent) >= AI_RATE_PER_MIN || length(rv$ai_call_times) >= AI_RATE_PER_SESS) {
          enable_ai <- FALSE
          showNotification("AI explanation rate-limited for this session. Running without AI.", type="warning", duration=6)
        } else {
          rv$ai_call_times <- c(recent, now)
        }
      }
      
      audit("run_requested", list(n_files = length(raw_list)))
      
      fut <- future({
        withr::with_envvar(c(TZ="UTC"), {
          R.utils::withTimeout({
            std_all <- list(); unmasked_all <- list()
            nm_vec <- names(raw_list)
            for (i in seq_along(nm_vec)) {
              nm <- nm_vec[i]
              df <- raw_list[[nm]]
              s <- tryCatch(standardize_df(df, map, source_file = nm, loc = locale), error = function(e) NULL)
              if (!is.null(s)) { std_all[[nm]] <- s; unmasked_all[[nm]] <- attr(s, "unmasked") }
            }
            if (!length(std_all)) stop("No files mapped.")
            std <- dplyr::bind_rows(std_all)
            std_unmasked <- dplyr::bind_rows(unmasked_all)
            std$..row_id.. <- seq_len(nrow(std))
            std$run_id <- run_id  # stamp run_id on rows
            
            v <- tryCatch(validate_claims(std, cfg, pol_start, pol_end), error = function(e) list(issues = tibble(), metrics = list()))
            Q   <- build_quality_summary(std, pol_start, pol_end)
            piv <- tryCatch(make_pivots(std), error = function(e) tibble::tibble(note = paste("Pivot error:", e$message)))
            tri_counts <- tryCatch(make_triangle_counts(std), error = function(e) tibble::tibble(note = paste("Triangle error:", e$message)))
            tri_paid   <- tryCatch(make_triangle_amounts(std, "paid"), error = function(e) tibble::tibble(note = paste("Triangle paid error:", e$message)))
            tri_incurred <- tryCatch(make_triangle_amounts(std, "incurred"), error = function(e) tibble::tibble(note = paste("Triangle incurred error:", e$message)))
            
            ai_text <- "(AI explanation disabled.)"
            if (enable_ai) {
              if (nrow(std) > 200000) {
                ai_text <- "(AI explanation skipped: dataset too large.)"
              } else {
                safe_Q <- Q; attr(safe_Q, "original") <- NULL
                ai_text <- tryCatch(generate_gpt_explainer(safe_Q), error = function(e) paste("AI explanation failed:", e$message))
              }
            }
            
            list(std = std, std_unmasked = std_unmasked, issues = v$issues, metrics = v$metrics,
                 Q = Q, piv = piv, tri_counts = tri_counts, tri_paid = tri_paid, tri_incurred = tri_incurred,
                 ai_text = ai_text, run_id = run_id)
          }, timeout = RUN_TIMEOUT_SEC, onTimeout = "error")
        })
      })
      
      fut %...>% (function(res){
        # Inline "why flagged": join back on row id
        if (!is.null(res$issues) && nrow(res$issues) > 0) {
          flags <- res$issues %>% group_by(row) %>% summarise(
            flags = paste(unique(issue), collapse = "; "),
            flags_detail = paste(unique(detail[!is.na(detail) & nzchar(detail)]), collapse = " | "),
            .groups = "drop"
          )
          res$std <- res$std %>% left_join(flags, by = c("..row_id.." = "row"))
        } else {
          res$std$flags <- NA_character_; res$std$flags_detail <- NA_character_
        }
        
        rv$std <- res$std; rv$std_unmasked <- res$std_unmasked
        rv$issues <- res$issues; rv$metrics <- res$metrics
        rv$Q <- res$Q; rv$piv <- res$piv; rv$tri_counts <- res$tri_counts; rv$tri_paid <- res$tri_paid; rv$tri_incurred <- res$tri_incurred
        rv$ai_text <- res$ai_text; rv$run_id <- res$run_id
        
        # persist to DB (optional)
        if (db_available()) {
          db_save_run(run_id = rv$run_id, tenant_id = TENANT_ID, Q = rv$Q, issues_df = rv$issues)
        }
        
        audit("run_completed", list(rows = nrow(rv$std), files = unique(vapply(rv$std$source_file, scrub_name, FUN.VALUE = character(1)))))
        if (isTRUE(input$enable_ai)) audit("ai_explainer_used", list(n_rows = nrow(rv$std)))
        message("Run completed | run_id=", rv$run_id,
                " | rows=", nrow(rv$std),
                " | claims=", dplyr::n_distinct(rv$std$claim_id),
                " | files=", paste(unique(rv$std$source_file), collapse = " | "))
        
        rv$run_busy <- FALSE
        shinyjs::enable("run"); shinyjs::enable("test_openai")
      }) %...!% (function(e){
        rv$run_busy <- FALSE
        shinyjs::enable("run"); shinyjs::enable("test_openai")
        showNotification(paste("Background error:", e$message), type = "error")
        audit("run_failed", list(error = e$message))
      })
      
    }, silent = TRUE)
  })
  
  # Downloads (masked by default; unmasked only if user opted in)
  output$download_csv <- downloadHandler(
    filename = function() paste0("clean_claims_", or_null(rv$run_id, "noRun"), "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv"),
    content  = function(file){
      req(rv$std)
      dat0 <- if (isTRUE(input$download_unmasked)) or_null(rv$std_unmasked, rv$std) else rv$std
      dat <- transform_export(dat0, input$export_format, locale = getOption("brokerops.locale_profile","AU"))
      readr::write_csv(dat, file, na="")
      audit("download_csv", list(unmasked = isTRUE(input$download_unmasked), format = input$export_format))
    }
  )
  
  output$download_xlsx <- downloadHandler(
    filename = function() paste0("clean_claims_", or_null(rv$run_id, "noRun"), "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".xlsx"),
    content  = function(file){
      req(rv$std)
      dat0 <- if (isTRUE(input$download_unmasked)) or_null(rv$std_unmasked, rv$std) else rv$std
      dat <- transform_export(dat0, input$export_format, locale = getOption("brokerops.locale_profile","AU"))
      m <- or_null(rv$metrics, list())
      summ <- if (length(m)) tibble::tibble(metric = names(m), value = unlist(m, use.names = FALSE)) else tibble::tibble()
      wb <- list(
        clean = dat,
        issues = or_null(rv$issues, tibble::tibble()),
        pivots = or_null(rv$piv, tibble::tibble()),
        triangles_counts = or_null(rv$tri_counts, tibble::tibble()),
        triangles_paid = or_null(rv$tri_paid, tibble::tibble()),
        triangles_incurred = or_null(rv$tri_incurred, tibble::tibble()),
        summary = summ
      )
      writexl::write_xlsx(wb, path = file)
      audit("download_xlsx", list(unmasked = isTRUE(input$download_unmasked), format = input$export_format))
    }
  )
  
  output$download_report <- downloadHandler(
    filename = function() paste0("brokerops_report_", or_null(rv$run_id, "noRun"), "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".html"),
    content  = function(file){
      req(rv$std, rv$Q)
      html <- make_html_report(rv$Q, rv$issues, rv$piv, rv$tri_counts, rv$tri_paid, rv$tri_incurred, rv$ai_text, rv$run_id)
      writeLines(html, con=file)
      audit("download_report", list(unmasked = FALSE))
    }
  )
  
  output$download_audit_pack <- downloadHandler(
    filename = function() paste0("audit_pack_", or_null(rv$run_id, "noRun"), "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".zip"),
    content = function(file) {
      req(rv$std, rv$Q)
      dir <- tempfile("auditpack"); dir.create(dir)
      csv_path   <- file.path(dir, "clean.csv")
      xlsx_path  <- file.path(dir, "clean.xlsx")
      html_path  <- file.path(dir, "report.html")
      json_path  <- file.path(dir, "summary.json")
      parquet_path <- file.path(dir, "clean.parquet")
      
      dat0 <- if (isTRUE(input$download_unmasked)) or_null(rv$std_unmasked, rv$std) else rv$std
      dat <- transform_export(dat0, input$export_format, locale = getOption("brokerops.locale_profile","AU"))
      
      readr::write_csv(dat, csv_path, na = "")
      wb <- list(
        clean = dat,
        issues = or_null(rv$issues, tibble::tibble()),
        pivots = or_null(rv$piv, tibble::tibble()),
        triangles_counts = or_null(rv$tri_counts, tibble::tibble()),
        triangles_paid = or_null(rv$tri_paid, tibble::tibble()),
        triangles_incurred = or_null(rv$tri_incurred, tibble::tibble())
      )
      writexl::write_xlsx(wb, path = xlsx_path)
      
      if (isTRUE(has_arrow)) {
        arrow::write_parquet(dat, parquet_path)
      }
      
      html <- make_html_report(rv$Q, rv$issues, rv$piv, rv$tri_counts, rv$tri_paid, rv$tri_incurred, rv$ai_text, rv$run_id)
      writeLines(html, con = html_path)
      
      summary_json <- list(
        app_version = APP_VERSION,
        run_id = or_null(rv$run_id, "N/A"),
        metrics = or_null(rv$metrics, list()),
        Q = rv$Q,
        export_format = input$export_format
      )
      jsonlite::write_json(summary_json, json_path, auto_unbox = TRUE, pretty = TRUE)
      
      files_to_zip <- c(csv_path, xlsx_path, html_path, json_path)
      if (file.exists(parquet_path)) files_to_zip <- c(files_to_zip, parquet_path)
      zip::zip(zipfile = file, files = files_to_zip, mode = "cherry-pick")
      audit("download_audit_pack", list(unmasked = isTRUE(input$download_unmasked), format = input$export_format))
    }
  )
}

shinyApp(ui, server) 