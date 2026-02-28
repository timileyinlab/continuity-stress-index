#!/usr/bin/env Rscript

###############################################################################
# WB_BPS Business Continuity — Results Pipeline v7 (Non-throwing time parser + p-values)
#
# Outputs:
#   - 7 Figures (PNG) in figures_png/
#   - 7 Tables (CSV + HTML) in tables/
#   - cleaned data + panel extracts in data/
#   - model objects in models/
#   - logs + audit files in logs/
#
# Run:
#   Rscript wb_bps_results_v7.R --data_root=/path/to/files --out_root=/path/to/outputs --do_mi=1
###############################################################################

options(stringsAsFactors = FALSE, scipen = 999)
Sys.setenv(TZ = "UTC")
set.seed(20260121)

`%||%` <- function(x, y) if (!is.null(x)) x else y

# ---------------------------- 01) Minimal CLI --------------------------------
args <- commandArgs(trailingOnly = TRUE)
get_arg <- function(key, default = NULL) {
  hit <- grep(paste0("^--", key, "="), args, value = TRUE)
  if (length(hit) == 0) return(default)
  sub(paste0("^--", key, "="), "", hit[1])
}

DATA_ROOT <- get_arg("data_root", default = if (dir.exists("/mnt/data")) "/mnt/data" else getwd())
OUT_ROOT  <- get_arg("out_root",  default = file.path(getwd(), "outputs"))
DO_MI     <- as.integer(get_arg("do_mi", default = "1")) == 1

# ---------------------------- 02) Packages -----------------------------------
install_if_missing <- function(pkgs) {
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    message("Installing missing packages: ", paste(missing, collapse = ", "))
    install.packages(missing, repos = "https://cloud.r-project.org")
  }
  invisible(TRUE)
}

pkgs <- c(
  "data.table","dplyr","tidyr","stringr","lubridate",
  "ggplot2","scales","fixest","broom","readr","fs","glue",
  "ggrepel","viridis","jsonlite","zoo","mice"
)
install_if_missing(pkgs)

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(lubridate)
  library(ggplot2)
  library(scales)
  library(fixest)
  library(broom)
  library(readr)
  library(fs)
  library(glue)
  library(ggrepel)
  library(viridis)
  library(jsonlite)
  library(zoo)
  library(mice)
})

has_scico <- requireNamespace("scico", quietly = TRUE)
if (has_scico) suppressPackageStartupMessages(library(scico))

# ---------------------------- 03) Paths + outputs ----------------------------
in_files <- c("WB_BPS.csv","WB_BPS_WIDEF.csv","WB_BPS_DATADICT.csv","WB_BPS.json","WB_BPS.pdf")
in_paths <- setNames(file.path(DATA_ROOT, in_files), in_files)

for (nm in names(in_paths)) {
  if (!fs::file_exists(in_paths[[nm]])) stop("Missing required file: ", in_paths[[nm]])
}

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
OUT_DIR   <- file.path(OUT_ROOT, paste0("wb_bps_business_continuity_results_", timestamp))
FIG_DIR   <- file.path(OUT_DIR, "figures_png")
TAB_DIR   <- file.path(OUT_DIR, "tables")
DAT_DIR   <- file.path(OUT_DIR, "data")
MOD_DIR   <- file.path(OUT_DIR, "models")
LOG_DIR   <- file.path(OUT_DIR, "logs")
fs::dir_create(c(OUT_DIR, FIG_DIR, TAB_DIR, DAT_DIR, MOD_DIR, LOG_DIR))

# Provenance copies (non-fatal if copy fails)
try(fs::file_copy(in_paths[["WB_BPS.json"]],         file.path(DAT_DIR, "WB_BPS_source_metadata.json"), overwrite = TRUE), silent = TRUE)
try(fs::file_copy(in_paths[["WB_BPS.pdf"]],          file.path(DAT_DIR, "WB_BPS_source_documentation.pdf"), overwrite = TRUE), silent = TRUE)
try(fs::file_copy(in_paths[["WB_BPS_DATADICT.csv"]], file.path(DAT_DIR, "WB_BPS_DATADICT.csv"), overwrite = TRUE), silent = TRUE)
try(fs::file_copy(in_paths[["WB_BPS_WIDEF.csv"]],    file.path(DAT_DIR, "WB_BPS_WIDEF.csv"), overwrite = TRUE), silent = TRUE)

# ---------------------------- 04) Logging + utilities -------------------------
log_path <- file.path(LOG_DIR, "run_log.txt")
log_msg <- function(...) {
  msg <- paste0("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S UTC"), "] ", paste(..., collapse = ""))
  cat(msg, "\n")
  cat(msg, "\n", file = log_path, append = TRUE)
}

assert_has_cols <- function(df, cols, df_name = "data") {
  miss <- setdiff(cols, names(df))
  if (length(miss) > 0) stop(df_name, " missing columns: ", paste(miss, collapse = ", "))
  invisible(TRUE)
}

safe_fwrite <- function(df, path) {
  tryCatch(data.table::fwrite(df, path),
           error = function(e) stop("Write CSV failed: ", path, " | ", conditionMessage(e)))
}

write_note_file <- function(path, note) {
  writeLines(as.character(note), con = path, useBytes = TRUE)
  invisible(TRUE)
}

safe_fwrite_or_note <- function(df, path, note_if_null = "No data available to write.") {
  if (is.null(df) || (is.data.frame(df) && nrow(df) == 0)) {
    write_note_file(path, note_if_null)
    return(invisible(FALSE))
  }
  safe_fwrite(df, path)
  invisible(TRUE)
}

safe_write_html_table <- function(df, path, title = NULL, note = NULL) {
  esc <- function(x) {
    x <- as.character(x)
    x <- gsub("&", "&amp;", x, fixed = TRUE)
    x <- gsub("<", "&lt;",  x, fixed = TRUE)
    x <- gsub(">", "&gt;",  x, fixed = TRUE)
    x <- gsub("\"","&quot;",x, fixed = TRUE)
    x
  }
  if (is.null(df) || (is.data.frame(df) && nrow(df) == 0)) {
    html <- paste0(
      "<!doctype html><html><head><meta charset='utf-8'></head><body>",
      "<div style='font-family:Arial;margin:24px;'>",
      "<div style='font-weight:700;font-size:16px;margin-bottom:8px;'>", esc(title %||% "Table"), "</div>",
      "<div style='color:#444;'>", esc(note %||% "No data available."), "</div>",
      "</div></body></html>"
    )
    writeLines(html, con = path, useBytes = TRUE)
    return(invisible(TRUE))
  }
  
  df2 <- df
  for (j in seq_along(df2)) df2[[j]] <- esc(df2[[j]])
  
  th  <- paste0("<th style='text-align:left;padding:8px;border-bottom:2px solid #111;'>",
                esc(names(df2)), "</th>", collapse = "")
  trs <- apply(df2, 1, function(r) {
    tds <- paste0("<td style='text-align:left;padding:8px;border-bottom:1px solid #ddd;vertical-align:top;'>",
                  r, "</td>", collapse = "")
    paste0("<tr>", tds, "</tr>")
  })
  
  cap <- if (!is.null(title)) paste0("<div style='font-weight:700;font-size:16px;margin-bottom:8px;'>", esc(title), "</div>") else ""
  nt  <- if (!is.null(note))  paste0("<div style='color:#444;font-size:12px;margin-top:10px;'>", esc(note), "</div>") else ""
  
  html <- paste0(
    "<!doctype html><html><head><meta charset='utf-8'>",
    "<style>body{font-family:Arial,Helvetica,sans-serif;margin:24px;} table{border-collapse:collapse;width:100%;}</style>",
    "</head><body>", cap,
    "<table><thead><tr>", th, "</tr></thead><tbody>", paste0(trs, collapse = ""), "</tbody></table>",
    nt, "</body></html>"
  )
  writeLines(html, con = path, useBytes = TRUE)
  invisible(TRUE)
}

# Publication formatting: replace NA/Inf with "—", keep a raw CSV as well.
format_pub <- function(df, digits = 3) {
  if (is.null(df) || nrow(df) == 0) return(df)
  out <- df
  for (nm in names(out)) {
    if (is.numeric(out[[nm]])) {
      x <- out[[nm]]
      x[!is.finite(x)] <- NA_real_
      out[[nm]] <- ifelse(is.na(x), "—", formatC(x, format = "f", digits = digits))
    } else {
      x <- as.character(out[[nm]])
      x[is.na(x) | !nzchar(x)] <- "—"
      out[[nm]] <- x
    }
  }
  out
}

write_table_dual <- function(df, out_prefix, title, note) {
  # raw
  safe_fwrite_or_note(df, file.path(TAB_DIR, paste0(out_prefix, "_raw.csv")), note)
  # publication CSV + HTML
  df_pub <- format_pub(df)
  safe_fwrite_or_note(df_pub, file.path(TAB_DIR, paste0(out_prefix, ".csv")), note)
  safe_write_html_table(df_pub, file.path(TAB_DIR, paste0(out_prefix, ".html")), title = title, note = note)
}

wrap_lab <- function(x, width = 24) stringr::str_wrap(as.character(x), width = width)

safe_mean <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  if (length(x) == 0 || all(is.na(x))) return(NA_real_)
  mean(x, na.rm = TRUE)
}
safe_quantile <- function(x, p) {
  x <- suppressWarnings(as.numeric(x))
  if (length(x) == 0 || all(is.na(x))) return(NA_real_)
  as.numeric(stats::quantile(x, probs = p, na.rm = TRUE, type = 7))
}

zscore <- function(x) {
  x <- suppressWarnings(as.numeric(x))
  if (all(is.na(x))) return(rep(NA_real_, length(x)))
  mu  <- mean(x, na.rm = TRUE)
  sdv <- stats::sd(x, na.rm = TRUE)
  if (!is.finite(sdv) || sdv == 0) return(ifelse(is.na(x), NA_real_, 0))
  (x - mu) / sdv
}

safe_ggsave <- function(p, path, w = 11, h = 6.5, dpi = 320) {
  tryCatch(
    ggsave(filename = path, plot = p, width = w, height = h, units = "in",
           dpi = dpi, bg = "white", limitsize = FALSE),
    error = function(e) stop("Save figure failed: ", path, " | ", conditionMessage(e))
  )
}

scale_color_adv_d <- function(...) {
  if (has_scico) scico::scale_color_scico_d(palette = "batlow", ...)
  else viridis::scale_color_viridis_d(option = "D", end = 0.95, ...)
}
scale_fill_adv_c <- function(...) {
  if (has_scico) scico::scale_fill_scico(palette = "batlow", ...)
  else viridis::scale_fill_viridis(option = "C", end = 0.95, ...)
}

bc_theme <- function() {
  theme_minimal(base_size = 12) +
    theme(
      plot.title.position   = "plot",
      plot.caption.position = "plot",
      plot.title   = element_text(face = "bold", size = 15, margin = margin(b = 6)),
      plot.subtitle= element_text(size = 11, color = "#333333", margin = margin(b = 8)),
      plot.caption = element_text(size = 9,  color = "#444444", margin = margin(t = 10)),
      axis.title   = element_text(face = "bold"),
      axis.text.x  = element_text(angle = 45, hjust = 1, vjust = 1),
      panel.grid.minor = element_blank(),
      plot.margin  = margin(14, 46, 14, 14)
    )
}

safe_quantile_cut <- function(x, probs = c(0, .33, .66, 1), labels = c("Low","Mid","High")) {
  x <- suppressWarnings(as.numeric(x))
  if (length(x) == 0 || all(is.na(x))) return(factor(rep(NA_character_, length(x)), levels = labels))
  br <- stats::quantile(x, probs = probs, na.rm = TRUE, type = 7)
  br <- unique(as.numeric(br))
  if (length(br) < 3) return(factor(rep("Mid", length(x)), levels = labels))
  cut(x, breaks = br, include.lowest = TRUE, labels = labels[seq_len(length(br) - 1)])
}

make_placeholder_plot <- function(title, subtitle = NULL, note = NULL) {
  ggplot() +
    theme_void(base_size = 12) +
    annotate("text", x = 0, y = 0.2, label = title, fontface = "bold", size = 6, hjust = 0) +
    annotate("text", x = 0, y = 0.0, label = subtitle %||% "", size = 4, hjust = 0) +
    annotate("text", x = 0, y = -0.2, label = note %||% "", size = 3.5, hjust = 0) +
    coord_cartesian(xlim = c(0, 1), ylim = c(-0.6, 0.6), clip = "off")
}

save_plot_or_placeholder <- function(expr_plot, out_path, fig_title, w = 12, h = 7) {
  tryCatch({
    p <- force(expr_plot)
    safe_ggsave(p, out_path, w = w, h = h)
    log_msg("Saved figure: ", basename(out_path))
    TRUE
  }, error = function(e) {
    log_msg("WARN figure failed: ", basename(out_path), " | ", conditionMessage(e))
    p <- make_placeholder_plot(
      title = fig_title,
      subtitle = "Figure generation failed; see logs for diagnostics.",
      note = conditionMessage(e)
    )
    safe_ggsave(p, out_path, w = w, h = h)
    FALSE
  })
}

# ---------------------------- 05) Non-throwing TIME_PERIOD parser --------------
# Supports: YYYY, YYYY-MM, YYYY-M, YYYYMM, YYYY/M, YYYYM01, YYYYQ1, YYYY-Q1, YYYY-W05, etc.
parse_tp <- function(tp) {
  tp0 <- trimws(as.character(tp))
  tp0[tp0 == ""] <- NA_character_
  
  # normalize
  x <- tp0
  x <- gsub("\\s+", "", x)
  x <- gsub("/", "-", x, fixed = FALSE)
  x <- gsub("\\.", "-", x)
  
  # 2020M01 / 2020m1 -> 2020-01
  x <- sub("^([0-9]{4})[Mm]([0-9]{1,2})$", "\\1-\\2", x)
  
  # YYYYMM -> YYYY-MM
  x <- sub("^([0-9]{4})([0-9]{2})$", "\\1-\\2", x)
  
  # YYYY-M -> YYYY-0M ; YYYY-MM-D -> pad day; YYYY-M-DD -> pad month
  x <- sub("^([0-9]{4})-([0-9])$", "\\1-0\\2", x)
  x <- sub("^([0-9]{4})-([0-9]{2})-([0-9])$", "\\1-\\2-0\\3", x)
  x <- sub("^([0-9]{4})-([0-9])-([0-9]{2})$", "\\1-0\\2-\\3", x)
  
  # Quarter: YYYYQq / YYYY-Qq -> map to first month of quarter
  q_idx <- !is.na(x) & grepl("^([0-9]{4})-?[Qq]([1-4])$", x)
  if (any(q_idx)) {
    yr <- sub("^([0-9]{4}).*$", "\\1", x[q_idx])
    qq <- as.integer(sub("^.*[Qq]([1-4])$", "\\1", x[q_idx]))
    mm <- sprintf("%02d", (qq - 1L) * 3L + 1L)
    x[q_idx] <- paste0(yr, "-", mm)
  }
  
  out <- rep(as.Date(NA), length(x))
  
  # daily
  idx_d <- !is.na(x) & grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", x)
  if (any(idx_d)) out[idx_d] <- as.Date(x[idx_d], format = "%Y-%m-%d")
  
  # monthly
  idx_m <- is.na(out) & !is.na(x) & grepl("^[0-9]{4}-[0-9]{2}$", x)
  if (any(idx_m)) out[idx_m] <- as.Date(paste0(x[idx_m], "-01"), format = "%Y-%m-%d")
  
  # yearly
  idx_y <- is.na(out) & !is.na(x) & grepl("^[0-9]{4}$", x)
  if (any(idx_y)) out[idx_y] <- as.Date(paste0(x[idx_y], "-01-01"), format = "%Y-%m-%d")
  
  # ISO week: YYYY-Www or YYYYWww
  idx_w <- is.na(out) & !is.na(x) & grepl("^[0-9]{4}-?[Ww][0-9]{1,2}$", x)
  if (any(idx_w)) {
    xw <- x[idx_w]
    xw <- gsub("-", "", xw)
    yr <- sub("^([0-9]{4}).*$", "\\1", xw)
    ww <- as.integer(sub("^.*[Ww]([0-9]{1,2})$", "\\1", xw))
    ww <- sprintf("%02d", ww)
    # Monday of ISO week
    st <- paste0(yr, "-W", ww, "-1")
    tmp <- suppressWarnings(as.Date(strptime(st, format = "%G-W%V-%u", tz = "UTC")))
    out[idx_w] <- tmp
  }
  
  # final fallback: lubridate parse_date_time (never throws)
  rest <- is.na(out) & !is.na(x)
  if (any(rest)) {
    tmp <- suppressWarnings(lubridate::parse_date_time(
      x[rest],
      orders = c("Ymd","Y-m-d","Y/m/d","Ym","Y-m","Y/m","Y"),
      quiet = TRUE,
      tz = "UTC"
    ))
    out[rest] <- as.Date(tmp)
  }
  
  out
}

# ---------------------------- 06) Load data ----------------------------------
bps <- tryCatch(
  data.table::fread(in_paths[["WB_BPS.csv"]], na.strings = c("", "NA", "NaN")),
  error = function(e) stop("Failed reading WB_BPS.csv: ", conditionMessage(e))
) %>% as.data.frame()

req_cols <- c(
  "REF_AREA","REF_AREA_LABEL","INDICATOR","INDICATOR_LABEL","TIME_PERIOD","OBS_VALUE",
  "COMP_BREAKDOWN_1","COMP_BREAKDOWN_1_LABEL",
  "COMP_BREAKDOWN_2","COMP_BREAKDOWN_2_LABEL",
  "COMP_BREAKDOWN_3","COMP_BREAKDOWN_3_LABEL",
  "UNIT_MEASURE","UNIT_MEASURE_LABEL","OBS_STATUS"
)
assert_has_cols(bps, req_cols, "WB_BPS")

bps <- bps %>%
  mutate(
    OBS_VALUE    = suppressWarnings(as.numeric(OBS_VALUE)),
    TIME_PERIOD  = as.character(TIME_PERIOD),
    OBS_STATUS   = ifelse(is.na(OBS_STATUS), "UNK", OBS_STATUS)
  ) %>%
  filter(!is.na(REF_AREA), !is.na(TIME_PERIOD), !is.na(INDICATOR))

bps$time_date <- parse_tp(bps$TIME_PERIOD)

# Audit unparsed TIME_PERIOD (do NOT stop; write audit + proceed)
bad_tp <- bps %>% filter(is.na(time_date)) %>% distinct(TIME_PERIOD) %>% arrange(TIME_PERIOD)
if (nrow(bad_tp) > 0) {
  safe_fwrite(bad_tp, file.path(LOG_DIR, "audit_unparsed_TIME_PERIOD.csv"))
  log_msg("WARN: Unparsed TIME_PERIOD values: ", nrow(bad_tp), " (see logs/audit_unparsed_TIME_PERIOD.csv)")
}

# Keep only rows with parsed dates for time-series analyses/models
bps <- bps %>% filter(!is.na(time_date))

if (nrow(bps) == 0) stop("All rows dropped after TIME_PERIOD parsing; inspect audit_unparsed_TIME_PERIOD.csv.")

safe_fwrite(bps, file.path(DAT_DIR, "bps_long_clean.csv"))

log_msg("Loaded WB_BPS.csv rows=", nrow(bps),
        " | economies=", dplyr::n_distinct(bps$REF_AREA),
        " | periods=", dplyr::n_distinct(bps$TIME_PERIOD))

# Optional metadata reads (non-fatal)
meta_json <- tryCatch(jsonlite::read_json(in_paths[["WB_BPS.json"]], simplifyVector = TRUE), error = function(e) NULL)
datadict  <- tryCatch(readr::read_csv(in_paths[["WB_BPS_DATADICT.csv"]], show_col_types = FALSE), error = function(e) NULL)
widef     <- tryCatch(readr::read_csv(in_paths[["WB_BPS_WIDEF.csv"]], show_col_types = FALSE), error = function(e) NULL)

# ---------------------------- 07) Define indicators ---------------------------
OUTCOME_IND <- c("WB_BPS_ARREARS","WB_BPS_CHANGE_SALES","WB_BPS_DROPSALES","WB_BPS_EXPECTATIONS_SALES")
CAP_IND     <- c("WB_BPS_USE_DIGITAL","WB_BPS_ONLINE_SALES","WB_BPS_REMOTE_WORKERS",
                 "WB_BPS_PROTOCOL_1","WB_BPS_PROTOCOL_2","WB_BPS_PROTOCOL_3","WB_BPS_PROTOCOL_4","WB_BPS_PROTOCOL_5")
STRESS_IND  <- c("WB_BPS_ARREARS","WB_BPS_DROPSALES","WB_BPS_PLANTS_FIRED","WB_BPS_PLANTS_HOURS_CUT",
                 "WB_BPS_PLANTS_WAGES_CUT","WB_BPS_ORDERS_CANCEL","WB_BPS_STOP_SELL","WB_BPS_PLANTS_ABSENCE","WB_BPS_CHANGE_SALES")
SUPPORT_IND <- c("WB_BPS_ACCESS","WB_BPS_RCV_POLICY")

ALL_NEED <- unique(c(OUTCOME_IND, CAP_IND, STRESS_IND, SUPPORT_IND))
ALL_AVAIL <- intersect(ALL_NEED, unique(bps$INDICATOR))

if (length(ALL_AVAIL) < 5) stop("Too few indicators available to proceed. Available=", length(ALL_AVAIL))

# ---------------------------- 08) Table 1: Coverage ---------------------------
tab1 <- bps %>%
  filter(INDICATOR %in% ALL_AVAIL) %>%
  group_by(INDICATOR, INDICATOR_LABEL) %>%
  summarise(
    economies = n_distinct(REF_AREA),
    periods   = n_distinct(TIME_PERIOD),
    rows      = sum(!is.na(OBS_VALUE)),
    .groups = "drop"
  ) %>%
  arrange(desc(rows)) %>%
  mutate(INDICATOR_LABEL = wrap_lab(INDICATOR_LABEL, 60))

tab1_overall <- bps %>%
  filter(INDICATOR %in% ALL_AVAIL) %>%
  summarise(
    INDICATOR = "OVERALL",
    INDICATOR_LABEL = "All required indicators (overall)",
    economies = n_distinct(REF_AREA),
    periods   = n_distinct(TIME_PERIOD),
    rows      = sum(!is.na(OBS_VALUE))
  )

tab1_out <- bind_rows(tab1_overall, tab1)

write_table_dual(
  tab1_out, "Table1_Coverage_ByIndicator",
  "Table 1. Dataset coverage by indicator",
  "Coverage reflects non-missing OBS_VALUE. Modeling uses minimum-completeness rules; sparse cells are handled via robust fallbacks."
)

# ---------------------------- 09) Build panels (TOTAL/SIZE/SECTOR) -----------
key_base <- c("REF_AREA","REF_AREA_LABEL","TIME_PERIOD","time_date")

build_panel <- function(bps, mode = c("total","size","sector")) {
  mode <- match.arg(mode)
  
  d <- bps %>%
    filter(INDICATOR %in% unique(c(OUTCOME_IND, CAP_IND, STRESS_IND, SUPPORT_IND))) %>%
    filter(!is.na(OBS_VALUE))
  
  if (mode == "total") {
    d <- d %>%
      filter(COMP_BREAKDOWN_1 == "_Z", COMP_BREAKDOWN_2 == "_Z") %>%
      mutate(STRATUM = "TOTAL", STRATUM_LABEL = "Total")
  } else if (mode == "size") {
    d <- d %>%
      filter(COMP_BREAKDOWN_2 == "_Z", COMP_BREAKDOWN_1 != "_Z") %>%
      mutate(STRATUM = COMP_BREAKDOWN_1, STRATUM_LABEL = COMP_BREAKDOWN_1_LABEL)
  } else {
    d <- d %>%
      filter(COMP_BREAKDOWN_1 == "_Z", COMP_BREAKDOWN_2 != "_Z") %>%
      mutate(STRATUM = COMP_BREAKDOWN_2, STRATUM_LABEL = COMP_BREAKDOWN_2_LABEL)
  }
  
  if (nrow(d) == 0) return(NULL)
  
  d2 <- d %>%
    group_by(across(all_of(c(key_base, "STRATUM","STRATUM_LABEL","INDICATOR")))) %>%
    summarise(OBS_VALUE = mean(OBS_VALUE, na.rm = TRUE), .groups = "drop")
  
  panel <- d2 %>% tidyr::pivot_wider(names_from = INDICATOR, values_from = OBS_VALUE)
  
  # Protocol mean
  protocol_cols <- c("WB_BPS_PROTOCOL_1","WB_BPS_PROTOCOL_2","WB_BPS_PROTOCOL_3","WB_BPS_PROTOCOL_4","WB_BPS_PROTOCOL_5")
  avail_protocols <- intersect(protocol_cols, names(panel))
  panel <- panel %>%
    mutate(protocols_mean = if (length(avail_protocols) >= 2) rowMeans(across(all_of(avail_protocols)), na.rm = TRUE) else NA_real_)
  
  # ACI
  base_aci <- intersect(c("WB_BPS_USE_DIGITAL","WB_BPS_ONLINE_SALES","WB_BPS_REMOTE_WORKERS","protocols_mean"), names(panel))
  for (v in base_aci) panel[[v]] <- suppressWarnings(as.numeric(panel[[v]]))
  
  aci_use <- base_aci[vapply(base_aci, function(v) {
    vv <- panel[[v]]
    if (all(is.na(vv))) return(FALSE)
    stats::sd(vv, na.rm = TRUE) > 0
  }, logical(1))]
  
  panel <- panel %>%
    mutate(
      ACI = if (length(aci_use) >= 2) {
        zmat <- do.call(cbind, lapply(aci_use, function(v) zscore(panel[[v]])))
        num <- rowSums(zmat, na.rm = TRUE)
        den <- rowSums(!is.na(zmat))
        ifelse(den >= 2, num / den, NA_real_)
      } else NA_real_
    )
  
  # CSI (stress); flip sales change
  if ("WB_BPS_CHANGE_SALES" %in% names(panel)) {
    panel$change_sales_stress <- -1 * suppressWarnings(as.numeric(panel$WB_BPS_CHANGE_SALES))
  } else {
    panel$change_sales_stress <- NA_real_
  }
  
  stress_cols <- intersect(setdiff(STRESS_IND, "WB_BPS_CHANGE_SALES"), names(panel))
  csi_use <- unique(c(stress_cols, "change_sales_stress"))
  csi_use <- csi_use[csi_use %in% names(panel)]
  csi_use <- csi_use[vapply(csi_use, function(v) {
    vv <- suppressWarnings(as.numeric(panel[[v]]))
    if (all(is.na(vv))) return(FALSE)
    stats::sd(vv, na.rm = TRUE) > 0
  }, logical(1))]
  
  panel <- panel %>%
    mutate(
      CSI = if (length(csi_use) >= 3) {
        zmat <- do.call(cbind, lapply(csi_use, function(v) zscore(panel[[v]])))
        num <- rowSums(zmat, na.rm = TRUE)
        den <- rowSums(!is.na(zmat))
        ifelse(den >= 3, num / den, NA_real_)
      } else NA_real_
    )
  
  # Lags within economy–stratum
  panel <- panel %>%
    arrange(REF_AREA, STRATUM, time_date) %>%
    group_by(REF_AREA, STRATUM) %>%
    mutate(
      ACI_l1     = dplyr::lag(ACI, 1),
      support_l1 = if ("WB_BPS_ACCESS" %in% names(.)) dplyr::lag(WB_BPS_ACCESS, 1) else NA_real_
    ) %>%
    ungroup()
  
  panel
}

panel_total  <- build_panel(bps, "total")
panel_size   <- build_panel(bps, "size")
panel_sector <- build_panel(bps, "sector")

safe_fwrite_or_note(panel_total,  file.path(DAT_DIR, "panel_total.csv"),  "panel_total not available.")
safe_fwrite_or_note(panel_size,   file.path(DAT_DIR, "panel_size.csv"),   "panel_size not available.")
safe_fwrite_or_note(panel_sector, file.path(DAT_DIR, "panel_sector.csv"), "panel_sector not available.")

log_msg("Panel sizes: total=", ifelse(is.null(panel_total), 0, nrow(panel_total)),
        " | size=", ifelse(is.null(panel_size), 0, nrow(panel_size)),
        " | sector=", ifelse(is.null(panel_sector), 0, nrow(panel_sector)))

choose_panel <- function(...) {
  panels <- list(...)
  for (p in panels) if (!is.null(p) && nrow(p) >= 80) return(p)
  for (p in panels) if (!is.null(p) && nrow(p) >= 40) return(p)
  for (p in panels) if (!is.null(p)) return(p)
  NULL
}

panel_model <- choose_panel(panel_size, panel_sector, panel_total)
if (is.null(panel_model) || nrow(panel_model) < 40) stop("Insufficient data to model (no panel with >=40 rows).")
safe_fwrite(panel_model, file.path(DAT_DIR, "panel_model_pre_impute.csv"))

# ---------------------------- 10) Deterministic time-series fill (Stage A) ----
ts_fill_group <- function(df, vars, group_cols = c("REF_AREA","STRATUM"), time_col = "time_date") {
  df <- df %>% arrange(across(all_of(group_cols)), .data[[time_col]])
  df %>%
    group_by(across(all_of(group_cols))) %>%
    mutate(across(all_of(vars), ~{
      x <- suppressWarnings(as.numeric(.x))
      if (all(is.na(x))) return(x)
      # interpolate internal gaps
      x2 <- suppressWarnings(zoo::na.approx(x, x = .data[[time_col]], na.rm = FALSE))
      # fill edges
      x3 <- suppressWarnings(zoo::na.locf(x2, na.rm = FALSE))
      x4 <- suppressWarnings(zoo::na.locf(x3, fromLast = TRUE, na.rm = FALSE))
      x4
    })) %>%
    ungroup()
}

# Only fill predictors (avoid imputing outcomes deterministically)
pred_fill_vars <- intersect(c("ACI","ACI_l1","WB_BPS_ACCESS","support_l1"), names(panel_model))
panel_model_f <- if (length(pred_fill_vars) > 0) ts_fill_group(panel_model, pred_fill_vars) else panel_model
safe_fwrite(panel_model_f, file.path(DAT_DIR, "panel_model_stageA_filled.csv"))

# ---------------------------- 11) Tables 2–4 (descriptives) -------------------
make_outcome_desc <- function(df, title_stub, out_prefix) {
  if (is.null(df) || nrow(df) == 0) {
    write_table_dual(NULL, out_prefix, title_stub, "No data available.")
    return(NULL)
  }
  tab <- df %>%
    transmute(
      Stratum = STRATUM_LABEL,
      ArrearsRisk = WB_BPS_ARREARS,
      SalesChange = WB_BPS_CHANGE_SALES,
      DropSales   = WB_BPS_DROPSALES,
      SalesExp6m  = WB_BPS_EXPECTATIONS_SALES
    ) %>%
    group_by(Stratum) %>%
    summarise(
      n = n(),
      Arrears_mean = safe_mean(ArrearsRisk),
      Arrears_p50  = safe_quantile(ArrearsRisk, 0.50),
      SalesChange_mean = safe_mean(SalesChange),
      DropSales_mean   = safe_mean(DropSales),
      SalesExp6m_mean  = safe_mean(SalesExp6m),
      .groups = "drop"
    ) %>%
    mutate(Stratum = wrap_lab(Stratum, 28)) %>%
    arrange(desc(n))
  
  write_table_dual(tab, out_prefix, title_stub,
                   "Unweighted summaries over available economy–period–stratum observations. Values are typically percentage points.")
  tab
}

make_cap_desc <- function(df, title_stub, out_prefix) {
  if (is.null(df) || nrow(df) == 0) {
    write_table_dual(NULL, out_prefix, title_stub, "No data available.")
    return(NULL)
  }
  tab <- df %>%
    transmute(
      Stratum = STRATUM_LABEL,
      UseDigital = WB_BPS_USE_DIGITAL,
      OnlineSalesShare = WB_BPS_ONLINE_SALES,
      RemoteWorkersShare = WB_BPS_REMOTE_WORKERS,
      ProtocolsMean = protocols_mean,
      ACI = ACI,
      CSI = CSI
    ) %>%
    group_by(Stratum) %>%
    summarise(
      n = n(),
      UseDigital_mean = safe_mean(UseDigital),
      OnlineSales_mean = safe_mean(OnlineSalesShare),
      RemoteWork_mean = safe_mean(RemoteWorkersShare),
      Protocols_mean = safe_mean(ProtocolsMean),
      ACI_mean = safe_mean(ACI),
      CSI_mean = safe_mean(CSI),
      .groups = "drop"
    ) %>%
    mutate(Stratum = wrap_lab(Stratum, 28)) %>%
    arrange(desc(n))
  
  write_table_dual(tab, out_prefix, title_stub,
                   "ACI: higher = stronger adaptation capability; CSI: higher = worse continuity stress. Indices are standardized composites with minimum-component rules.")
  tab
}

tab2 <- make_outcome_desc(panel_size,   "Table 2. Primary outcomes by firm size", "Table2_Outcomes_BySize")
tab3 <- make_outcome_desc(panel_sector, "Table 3. Primary outcomes by sector",    "Table3_Outcomes_BySector")
tab4 <- make_cap_desc(panel_size,       "Table 4. Capabilities and indices by firm size", "Table4_Capabilities_BySize")

# ---------------------------- 12) Core FE models with valid p-values -----------
panel_model_f <- panel_model_f %>%
  mutate(
    time_fe  = factor(TIME_PERIOD),
    econ_fe  = factor(REF_AREA),
    strat_fe = factor(STRATUM)
  )

if (!("WB_BPS_ACCESS" %in% names(panel_model_f))) panel_model_f$WB_BPS_ACCESS <- NA_real_

lag_feasible <- function(df) {
  if (!all(c("ACI_l1","support_l1","WB_BPS_ARREARS") %in% names(df))) return(FALSE)
  ok <- sum(is.finite(df$ACI_l1) & is.finite(df$support_l1) & is.finite(df$WB_BPS_ARREARS))
  ok >= 60
}
use_lag <- lag_feasible(panel_model_f)
x_label <- if (use_lag) "lag(1)" else "contemporaneous"
log_msg("Lag(1) feasible? ", use_lag)

panel_model_f$ACI_x <- if (use_lag) panel_model_f$ACI_l1 else panel_model_f$ACI
panel_model_f$sup_x <- if (use_lag) panel_model_f$support_l1 else panel_model_f$WB_BPS_ACCESS
panel_model_f$int_x <- panel_model_f$ACI_x * panel_model_f$sup_x

fe_terms <- c("econ_fe","time_fe","strat_fe")

fit_fe_fixest <- function(data, y, x_terms, fe_terms, cluster = "REF_AREA", model_name = "model") {
  if (!y %in% names(data)) { log_msg("WARN: outcome missing for ", model_name); return(NULL) }
  
  d <- data %>% filter(is.finite(.data[[y]]))
  if (nrow(d) < 60) { log_msg("WARN: ", model_name, " has <60 usable rows; returning NULL."); return(NULL) }
  
  x_ok <- c()
  for (xt in x_terms) {
    if (!xt %in% names(d)) next
    v <- suppressWarnings(as.numeric(d[[xt]]))
    v[!is.finite(v)] <- NA_real_
    if (all(is.na(v))) next
    if (isTRUE(stats::sd(v, na.rm = TRUE) == 0)) next
    x_ok <- c(x_ok, xt)
  }
  if (length(x_ok) == 0) { log_msg("WARN: ", model_name, " has no usable predictors; returning NULL."); return(NULL) }
  
  fe_ok <- fe_terms[fe_terms %in% names(d)]
  fe_ok <- fe_ok[vapply(fe_ok, function(f) length(unique(d[[f]])) > 1, logical(1))]
  
  rhs <- paste(x_ok, collapse = " + ")
  fml <- if (length(fe_ok) > 0) as.formula(paste0(y, " ~ ", rhs, " | ", paste(fe_ok, collapse = " + "))) else as.formula(paste0(y, " ~ ", rhs))
  
  use_cluster <- FALSE
  ncl <- NA_integer_
  if (!is.null(cluster) && cluster %in% names(d)) {
    ncl <- length(unique(d[[cluster]]))
    if (ncl >= 10) use_cluster <- TRUE else log_msg("WARN: ", model_name, " has <10 clusters; fitting without clustering.")
  }
  
  m <- tryCatch({
    if (use_cluster) feols(fml, data = d, cluster = as.formula(paste0("~", cluster)))
    else feols(fml, data = d)
  }, error = function(e) {
    log_msg("WARN: fixest failed for ", model_name, " | ", conditionMessage(e))
    NULL
  })
  
  attr(m, "n_clusters") <- ncl
  attr(m, "clustered")  <- isTRUE(use_cluster)
  m
}

# Robust tidy with p-values always computed (no NA p-values unless SE is NA)
tidy_fixest <- function(m, keep_terms, model_label) {
  if (is.null(m)) return(NULL)
  if (!inherits(m, "fixest")) return(NULL)
  
  ct <- tryCatch(fixest::coeftable(m), error = function(e) NULL)
  if (is.null(ct)) return(NULL)
  
  ct <- as.data.frame(ct)
  ct$term <- rownames(ct)
  rownames(ct) <- NULL
  ct <- ct %>% filter(term %in% keep_terms)
  
  if (nrow(ct) == 0) return(NULL)
  
  # standard columns: Estimate, Std. Error, t value, Pr(>|t|)
  est <- ct$Estimate
  se  <- ct$`Std. Error`
  tval <- est / se
  
  clustered <- isTRUE(attr(m, "clustered"))
  ncl <- attr(m, "n_clusters")
  df <- if (clustered && is.finite(ncl) && ncl >= 2) (ncl - 1) else max(1, fixest::nobs(m) - length(coef(m)) - 1)
  
  pval <- 2 * stats::pt(abs(tval), df = df, lower.tail = FALSE)
  crit <- stats::qt(0.975, df = df)
  
  out <- data.frame(
    model = model_label,
    term = ct$term,
    estimate = as.numeric(est),
    std.error = as.numeric(se),
    conf.low = as.numeric(est - crit * se),
    conf.high = as.numeric(est + crit * se),
    p.value = as.numeric(pval),
    stringsAsFactors = FALSE
  )
  
  out
}

format_terms <- function(x) dplyr::recode(
  x,
  "ACI_x" = paste0("ACI (", x_label, ")"),
  "sup_x" = paste0("Public support access (", x_label, ")"),
  "int_x" = paste0("ACI × Support (", x_label, ")"),
  .default = x
)

m_arrears <- fit_fe_fixest(panel_model_f, "WB_BPS_ARREARS",      c("ACI_x","sup_x","int_x"), fe_terms, cluster = "REF_AREA",
                           model_name = paste0("arrears_FE_", x_label))
m_sales   <- fit_fe_fixest(panel_model_f, "WB_BPS_CHANGE_SALES", c("ACI_x","sup_x","int_x"), fe_terms, cluster = "REF_AREA",
                           model_name = paste0("sales_FE_", x_label))

saveRDS(m_arrears, file.path(MOD_DIR, paste0("model_arrears_", x_label, ".rds")))
saveRDS(m_sales,   file.path(MOD_DIR, paste0("model_saleschange_", x_label, ".rds")))

tab5 <- tidy_fixest(m_arrears, c("ACI_x","sup_x","int_x"), paste0("Arrears risk FE (", x_label, ")"))
if (!is.null(tab5)) {
  tab5$term <- format_terms(tab5$term)
  write_table_dual(
    tab5, "Table5_FE_Arrears",
    "Table 5. Fixed-effects model — arrears risk",
    "Economy, period, and stratum fixed effects included where estimable. SEs clustered at economy when feasible; p-values use t(df=clusters−1) when clustered."
  )
} else {
  write_table_dual(NULL, "Table5_FE_Arrears",
                   "Table 5. Fixed-effects model — arrears risk",
                   "Model not estimable with current completeness/variance constraints.")
}

tab6 <- tidy_fixest(m_sales, c("ACI_x","sup_x","int_x"), paste0("Sales change FE (", x_label, ")"))
if (!is.null(tab6)) {
  tab6$term <- format_terms(tab6$term)
  write_table_dual(
    tab6, "Table6_FE_SalesChange",
    "Table 6. Fixed-effects model — monthly sales change",
    "Economy, period, and stratum fixed effects included where estimable. Negative values indicate contraction vs prior year. SEs clustered at economy when feasible."
  )
} else {
  write_table_dual(NULL, "Table6_FE_SalesChange",
                   "Table 6. Fixed-effects model — monthly sales change",
                   "Model not estimable with current completeness/variance constraints.")
}

# ---------------------------- 13) Optional MI (Stage B) pooled sensitivity ----
# This is run only if DO_MI=1 and there is remaining missingness in predictors.
pool_rubin <- function(Q, U) {
  # Q: m x k matrix of estimates; U: list of kxk variance matrices length m
  m <- nrow(Q)
  k <- ncol(Q)
  qbar <- colMeans(Q)
  Ubar <- Reduce(`+`, U) / m
  B <- stats::cov(Q)
  Tmat <- Ubar + (1 + 1/m) * B
  se <- sqrt(diag(Tmat))
  
  # Barnard-Rubin df approximation (vectorized)
  # lambda = (1 + 1/m) * diag(B) / diag(Tmat)
  lam <- (1 + 1/m) * diag(B) / diag(Tmat)
  # df_old ~ (m-1)/lambda^2
  df <- (m - 1) / (lam^2)
  df[!is.finite(df) | df < 1] <- 9999
  
  list(est = qbar, se = se, df = df, vcov = Tmat)
}

run_mi_fixest <- function(df, y, fe_terms, cluster = "REF_AREA", m = 20, maxit = 20) {
  need <- c(y, "ACI_x", "sup_x", "econ_fe", "time_fe", "strat_fe", cluster)
  need <- intersect(need, names(df))
  d0 <- df %>% select(all_of(need)) %>% mutate(across(where(is.character), as.factor))
  
  # Drop rows missing outcome (standard practice), impute RHS only
  d0 <- d0 %>% filter(is.finite(.data[[y]]))
  if (nrow(d0) < 80) return(NULL)
  
  miss_rate <- mean(is.na(d0$ACI_x) | is.na(d0$sup_x))
  if (!is.finite(miss_rate) || miss_rate < 0.01) return(NULL)
  
  # mice setup: impute ACI_x, sup_x using PMM; do NOT impute FE factors
  ini <- suppressWarnings(mice(d0, m = 1, maxit = 0, printFlag = FALSE))
  meth <- ini$method
  pred <- ini$predictorMatrix
  
  # only impute ACI_x and sup_x
  for (nm in names(meth)) meth[nm] <- ""
  for (nm in intersect(c("ACI_x","sup_x"), names(meth))) meth[nm] <- "pmm"
  
  # prevent using FE IDs as predictors in a way that explodes dimension
  # allow cluster as a predictor only if it helps; typically safe to exclude
  pred[,] <- 0
  # numeric predictors among the two
  pred["ACI_x","sup_x"] <- 1
  pred["sup_x","ACI_x"] <- 1
  
  imp <- tryCatch(
    mice(d0, m = m, maxit = maxit, method = meth, predictorMatrix = pred, printFlag = FALSE, seed = 20260121),
    error = function(e) NULL
  )
  if (is.null(imp)) return(NULL)
  
  models <- vector("list", m)
  Q <- NULL
  U <- vector("list", m)
  
  for (i in 1:m) {
    di <- complete(imp, i)
    di$int_x <- di$ACI_x * di$sup_x
    fml <- as.formula(paste0(y, " ~ ACI_x + sup_x + int_x | ",
                             paste(intersect(fe_terms, names(di)), collapse = " + ")))
    mi_mod <- tryCatch(
      feols(fml, data = di, cluster = as.formula(paste0("~", cluster))),
      error = function(e) NULL
    )
    if (is.null(mi_mod)) next
    models[[i]] <- mi_mod
    b <- coef(mi_mod)[c("ACI_x","sup_x","int_x")]
    V <- tryCatch(vcov(mi_mod)[c("ACI_x","sup_x","int_x"), c("ACI_x","sup_x","int_x"), drop = FALSE], error = function(e) NULL)
    if (any(is.na(b)) || is.null(V)) next
    
    if (is.null(Q)) Q <- matrix(NA_real_, nrow = 0, ncol = 3, dimnames = list(NULL, c("ACI_x","sup_x","int_x")))
    Q <- rbind(Q, as.numeric(b))
    U[[nrow(Q)]] <- V
  }
  
  if (is.null(Q) || nrow(Q) < 5) return(NULL)
  colnames(Q) <- c("ACI_x","sup_x","int_x")
  U <- U[seq_len(nrow(Q))]
  
  pooled <- pool_rubin(Q, U)
  pooled
}

tab5_mi <- NULL
if (DO_MI && !is.null(m_arrears)) {
  pooled <- run_mi_fixest(panel_model_f, "WB_BPS_ARREARS", fe_terms, cluster = "REF_AREA", m = 20, maxit = 20)
  if (!is.null(pooled)) {
    est <- pooled$est; se <- pooled$se; df <- pooled$df
    tval <- est / se
    pval <- 2 * pt(abs(tval), df = df, lower.tail = FALSE)
    crit <- qt(0.975, df = df)
    tab5_mi <- data.frame(
      model = paste0("Arrears risk FE MI (", x_label, ")"),
      term = c("ACI_x","sup_x","int_x"),
      estimate = est,
      std.error = se,
      conf.low = est - crit * se,
      conf.high = est + crit * se,
      p.value = pval
    )
    tab5_mi$term <- format_terms(tab5_mi$term)
    write_table_dual(
      tab5_mi, "Table5b_FE_Arrears_MI",
      "Table 5b. Fixed-effects model — arrears risk (multiple imputation sensitivity)",
      "MI uses PMM for predictors (ACI_x, sup_x) after deterministic time-series fill; Rubin-pooled estimates with Barnard–Rubin df. Outcome not imputed."
    )
  }
}

# ---------------------------- 14) Table 7: Archetypes -------------------------
eco_features <- panel_model_f %>%
  group_by(REF_AREA, REF_AREA_LABEL) %>%
  summarise(
    ACI_mean         = safe_mean(ACI),
    CSI_mean         = safe_mean(CSI),
    Arrears_mean     = safe_mean(WB_BPS_ARREARS),
    SalesChange_mean = safe_mean(WB_BPS_CHANGE_SALES),
    Support_mean     = safe_mean(WB_BPS_ACCESS),
    n = n(),
    .groups = "drop"
  )

make_kmeans_matrix <- function(df, vars) {
  M <- df[, vars, drop = FALSE]
  for (v in names(M)) {
    x <- suppressWarnings(as.numeric(M[[v]]))
    x[!is.finite(x)] <- NA_real_
    M[[v]] <- x
  }
  keep_col <- vapply(names(M), function(v) sum(is.finite(M[[v]])) >= 6, logical(1))
  M <- M[, keep_col, drop = FALSE]
  if (ncol(M) < 2) return(list(mat = NULL, keep = rep(FALSE, nrow(df)), cols = character(0)))
  
  for (v in names(M)) {
    med <- suppressWarnings(stats::median(M[[v]], na.rm = TRUE))
    if (!is.finite(med)) med <- 0
    M[[v]][is.na(M[[v]])] <- med
  }
  for (v in names(M)) {
    mu  <- mean(M[[v]])
    sdv <- stats::sd(M[[v]])
    if (!is.finite(sdv) || sdv == 0) M[[v]] <- 0 else M[[v]] <- (M[[v]] - mu) / sdv
  }
  
  mat <- as.matrix(M)
  keep_row <- apply(mat, 1, function(r) all(is.finite(r)))
  list(mat = mat[keep_row, , drop = FALSE], keep = keep_row, cols = colnames(mat))
}

km_vars <- c("ACI_mean","CSI_mean","Arrears_mean","SalesChange_mean","Support_mean")
km_obj  <- make_kmeans_matrix(eco_features, km_vars)

if (!is.null(km_obj$mat) && nrow(km_obj$mat) >= 12) {
  k <- max(2, min(4, floor(nrow(km_obj$mat) / 8)))
  k <- min(k, nrow(km_obj$mat) - 1)
  
  km <- tryCatch(kmeans(km_obj$mat, centers = k, nstart = 100),
                 error = function(e) kmeans(km_obj$mat, centers = 2, nstart = 50))
  
  eco_features$cluster <- NA_character_
  eco_features$cluster[km_obj$keep] <- paste0("Archetype ", km$cluster)
  
  tab7 <- eco_features %>%
    filter(!is.na(cluster)) %>%
    group_by(cluster) %>%
    summarise(
      economies = n(),
      ACI_mean = safe_mean(ACI_mean),
      CSI_mean = safe_mean(CSI_mean),
      Arrears_mean = safe_mean(Arrears_mean),
      SalesChange_mean = safe_mean(SalesChange_mean),
      Support_mean = safe_mean(Support_mean),
      .groups = "drop"
    ) %>%
    arrange(desc(economies))
  
  write_table_dual(tab7, "Table7_ResilienceArchetypes",
                   "Table 7. Resilience archetypes (economy-level clusters)",
                   "Typology derived from standardized economy-level averages; intended for strategic segmentation rather than causal classification.")
} else {
  tab7 <- eco_features %>% arrange(desc(n))
  write_table_dual(tab7, "Table7_EconomyProfiles_Fallback",
                   "Table 7. Economy profiles (fallback; insufficient for clustering)",
                   "Not enough economies with complete features for stable clustering; reporting economy-level averages instead.")
}

# ---------------------------- 15) Figures (7 PNG) -----------------------------
label_pct <- scales::label_number(accuracy = 1, suffix = "%")
label_idx <- scales::label_number(accuracy = 0.1)

# Figure 1: Arrears trend by firm size
save_plot_or_placeholder({
  fig1_df <- panel_size %>%
    group_by(time_date, STRATUM_LABEL) %>%
    summarise(arrears = safe_mean(WB_BPS_ARREARS), .groups = "drop") %>%
    filter(is.finite(arrears))
  
  ggplot(fig1_df, aes(x = time_date, y = arrears, color = STRATUM_LABEL)) +
    geom_line(linewidth = 1.1, alpha = 0.95) +
    geom_point(size = 2.0, alpha = 0.95) +
    scale_color_adv_d(name = "Firm size") +
    scale_y_continuous(labels = label_pct, expand = expansion(mult = c(0.02, 0.08))) +
    scale_x_date(date_breaks = "2 months", date_labels = "%Y-%m") +
    labs(
      title = "Figure 1. Liquidity stress: Share of firms with arrears over time, by firm size",
      subtitle = "WB Business Pulse Surveys (economy–month aggregates by size stratum).",
      x = "Survey month", y = "Firms in arrears",
      caption = "Note: Values are typically percentage points. Plot margins expanded; clipping disabled."
    ) +
    bc_theme() +
    coord_cartesian(clip = "off")
}, file.path(FIG_DIR, "Figure1_ArrearsTrend_BySize.png"),
fig_title = "Figure 1. Arrears trend by firm size", w = 12, h = 7)

# Figure 2: Sales change trend by firm size
save_plot_or_placeholder({
  fig2_df <- panel_size %>%
    group_by(time_date, STRATUM_LABEL) %>%
    summarise(saleschg = safe_mean(WB_BPS_CHANGE_SALES), .groups = "drop") %>%
    filter(is.finite(saleschg))
  
  ggplot(fig2_df, aes(x = time_date, y = saleschg, color = STRATUM_LABEL)) +
    geom_hline(yintercept = 0, linewidth = 0.6, alpha = 0.35) +
    geom_line(linewidth = 1.1, alpha = 0.95) +
    geom_point(size = 2.0, alpha = 0.95) +
    scale_color_adv_d(name = "Firm size") +
    scale_y_continuous(labels = label_pct, expand = expansion(mult = c(0.08, 0.10))) +
    scale_x_date(date_breaks = "2 months", date_labels = "%Y-%m") +
    labs(
      title = "Figure 2. Revenue shock: Monthly sales change over time, by firm size",
      subtitle = "Negative values indicate contraction vs prior year (per WB_BPS definition).",
      x = "Survey month", y = "Sales change",
      caption = "Note: Series are unweighted means across economies with available reports."
    ) +
    bc_theme() +
    coord_cartesian(clip = "off")
}, file.path(FIG_DIR, "Figure2_SalesChangeTrend_BySize.png"),
fig_title = "Figure 2. Sales change trend by firm size", w = 12, h = 7)

# Figure 3: ACI vs Arrears (scatter + smooth), by size
save_plot_or_placeholder({
  fig3_df <- panel_size %>%
    transmute(ACI = ACI, Arrears = WB_BPS_ARREARS, Size = STRATUM_LABEL) %>%
    filter(is.finite(ACI), is.finite(Arrears))
  
  ggplot(fig3_df, aes(x = ACI, y = Arrears, color = Size)) +
    geom_point(alpha = 0.55, size = 2.0) +
    geom_smooth(method = "lm", se = TRUE, linewidth = 1.1, alpha = 0.12) +
    scale_color_adv_d(name = "Firm size") +
    scale_y_continuous(labels = label_pct, expand = expansion(mult = c(0.03, 0.08))) +
    scale_x_continuous(labels = label_idx, expand = expansion(mult = c(0.03, 0.05))) +
    labs(
      title = "Figure 3. Adaptation capability vs liquidity stress",
      subtitle = "Association between ACI (standardized composite) and arrears risk, by firm size.",
      x = "Adaptation Capability Index (ACI, standardized)", y = "Firms in arrears",
      caption = "Note: ACI aggregates digital adoption, online sales, remote work, and protocols. Descriptive association; see FE models."
    ) +
    bc_theme() +
    coord_cartesian(clip = "off")
}, file.path(FIG_DIR, "Figure3_ACI_vs_Arrears_BySize.png"),
fig_title = "Figure 3. ACI vs arrears by size", w = 12, h = 7)

# Figure 4: Capability–support complementarity (binned means)
save_plot_or_placeholder({
  fig4_df <- panel_model_f %>%
    filter(is.finite(ACI_x), is.finite(sup_x), is.finite(WB_BPS_ARREARS)) %>%
    mutate(
      SupportTier = safe_quantile_cut(sup_x, probs = c(0, .33, .66, 1), labels = c("Low support","Mid support","High support")),
      ACIBin = safe_quantile_cut(ACI_x, probs = seq(0, 1, by = 0.2), labels = paste0("ACI Q", 1:5))
    ) %>%
    group_by(SupportTier, ACIBin) %>%
    summarise(
      ACI_mid = safe_mean(ACI_x),
      Arrears = safe_mean(WB_BPS_ARREARS),
      n = n(),
      .groups = "drop"
    ) %>%
    filter(is.finite(ACI_mid), is.finite(Arrears))
  
  ggplot(fig4_df, aes(x = ACI_mid, y = Arrears, color = SupportTier)) +
    geom_line(linewidth = 1.1, alpha = 0.95) +
    geom_point(aes(size = n), alpha = 0.95) +
    scale_color_adv_d(name = "Public support tier") +
    scale_size_continuous(name = "Cells", range = c(2, 6)) +
    scale_y_continuous(labels = label_pct, expand = expansion(mult = c(0.03, 0.10))) +
    scale_x_continuous(labels = label_idx, expand = expansion(mult = c(0.03, 0.05))) +
    labs(
      title = "Figure 4. Capability–support complementarity",
      subtitle = paste0("Binned means using ", x_label, " predictors: arrears risk across ACI, stratified by support intensity."),
      x = paste0("ACI (", x_label, ", standardized)"), y = "Firms in arrears",
      caption = "Note: Support tiers are within-sample tertiles. Visualization complements FE interaction terms."
    ) +
    bc_theme() +
    coord_cartesian(clip = "off")
}, file.path(FIG_DIR, "Figure4_Arrears_by_ACI_by_SupportTier.png"),
fig_title = "Figure 4. ACI-by-support tiers", w = 12, h = 7)

# Figure 5: Heatmap — arrears over time by economy (TOP 30 by mean arrears)
save_plot_or_placeholder({
  if (is.null(panel_total) || nrow(panel_total) == 0) stop("panel_total is empty; cannot construct economy heatmap.")
  
  heat_df <- panel_total %>%
    transmute(Economy = wrap_lab(REF_AREA_LABEL, 28), time_date = time_date, Arrears = WB_BPS_ARREARS) %>%
    filter(is.finite(Arrears))
  
  if (nrow(heat_df) == 0) stop("No finite arrears values in TOTAL panel.")
  
  top_econs <- heat_df %>%
    group_by(Economy) %>%
    summarise(m = safe_mean(Arrears), n = dplyr::n(), .groups = "drop") %>%
    arrange(desc(m), desc(n)) %>%
    slice_head(n = 30) %>%
    pull(Economy)
  
  heat_df2 <- heat_df %>%
    filter(Economy %in% top_econs) %>%
    mutate(Economy = factor(Economy, levels = rev(top_econs)))
  
  ggplot(heat_df2, aes(x = time_date, y = Economy, fill = Arrears)) +
    geom_tile(alpha = 0.98) +
    scale_fill_adv_c(name = "Arrears", labels = label_pct) +
    scale_x_date(date_breaks = "2 months", date_labels = "%Y-%m", expand = expansion(mult = c(0.01, 0.03))) +
    labs(
      title = "Figure 5. Liquidity stress heatmap: Arrears by economy over time (top 30 by mean arrears)",
      subtitle = "TOTAL stratum only; economies ordered by mean arrears across observed months.",
      x = "Survey month", y = NULL,
      caption = "Note: TOTAL stratum uses COMP_BREAKDOWN_1=_Z and COMP_BREAKDOWN_2=_Z. Margins expanded; clipping disabled."
    ) +
    bc_theme() +
    theme(axis.text.y = element_text(size = 9)) +
    coord_cartesian(clip = "off")
}, file.path(FIG_DIR, "Figure5_Heatmap_Arrears_ByEconomy_Top30.png"),
fig_title = "Figure 5. Heatmap (arrears by economy, top 30)", w = 14, h = 10)

# Figure 6: Sector resilience profile (ACI vs CSI)
save_plot_or_placeholder({
  if (is.null(panel_sector) || nrow(panel_sector) == 0) stop("panel_sector is empty; cannot build sector profile.")
  sec_df <- panel_sector %>%
    group_by(STRATUM_LABEL) %>%
    summarise(
      n = n(),
      ACI_m = safe_mean(ACI),
      CSI_m = safe_mean(CSI),
      .groups = "drop"
    ) %>%
    filter(is.finite(ACI_m) | is.finite(CSI_m)) %>%
    mutate(Sector = wrap_lab(STRATUM_LABEL, 26)) %>%
    arrange(desc(n))
  
  sec_long <- sec_df %>%
    select(Sector, n, ACI_m, CSI_m) %>%
    pivot_longer(cols = c(ACI_m, CSI_m), names_to = "Metric", values_to = "Value") %>%
    mutate(
      Metric = recode(Metric, ACI_m = "Adaptation Capability (ACI)", CSI_m = "Continuity Stress (CSI)"),
      Sector = factor(Sector, levels = rev(unique(sec_df$Sector)))
    )
  
  ggplot(sec_long, aes(x = Value, y = Sector, color = Metric)) +
    geom_vline(xintercept = 0, alpha = 0.20) +
    geom_point(size = 3.0, alpha = 0.95) +
    scale_color_adv_d(name = NULL) +
    scale_x_continuous(labels = label_idx, expand = expansion(mult = c(0.06, 0.12))) +
    labs(
      title = "Figure 6. Sector resilience profile: capability vs stress (standardized indices)",
      subtitle = "Sector means of ACI and CSI computed from sector panel; higher ACI is better, higher CSI is worse.",
      x = "Index value (standardized)", y = NULL,
      caption = "Note: Indices are standardized composites; interpret relative positions rather than absolute magnitudes."
    ) +
    bc_theme() +
    coord_cartesian(clip = "off")
}, file.path(FIG_DIR, "Figure6_Sector_Profile_ACI_CSI.png"),
fig_title = "Figure 6. Sector profile (ACI vs CSI)", w = 12, h = 9)

# Figure 7: Coefficient plot (ACI, support, interaction) for arrears + sales change
save_plot_or_placeholder({
  if (is.null(tab5) && is.null(tab6)) stop("No estimable models for coefficient plot.")
  coef_df <- bind_rows(tab5 %||% NULL, tab6 %||% NULL) %>%
    mutate(outcome = ifelse(grepl("^Arrears", model), "Arrears risk", "Sales change")) %>%
    select(outcome, term, estimate, conf.low, conf.high) %>%
    mutate(term = factor(term, levels = rev(unique(term))))
  
  ggplot(coef_df, aes(x = estimate, y = term)) +
    geom_vline(xintercept = 0, alpha = 0.25) +
    geom_errorbar(aes(xmin = conf.low, xmax = conf.high), width = 0.20, orientation = "y", alpha = 0.9) +
    geom_point(size = 2.8, alpha = 0.95) +
    facet_wrap(~ outcome, scales = "free_x") +
    scale_x_continuous(labels = label_idx, expand = expansion(mult = c(0.10, 0.15))) +
    labs(
      title = "Figure 7. Fixed-effects associations: capability, support, and their interaction",
      subtitle = paste0("Models estimated using ", x_label, " predictors; 95% t-based intervals shown."),
      x = "Coefficient estimate", y = NULL,
      caption = "Note: Associational fixed-effects estimates with economy, period, and stratum FE where estimable."
    ) +
    bc_theme() +
    coord_cartesian(clip = "off")
}, file.path(FIG_DIR, "Figure7_FE_Coefficients.png"),
fig_title = "Figure 7. FE coefficient plot", w = 12, h = 7)

# ---------------------------- 16) Manifest + sessionInfo -----------------------
manifest <- list(
  run_timestamp_utc = as.character(Sys.time()),
  data_root = DATA_ROOT,
  out_dir = OUT_DIR,
  inputs = in_paths,
  outputs = list(
    figures_dir = FIG_DIR,
    tables_dir = TAB_DIR,
    models_dir = MOD_DIR,
    data_dir   = DAT_DIR,
    log_path   = log_path
  ),
  md5_inputs = tryCatch(as.list(tools::md5sum(unname(in_paths))), error = function(e) NULL)
)

writeLines(jsonlite::toJSON(manifest, pretty = TRUE, auto_unbox = TRUE),
           con = file.path(OUT_DIR, "manifest.json"), useBytes = TRUE)
writeLines(capture.output(sessionInfo()), con = file.path(OUT_DIR, "sessionInfo.txt"), useBytes = TRUE)

log_msg("DONE. Outputs in: ", OUT_DIR)
cat("\nDONE. Outputs in:\n", OUT_DIR, "\n")
