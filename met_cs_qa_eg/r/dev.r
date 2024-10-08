# ## build and load packages ---
detach("package:ipayipi", unload = TRUE)
source("r_misc/ipayipi_build_install.r")
library(ipayipi)
options(chunk_dir = "~/ipip")
getOption("chunk_dir")
#'
## pipeline structure ----
wd <- "ipayipi_vignettes/met_cs_qa_eg"
sr <- file.path("ipayipi_vignettes/met_cs_qa_eg/dta_in")
pipe_house <- ipip_house(wd, source_room = sr)
pipe_house$raw_room <- NULL

logger_data_import_batch(pipe_house)
imbibe_raw_batch(pipe_house, data_setup = ipayipi::cs_tod)
header_sts(pipe_house)
phenomena_sts(pipe_house, external_phentab = ipayipi::phens_sts)
transfer_sts_files(pipe_house)

# append standardised data files ----
append_station_batch(pipe_house, verbose = TRUE)

# evaluate gaps in raw data tables ---
gap_eval_batch(pipe_house, verbose = TRUE, phen_eval = TRUE)
p <- dta_availability(pipe_house = pipe_house)
p$plt

# process data ----
source("ipayipi_vignettes/met_cs_qa_eg/r/pipe_seq/vasi_pipe_seq_eg.r")
dt_process_batch(pipe_house = pipe_house, pipe_seq = pipe_seq,
  overwrite_pipe_memory = TRUE, verbose = TRUE
)

s <- readRDS(
  "ipayipi_vignettes/met_cs_qa_eg/ipip_room/mcp_vasi_science_aws.ipip"
)

# imbibe_raw_batch ----
wipe_source = FALSE
file_ext_in = NULL
file_ext_out = ".ipr"
col_dlm = NULL
dt_format = c(
    "Ymd HMOS", "Ymd HMS",
    "Ymd IMOSp", "Ymd IMSp",
    "ymd HMOS", "ymd HMS",
    "ymd IMOSp", "ymd IMSp",
    "mdY HMOS", "mdY HMS",
    "mdy HMOS",  "mdy HMS",
    "mdy IMOSp",  "mdy IMSp",
    "dmY HMOS", "dmY HMS",
    "dmy IMOSp", "dmy IMSp"
)
dt_tz = "Africa/Johannesburg"
record_interval_type = "continuous"
remove_prompt = FALSE
max_rows = 1000
logg_interfere_type = "on_site"
data_setup = ipayipi::cs_tod
prompt = FALSElong
prompt = FALSE
recurr = TRUE
wanted = NULL
unwanted = NULL
file_ext_in = ".ipr"
file_ext_out = ".iph"
verbose = TRUE
## nomtab_chk ----

# standardise phens with saeon phentab
pipe_house = pipe_house
remove_dups = FALSE
external_phentab = ipayipi::phens_sts
prompt = FALSE
recurr = TRUE
wanted = NULL
unwanted = NULL
file_ext_in = ".iph"
file_ext_out = ".ipi"
verbose = TRUE
ipayipi::phens_sts[phen_name %ilike% "lat"]


# dta_availability ----
input_dir = "."
pipe_house = pipe_house
phen_names = NULL
station_ext = ".ipip"
gap_problem_thresh_s = 6 * 60 * 60
event_thresh_s = 10 * 60
start_dttm = NULL
end_dttm = NULL
tbl_names = NULL
meta_events = "meta_events"
verbose = TRUE
wanted = NULL
unwanted = NULL
recurr = TRUE
prompt = FALSE
keep_open = TRUE
xtra_v = FALSE