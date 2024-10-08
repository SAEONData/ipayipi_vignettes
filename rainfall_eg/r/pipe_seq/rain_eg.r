# Load packages
# these are all the packages used for the markdown report
packages <- c("corrplot", "cowplot", "devtools", "DT", "dygraphs", "egg",
  "ggplot2", "Hmisc", "ipayipi", "kableExtra", "khroma", "leaflet",
  "lubridate", "mice", "sf", "viridisLite"
)

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}
# ipayipi is installed from github
devtools::install_github("pauljeco/ipayipi", force = TRUE)

# load package --- for now only ipayipi
library(ipayipi)

# define the working directory ---
# must be reletive to the console/terminal working directory
wd <- "ipayipi_vignettes/rainfall_eg"

pipe_house <- ipip_house(wd)
pipe_house$raw_room <- NULL
logger_data_import_batch(pipe_house)
imbibe_raw_batch(pipe_house, data_setup = ipayipi::hobo_rain,
  verbose = TRUE, record_interval_type = "event_based"
)

header_sts(pipe_house)
phenomena_sts(pipe_house, remove_dups = TRUE)
transfer_sts_files(pipe_house)
append_station_batch(pipe_house, verbose = TRUE)

# list station files in the ipip directory
sflist <- dta_list(
  input_dir = pipe_house$ipip_room, # search directory
  file_ext = ".ipip", # note the station's default file extension
)

# push metadata to sttations
meta_to_station(pipe_house = pipe_house, meta_file = "event_db", verbose = T)

# evaluate gaps
p <- gap_eval_batch(pipe_house)
p <- dta_availability(pipe_house = pipe_house, plot_tbls = "raw_rain")
p$plt
rmarkdown::render(file.path("ipayipi_vignettes/rainfall_eg/reports/markdown/",
    "rainfall_pipe_eg.rmd"
  ), output_dir = "ipayipi_vignettes/rainfall_eg/reports"
)
source('ipayipi_vignettes/rainfall_eg/r/pipe_seq/txs_tb.r')
plan(sequential, split = TRUE)
dt_process_batch(pipe_house = pipe_house, pipe_seq = pipe_seq, verbose = TRUE,
  overwrite_pipe_memory = TRUE
)
dta_flat_pull(pipe_house = pipe_house,
  phen_name = "rain_tot", # name of the phenomena --- exact match
  tab_names = "dt_1_years_agg", # table within which to search for phen
  out_csv = TRUE,
  out_csv_preffix = "mcp"
)

plot_m_anomaly(pipe_house = pipe_house, phen_name = "rain_tot")
dta_flat_pull(pipe_house = pipe_house,
  phen_name = "rain_tot", # name of the phenomena --- exact match
  tab_names = "dt_1_years_agg", # table within which to search for phen
  out_csv = TRUE,
  out_csv_preffix = "mcp"
)

plot_bar_agg(phen_name = "rain_tot", input_dir = pipe_house$ipip_room)

rmarkdown::render(
  input = "ipayipi_vignettes/rainfall_eg/reports/markdown/rainfall_pipe_eg.rmd",
  output_file = file.path("/home/paulg/Documents/projects_current",
    "ipayipi_data_pipe/ipayipi_vignettes/rainfall_eg/reports",
    "rainfall_pipe_eg.html"
  )
)
