# flagging met data example ----

# Description
# this script is an example that can be used to build processing pipelines
# for met data obtained from the 'loggernet' or terrestrial observation
# dashboard, specifically this example serves as an intro for testing
# threshold values and functions for quality checking associated data
#
# Data for this exercise should be downloaded from the dashboard.
# Importantly we rely on the data formatted as per the example data downloaded
# here: https://github.com/SAEONData/ipayipi_vignettes/blob/main_vig/met_cs_qa_eg/dta_in/5%20minute_Vasi%20Science%20Centre%20AWS_data.csv
# this data is 5_min time step/interal data

# set up pipeline ----
## build and load ipayipi ---

## install ipayipi in r ----
# fisrt detach ipayipi if already loaded
detach("package:ipayipi", unload = TRUE)

## installation options (option two is easier)
# 1. download from git installation tar.gz file and install using this command
# argument provided is the relative path to the tar.gz file
install.packages("./ipayipi_0.0.4.tar.gz")

# 2. this is probably the easiest option using the package 'devtools'
devtools::install_github("SAEONdata/ipayipi", force = TRUE)

# now load the package
library(ipayipi)

## setup pipeline structure ----
# define the working directory. This is the directory in which subfolders will
# be generated and through which data will flow/be processed
wd <- "ipayipi_vignettes/met_cs_qa_eg" # relative working directory

# run ipip_house to generate the folder structure for the pipeline functioning
pipe_house <- ipip_house(wd)
pipe_house$raw_room <- NULL # setting this directory to NULL to avoid archival
# of raw data in the example folder.

# print out the result, i.e., a list of directories
#  - this object will be 'fed' to other functions, as the pipeline is setup
#    to operate in a standardised way. Also, putting these in one list saves
#    having to feed each seperate directory to a function.
print(pipe_house)

# getting 'raw' data
# once this is done paste input data from the terrestrial observation dashboard
# into the pipe lines 'source_room' folder.

## get data into ipayipi ----
# basically copies data from the 'source room'
logger_data_import_batch(pipe_house)

# reads data into R format
#  - sorts out time intervals + organise header, phen data
imbibe_raw_batch(pipe_house, data_setup = ipayipi::cs_tod,
  verbose = TRUE
)

## standardise data ----
# standardise header data
# - run the below function. if new synonyms are introduced there is an
#   interactive process to resolve this described in the vignette below:
# https://github.com/SAEONData/ipayipi_vignettes/blob/main_vig/rainfall_eg/reports/rainfall_pipe_eg.html
# follow the option to download the raw html file above

# this function will run the header standardisation
header_sts(pipe_house)

# in a similar manner this function standardises phenomena
phenomena_sts(pipe_house, external_phentab = ipayipi::phens_sts)

# this function saves standardised files in the 'nomvet_room' directory
# this is where all standardised files are stored
transfer_sts_files(pipe_house)

## append standardised data files ----
# the append function evaluates the 'nomvet room' and appends
# standardised files there in to create/update station files
append_station_batch(pipe_house, verbose = TRUE)
# note that in the background ipayipi 'chunks' the station file
# in temporary memory giving it the ability to minimise
# memory usage and setup quick queries
# Each time the station file is opened in another function below
# the background work is done in the chunked files and saved to
# the station file you see in the ipip_room directory.
# there are ways to optimise how this is done that can be discussed
# sometime i.e., setting the chunk directory to a permanent one


## evaluate gaps in raw data tables ---
# generate a 'gap' table - can be used for further processing
gap_eval_batch(pipe_house, verbose = TRUE, phen_eval = TRUE)

# basic plot of gaps
p <- dta_availability(pipe_house = pipe_house)
p$plt

# process data ----
# first run the script with the 'pipe sequence'
source("ipayipi_vignettes/met_cs_qa_eg/r/pipe_seq/vasi_pipe_seq_eg.r")
# then embed the pipe sequence, evaluate it and develop standards for
#  processing, and process data
dt_process_batch(pipe_house = pipe_house, pipe_seq = pipe_seq,
  overwrite_pipe_memory = TRUE, verbose = TRUE
)
# this with the aggregations on my desktop takes just under a minute to
# process these just under half a mil rows

# read a station into memory
s <- readRDS(
  "ipayipi_vignettes/met_cs_qa_eg/ipip_room/mcp_vasi_science_aws.ipip"
)

# view names of 'items' in station file (a station file is an R list)
names(s)

# query station data
# see the help files for more options here this can be used to
# convert queries to csv format
dta_flat_pull(pipe_house = pipe_house,
  tab_names = "dt_1_hours_agg", # vector of table names
  phen_name = "rain_tot" # phenomena name being queried
)

# alternatively, if you don't only want to view station tables in r
# you can export them to csv format using `ipayipi::ipayipi2csv()`
ipayipi2csv(pipe_house = pipe_house, wanted_tabs = "dt_",
  output_dir = pipe_house$dta_out
)
