library(ipayipi)
options(chunk_dir = "~/ipip")
getOption("chunk_dir")

wd <- "ipayipi_vignettes/rainfall_eg"
pipe_house <- ipip_house(wd)

wanted <- "office"
source('ipayipi_vignettes/rainfall_eg/r/pipe_seq/txs_tb.r')
station_file <- "ipayipi_vignettes/rainfall_eg/ipip_room/mcp_manz_office_rn.ipip"
pipe_seq = pipe_seq
output_dt_preffix = "dt_"
output_dt_suffix = NULL
overwrite_pipe_memory = F
verbose = TRUE
unwanted_tbls = "_tmp"
xtra_v <- TRUE
keep_open <- TRUE
stages = NULL

### dt_calc args ----
station_file_ext <- ".ipip"


# plot_bar_agg
input_dir = "."
agg = "1 month"
phen_name = "rain_tot"
tbl_search_key = "dt_1_months_agg"
show_gaps = FALSE
wanted = NULL
unwanted = NULL
x_lab = "Date-time"
y_lab = NULL
file_ext = ".ipip"
prompt = FALSE
recurr = TRUE
pipe_house = pipe_house

