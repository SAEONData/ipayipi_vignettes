library(ipayipi)
# Last updated: 2024-12-17

# Station data processing sequence (pipe_seq) for:
# vasi_sci_centre_aws ----
#
# Summary ----
# This is a processing pipeline setup customised for the vasi science centre.
# The processes and concepts here can be adapted and tested on other AWS.
# The first part of the code sets up some calculations that are embedded in
# the following 'pipe build' sequence.
# Before code though, some comments on checks or flags, each flag or check is
# numbered by phenomena (variable) type and sources, where appropriate
# referenced.

# to do ----
# Once these checks have been tested decisions have to be be made on the
# consequences of raising a flag or check warning. In some cases values may be
# deleted, other values and covariates may require inspection.

# object assignments ----
#  define false tip threshold (in seconds) that will be used to remove
# rainfall tips near the time of on site visitation of weather stations
ftip_thresh <- 60 * 35

# flag_cols_5m is parsed to x42 ---- !must be kept updated!
flag_cols_5m <- c("1.1", "1.21", "1.22", "1.4", "1.5", "2.1", "2.2", "2.4",
  "2.5", "2.7", "2.8", "4.1", "4.2", "4.3", "4.5", "4.6", "4.7", "5.1",
  "5.2", "6.2"
)
flag_cols_5m <- paste0("\'flag", flag_cols_5m, collapse = "\',")

# flag_cols_1hr is parsed to x72 --- !must be kept updated!
flag_cols_1hr <- c("0.1", "3.2", "3.6", "4.1", "4.2", "6.2")
flag_cols_1hr <- paste0("\'flag", flag_cols_1hr, collapse = "\',")

# flag_cols_1d is parsed to x102 --- !must be kept updated!
flag_cols_1d <- c("1.3", "1.5", "1.6", "1.7", "2.1", "2.3", "2.6", "4.1", "4.2",
  "5.1", "6.1"
)
flag_cols_1d <- paste0("\'flag", flag_cols_1d, collapse = "\',")

# calc chains ----
## prep logg/gauge interfere tables ----
x12 <- list(
  prep = chainer(j = ".(start_dttm, end_dttm, table_name, logg_interfere)"),
  lg_interfere0 = chainer(
    i = "table_name %chin% \'raw_5_mins\' & logg_interfere %chin% \'on_site\'"
  ),
  dup = chainer(j = c(
    ".(start_dttm = rep(start_dttm, 2), end_dttm = rep(end_dttm, 2),",
    "table_name = rep(table_name), logg_interfere = rep(logg_interfere, 2))"
  )),
  dttm1 = chainer(
    j = "date_time := c(start_dttm[1:(.N / 2)], end_dttm[1:(.N / 2)])"
  ),
  id = chainer(i = "order(date_time)"),
  logg_interfere_type = chainer(
    j = "logg_interfere_type := .SD, .SDcols = \'logg_interfere\'",
    measure = "smp", var_type = "chr", units = "interfere_event"
  ),
  neaten1 = chainer(j = ".(date_time, logg_interfere_type)"),
  neaten2 = chainer(i = "!duplicated(date_time)")
)

## remove false tips ----
x24 <- list(
  false_tip_type = chainer(j = "false_tip_type := logg_interfere_type",
    measure = "smp", units = "false_tip", var_type = "chr"
  ),
  false_tip_mm = chainer(j = "false_tip_mm := rain_tot",
    measure = "tot", units = "mm", var_type = "num"
  ),
  rain_tot = chainer(
    j = "rain_tot := fifelse(is.na(logg_interfere_type), rain_tot, 0)",
    measure = "tot", units = "mm", var_type = "num"
  )
)

## false_tip table (dt_pseudo events) ----
x32 <- list(
  ftiptbl1 = chainer(i = "!is.na(false_tip_type) & false_tip_mm > 0"),
  table_name = chainer(j = "table_name := \'raw_5_mins\'",
    measure = "smp", units = "name", var_type = "str"
  ),
  ftiptbl3 = chainer(
    j = ".(date_time, false_tip_type, table_name, false_tip_mm)"
  )
)

## flags and checks ----
### 5 min data ----
x42 <- list(
  # select columns
  prep = chainer(
    j = c("names(dt)[names(dt) %ilike% ",
      "\'date_time|temp_|rad_solar|rain_tot|humid_rel|wind_\'],",
      " with = FALSE"
    )
  ),
  #### temperature '1' ----
  flag1.1 = chainer( # low variance flag
    j = c("flag1.1 := frollapply(temp_air_avg, 10,",
      " FUN = flag_all_equal, align = \'left\')"
    )
  ),
  flag1.21 = chainer( # variance high extreme - air temp
    j = c("flag1.21 := fifelse(",
      "c(NA, temp_air_avg[2:.N] - temp_air_avg[1:(.N - 1)]) > 5, 1, 0)"
    )
  ),
  flag1.22 = chainer( # variance high extreme - ground temp
    j = c("flag1.22 := fifelse(",
      "c(NA, temp_ground_avg[2:.N] - temp_ground_avg[1:(.N - 1)]) > 5, 1, 0)"
    )
  ),
  flag1.4 = chainer( # variance high extreme
    j = c("flag1.4 := fifelse(abs(",
      "c(temp_air_avg[2:(.N - 1)] - temp_air_avg[1:(.N - 2)], NA, NA)) > 1 &",
      "abs(c(temp_air_avg[3:.N] - temp_air_avg[2:(.N - 1)], NA, NA)) < 0.5",
      ", 1, 0)"
    )
  ),
  check1.5 = chainer(
    j = c("check1.5 := fcase(",
      "temp_air_avg < -25, \'low_warn\',",
      "temp_air_avg < -30, \'extreme_low_warn\',",
      "temp_air_avg > 52, \'high_warn\',",
      "temp_air_avg > 60, \'extreme_high_warn\',",
      "is.na(temp_air_avg), NA, default = \'normal\')"
    )
  ),
  #### humidity '2' ----
  flag2.1 = chainer(
    j = c("flag2.1 := ifelse(humid_rel > 100, 1, 0)")
  ),
  flag2.2 = chainer(
    j = c("flag2.2 := fifelse(",
      "rain_tot > 0.5 & humid_rel < 90 &",
      "c(humid_rel[2:(.N)], NA) < 90 &",
      "c(humid_rel[3:(.N)], NA, NA) < 90,",
      " 1, 0)"
    )
  ),
  check2.41 = chainer(
    j = c("check2.41 := frollapply(humid_rel, 7,",
      " FUN = function(x) flag_all_equal(x, ignore = 100),",
      "align = \'left\')"
    )
  ),
  check2.42 = chainer(
    j = c("check2.42 := frollapply(temp_air_avg, 7,",
      " FUN = function(x) if (isTRUE(sd(x) <= 0.05)) 1 else 0,",
      "align = \'left\')"
    )
  ),
  flag2.4 = chainer(
    j = c("flag2.4 := fifelse(",
      "isTRUE(.SD[[\'check2.41\']] %in% 1) & ",
      "isTRUE(.SD[[\'check2.42\']] %in% 1),",
      " 1, 0)"
    )
  ),
  flag2.5 = chainer(
    j = c("flag2.5 := fifelse(",
      "c(NA, humid_rel[2:.N] - humid_rel[1:(.N - 1)]) > 20, 1, 0)"
    )
  ),
  flag2.7 = chainer(
    j = c("flag2.7 := fifelse(abs(",
      "c(humid_rel[2:(.N - 1)] - humid_rel[1:(.N - 2)], NA, NA)) > 10 & ",
      "abs(c(humid_rel[3:.N] - humid_rel[2:(.N - 1)], NA, NA)) < 5",
      ", 1, 0)"
    )
  ),
  flag2.8 = chainer(
    j = c("flag2.8 := fifelse(abs(",
      "c(humid_rel[2:(.N - 1)] - humid_rel[1:(.N - 2)], NA, NA)) > 15 & ",
      "abs(c(humid_rel[3:.N] - humid_rel[2:(.N - 1)], NA, NA)) < 5",
      ", 1, 0)"
    )
  ),
  #### wind '4' ----
  flag4.1 = chainer(
    j = "flag4.1 := fifelse(wind_speed > 50 | wind_speed < 0, 1, 0)"
  ),
  flag4.2 = chainer(
    j = "flag4.2 := fifelse(wind_dir > 360 | wind_dir < 0, 1, 0)"
  ),
  flag4.3 = chainer(
    j = c("flag4.3 := frollapply(wind_dir, 288, FUN = ",
      "function(x) sd(x) < 5, align = \'left\')"
    )
  ),
  flag4.4 = chainer(
    j = c("flag4.4 := frollapply(wind_dir, 40, FUN = ",
      "flag_all_equal, align = \'left\')"
    )
  ),
  flag4.5 = chainer(
    j = c("flag4.5 := frollapply(wind_dir, 288, FUN = ",
      "flag4.5, align = \'left\')"
    )
  ),
  flag4.6 = chainer(
    j = "flag4.6 := fifelse(wind_speed > 32.7 & wind_speed < 120, 1, 0)"
  ),
  flag4.7 = chainer(
    j = c("flag4.7 := frollapply(wind_speed, 288, FUN = ",
      "flag4.7, align = \'left\')"
    )
  ),
  #### radiation '5' ----
  flag5.1 = chainer(
    j = "flag5.1 := fifelse(rad_solar_avg > 2221 | rad_solar_avg < -20, 1, 0)"
  ),
  flag5.2 = chainer(
    j = c("flag5.2 := fifelse(",
      "c(NA, rad_solar_avg[2:.N] - rad_solar_avg[1:(.N - 1)]) > 1000, 1, 0)"
    )
  ),
  #### precipitation '6' ----
  flag6.2 = chainer(j = "flag6.2 := fifelse(rain_tot > 12.8, 1, 0)"),
  #### summarise and total checks and flags ----
  checkcalc1 = chainer(
    j = "flag1.5 := fifelse(.SD[[\'check1.5\']] != \'normal\', 1, 0)"
  ),
  flagsfilter1 = chainer(
    j = c(
      "flag_tot := rowSums(.SD, na.rm = TRUE), ",
      ".SDcols = c(", flag_cols_5m, "\')"
    )
  )
)

### 1 hours data ----
# dt_calc: generate false tip tablefor hourly data
x52 <- list(
  table_name = chainer(j = "table_name := \'raw_1_hours\'",
    measure = "smp", units = "name", var_type = "str"
  ),
  ftip2 = chainer(j = "dttm := lubridate::floor_date(date_time, \'1 hours\')"),
  false_tip_mm = chainer(
    j = "false_tip_mm := sum(false_tip_mm, na.rm = TRUE), by = \'dttm\'",
    measure = "tot", units = "mm", var_type = "num"
  ),
  ftip4 = chainer(j = "date_time := dttm"),
  ftip5 = chainer(i = "!duplicated(date_time)")
)
# dt_calc: remove false tips from hourly data
x64 <- list(
  rain_tot = chainer(j = c("rain_tot := fifelse(!is.na(false_tip_type),",
      " rain_tot - false_tip_mm, rain_tot)"
    ), measure = "tot", units = "mm", var_type = "num"
  )
)


# dt_calc: hourly flags ----
x72 <- list(
  # select columns
  prep = chainer(
    j = c("names(dt)[",
      "names(dt) %ilike% \'date_time|batt_|humid_|pressure_|rad_sol|",
      "rain_tot|temp_|wind_\'], with = FALSE"
    )
  ),
  #### battery '0' ----
  check0.1 = chainer(
    j = c("check0.1 := fcase(batt_min > 13.6, \'high_warn\',",
      "batt_min >= 11 & batt_min < 12.5, \'med_warn\', ",
      "batt_min >= 9.6 & batt_min < 11, \'med_low_warn\', ",
      "batt_min < 9.6, \'low_err\', is.na(batt_min), NA, default = \'normal\')"
    )
  ),
  #### pressure '3' ----
  flag3.2 = chainer(
    j = c("flag3.2 := frollapply(pressure_atm, 24, FUN = ",
      "flag_all_equal, align = \'left\')"
    )
  ),
  flag3.6 = chainer(
    j = c("flag3.6 := fifelse(pressure_atm > 1050 | ",
      "pressure_atm < 800, 1, 0)"
    )
  ),
  #### wind '4' ----
  flag4.1 = chainer(
    j = c(
      "flag4.1 := fifelse(wind_speed > 50 | wind_speed < 0, 1, 0)"
    )
  ), flag4.2 = chainer(
    j = "flag4.2 := fifelse(wind_dir > 360 | wind_dir < 0, 1, 0)"
  ),
  #### precipitation '6' ----
  flag6.2 = chainer(
    j = "flag6.2 := fifelse(rain_tot > 22.9, 1, 0)"
  ), check_calcs = chainer(
    j = "flag0.1 := fifelse(.SD[[\'check0.1\']] != \'normal\', 1, 0)"
  ),
  #### summarise and total checks and flags ----
  flagsfilter1 = chainer(
    j = c(
      "flag_tot := rowSums(.SD, na.rm = TRUE), ",
      ".SDcols = c(", flag_cols_1hr, "\')"
    )
  )
)


### 1 days data ----
#### false tip table gen ----
x82 <- list(
  table_name = chainer(j = "table_name := \'raw_1_days\'",
    measure = "smp", units = "name", var_type = "str"
  ),
  ftip2 = chainer(j = "dttm := lubridate::floor_date(date_time, \'1 days\')"),
  false_tip_mm = chainer(
    j = "false_tip_mm := sum(false_tip_mm, na.rm = TRUE), by = \'dttm\'",
    measure = "tot", units = "mm", var_type = "num"
  ),
  ftip4 = chainer(j = "date_time := dttm"),
  ftip5 = chainer(i = "!duplicated(date_time)")
)

# dt_calc: remove false tips from daily data
x94 <- list(
  rain_tot = chainer(j = c("rain_tot := fifelse(!is.na(false_tip_type),",
      " rain_tot - false_tip_mm, rain_tot)"
    ), measure = "tot", units = "mm", var_type = "num"
  )
)

#### checks and flags ----
x102 <- list(
  # select columns
  prep = chainer(
    j = c("names(dt)[",
      "names(dt) %ilike% \'date_time|temp_|rad_|rain_tot|humid_rel|wind_\'],",
      " with = FALSE"
    )
  ),
  #### temperature '1' ----
  flag1.3 = chainer(
    j = "flag1.3 := fifelse(abs(temp_air_max - temp_air_min) < 0.5, 1, 0)"
  ),
  check1.5 = chainer(
    j = c("check1.5 := fcase(",
      "temp_air_min < -30, \'extreme_low_warn\',",
      "temp_air_min < -25, \'low_warn\',",
      "temp_air_max > 52, \'high_warn\',",
      "temp_air_max > 60, \'extreme_high_warn\',",
      "temp_ground_avg > 52, \'high_warn\',",
      "temp_ground_avg > 60, \'extreme_high_warn\',",
      "temp_ground_avg < -25, \'low_warn\',",
      "temp_ground_avg < -30, \'extreme_low_warn\',",
      "temp_ground_min < -25, \'low_warn\',",
      "temp_ground_min < -30, \'extreme_low_warn\',",
      "is.na(temp_air_min) | is.na(temp_air_max) |",
      "is.na(temp_ground_avg) | is.na(temp_ground_min), NA",
      "default = \'normal\')"
    )
  ),
  flag1.6 = chainer(
    j = "flag1.6 := fifelse(temp_air_max < temp_air_min, 1, 0)"
  ),
  flag1.7 = chainer(
    j = "flag1.7 := fifelse(temp_ground_avg > temp_air_max, 1, 0)"
  ),
  #### humidity '2' ----
  flag2.1 = chainer(
    j = c("flag2.1 := fcase(humid_rel_min > 100, 1, ",
      "humid_rel_max > 100, 1, is.na(humid_rel_min) | is.na(humid_rel_max),",
      " NA, default = 0)"
    )
  ),
  flag2.3 = chainer(
    j = "flag2.3 := fifelse(humid_rel_min > humid_rel_max, 1, 0)"
  ),
  flag2.6 = chainer(
    j = "flag2.6 := fifelse(abs(humid_rel_min - humid_rel_max) < 5, 1, 0)"
  ),
  #### wind '4' ----
  flag4.1 = chainer(
    j = c("flag4.1 := fifelse(wind_speed > 50 | ",
      "wind_speed_max > 50, 1, 0)"
    )
  ),
  flag4.2 = chainer(
    j = "flag4.2 := fifelse(wind_dir > 360 | wind_dir < 0, 1, 0)"
  ),
  #### radiation '5' ----
  flag5.1 = chainer(
    j = c("flag5.1 := fifelse(",
      "rad_solar_max > 2221 | rad_solar_max < -20, 1, 0)"
    )
  ),
  #### precipitation '6' ----
  flag6.1 = chainer(j = "flag6.1 := fifelse(rain_tot > 500, 1, 0)"),
  #### summarise checks and flags ----
  check_sum = chainer(
    j = "flag1.5 := fifelse(.SD[[\'check1.5\']] != \'normal\', 1, 0)"
  ),
  flagsfilter1 = chainer(
    j = c(
      "flag_tot := rowSums(.SD, na.rm = TRUE), ",
      ".SDcols = c(", flag_cols_1d, "\')"
    )
  )
)

# put pipeline steps together (also using described calculations above)
# build pipe sequence ----

# to do ...
# extract 5 min interfere events
# summarise and join to 5 min data
# generate false tip table and 'new' rainfall false-tip tag
pipe_seq <- pipe_seq(p = pdt(
  # rework the logg_interfere table to prepare to remove false tips
  p_step(dt_n = 1, dtp_n = 1,
    f_params = hsf_param_eval(hsf_table = "data_summary"),
    output_dt = "dt_lg_interfere", time_interval = "discnt"
  ),
  p_step(dt_n = 1, dtp_n = 2,
    f_params = calc_param_eval(x12),
    output_dt = "dt_lg_interfere", time_interval = "discnt"
  ),
  # prep 5 min data ----
  p_step(dt_n = 2, dtp_n = 1,
    f_params = hsf_param_eval(hsf_table = "raw_5_mins"),
    output_dt = "dt_5_mins_agg", time_interval = "5 mins"
  ),
  p_step(dt_n = 2, dtp_n = 2,
    f_params = hsf_param_eval(hsf_table = "dt_lg_interfere"),
    output_dt = "dt_5_mins_agg", time_interval = "5 mins"
  ),
  p_step(dt_n = 2, dtp_n = 3,
    f_params = join_param_eval(join = "left_join", fuzzy = c(0, ftip_thresh)),
    output_dt = "dt_5_mins_agg", time_interval = "5 mins"
  ),
  # remove false tips
  p_step(dt_n = 2, dtp_n = 4, f_params = calc_param_eval(x24),
    output_dt = "dt_5_mins_agg", time_interval = "5 mins"
  ),
  # generate pseudo event (false-tip table) from 5 min data
  p_step(dt_n = 3, dtp_n = 1,
    f_params = hsf_param_eval(hsf_table = "dt_5_mins_agg"),
    output_dt = "dt_pseudo_events", time_interval = "discnt"
  ),
  p_step(dt_n = 3, dtp_n = 2, f_params = calc_param_eval(x32),
    output_dt = "dt_pseudo_events", time_interval = "discnt"
  ),
  # checks and flags: 5 min data ----
  p_step(dt_n = 4, dtp_n = 1,
    f_params = hsf_param_eval(hsf_table = "dt_5_mins_agg"),
    output_dt = "dt_flag_raw_5_mins", time_interval = "5 mins"
  ),
  p_step(dt_n = 4, dtp_n = 2, f_params = calc_param_eval(x42),
    output_dt = "dt_flag_raw_5_mins", time_interval = "5 mins"
  ),
  # 1 hours data ----
  ## false-tip: make hourly table ----
  p_step(dt_n = 5, dtp_n = 1,
    f_params = hsf_param_eval(hsf_table = "dt_pseudo_events"),
    output_dt = "dt_pseudo_events_tmp", time_interval = "discnt"
  ),
  p_step(dt_n = 5, dtp_n = 2, f_params = calc_param_eval(x52),
    output_dt = "dt_pseudo_events_tmp", time_interval = "discnt"
  ),
  # get hourly data & remove false tips
  p_step(dt_n = 6, dtp_n = 1,
    f_params = hsf_param_eval(hsf_table = "raw_1_hours"),
    output_dt = "dt_1_hours_agg", time_interval = "1 hours"
  ),
  p_step(dt_n = 6, dtp_n = 2,
    f_params = hsf_param_eval(hsf_table = "dt_pseudo_events_tmp"),
    output_dt = "dt_1_hours_agg", time_interval = "1 hours"
  ),
  p_step(dt_n = 6, dtp_n = 3,
    f_params = join_param_eval(join = "left_join", fuzzy = c(0, ftip_thresh)),
    output_dt = "dt_1_hours_agg", time_interval = "1 hours"
  ),
  # remove false tip mm from rain tot in hourly data
  p_step(dt_n = 6, dtp_n = 4, f_params = calc_param_eval(x64),
    output_dt = "dt_1_hours_agg", time_interval = "1 hours"
  ),
  # checks and flags: hourly data ----
  p_step(dt_n = 7, dtp_n = 1,
    f_params = hsf_param_eval(hsf_table = "dt_1_hours_agg"),
    output_dt = "dt_flag_raw_1_hours", time_interval = "1 hours"
  ),
  # checks and flags in hourly data
  p_step(dt_n = 7, dtp_n = 2, f_params = calc_param_eval(x72),
    output_dt = "dt_flag_raw_1_hours", time_interval = "1 hours"
  ),
  # false-tip table: daily data ----
  p_step(dt_n = 8, dtp_n = 1,
    f_params = hsf_param_eval(hsf_table = "dt_pseudo_events_tmp"),
    output_dt = "dt_pseudo_events_tmp", time_interval = "discnt"
  ),
  p_step(dt_n = 8, dtp_n = 2, f_params = calc_param_eval(x82),
    output_dt = "dt_pseudo_events_tmp", time_interval = "discnt"
  ),
  # remove false tips ----
  p_step(dt_n = 9, dtp_n = 1,
    f_params = hsf_param_eval(hsf_table = "raw_1_days"),
    output_dt = "dt_1_days_agg", time_interval = "1 days"
  ),
  p_step(dt_n = 9, dtp_n = 2,
    f_params = hsf_param_eval(hsf_table = "dt_pseudo_events_tmp"),
    output_dt = "dt_1_days_agg", time_interval = "1 days"
  ),
  p_step(dt_n = 9, dtp_n = 3,
    f_params = join_param_eval(join = "left_join", fuzzy = c(0, ftip_thresh)),
    output_dt = "dt_1_days_agg", time_interval = "1 days"
  ),
  # remove false tip mm from rain tot in daily data
  p_step(dt_n = 9, dtp_n = 4, f_params = calc_param_eval(x94),
    output_dt = "dt_1_days_agg", time_interval = "1 days"
  ),
  # checks and flags ----
  p_step(dt_n = 10, dtp_n = 1,
    f_params = hsf_param_eval(hsf_table = "dt_1_days_agg"),
    output_dt = "dt_flag_raw_1_days", time_interval = "1 days"
  ),
  p_step(dt_n = 10, dtp_n = 2, f_params = calc_param_eval(x102),
    output_dt = "dt_flag_raw_1_days", time_interval = "1 days"
  )
))

# References ----
# Alani O. E., H. Ghennioui, A. Ghennioui, Y.-M. Saint-Drenan, P. Blanc, N.
#  Hanrieder, F.-E. Dahr. 2021. A visual support of standard procedures for
#  solar radiation quality control. International Journal of Renewable Energy
#  Development 10(3): 401--414.
# Bian, Z., W. Chong,, L. Ding, and W. Yang. 2018. Analysis and research on
#  quality control method of global radiation observation data. 7th
#  International Symposium on Test Automation and Instrumentation. The Journal
#  of Engineering, 23:8975--8979.
# Kruger, A. C., J. V. Retief, and A. M. Goliger. 2013. Strong winds in South
#  Africa: Part 2. Mapping of updated statistics. Journal of the South
#  African Institution of Civil Engineering, 55(2):46--58.
# SAWS. 2024. Climate Data Quality Control Course. Course notes. Technical
#  Report SAWS, Pretoria, ZA.
# Vermeulen, J. H., D. W. Hedding, and N. Letsatsi. 2021. Forecasting extreme
#  hourly rainfall in South Africa for disaster risk reduction: thresholds
#  and return periods. Water SA, 50(4): 330--344.