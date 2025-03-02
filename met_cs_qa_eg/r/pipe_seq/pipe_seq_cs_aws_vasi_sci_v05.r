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

# Flag/check rule summaries ----
# Note that a '+' sign, opposed to a '-' sign indicates that the respective
# check/flag has been implenented. Flags are organised per 'general' phenomena,
# but in some cases there is overlap across related phenomena types. '-' flags
# are retained as a note of their availability.
#
## rule 0: battery ----
#  + 0.1: 'high_warn': voltage not greater than 13.6 volts.
#         [saws warning: 2.3.9.1]
#       + 'med_warn': voltage is greater or equal to 11, but less than 12.5
#         volts.
#         [saws warning: 2.3.9.2]
#       + 'med_low_warn': voltage is greater or equal to 9.6, but less than
#         11.0 volts.
#         [saws warning: delete pressure values; warning for other parameters.
#         2.3.9.3]
#       + 'low_err': voltage less than 9.6 volts.
#         (all time steps)
#         [saws delete all: 2.3.8.4]
## rule 1: temperature ----
#  + 1.1: Error if consecutive values in window (n=10) are identical
#         (5 min time step).
#         [saws 2.3.1.3 used n=20]
#         [gfw testing n=10 for 60 min stability]
#  + 1.2: Error if difference in temperature between two consecutive values
#         exceeds 5.0 deg C.
#         *1.21: Run for temp_air_avg
#         *1.22: Run for temp_ground_avg
#         (5 min time step)
#         [saws 2.3.2.3]
#  + 1.3: Error if the difference between the max and min temperature is less
#         than 0.5 deg C.
#         (daily time step)
#         [saws 2.3.3.3]
#         [M. Toucher suggested deleting values that don't adhere to this]
#  + 1.4: Spike filter window of length 3.
#         Error if the difference between values 1 & 2 is greater than 1.0
#         deg C, and the difference between values 2 & 3 less than 0.5 deg C.
#         (5 min time step)
#         [saws 2.3.4.3]
#  + 1.5: Check warning if temp (air and ground) greater than 52 or less than
#         -25.0 deg C. Delete values greater than 60.0 or less than -30.0 deg C.
#         (all time steps)
#         [M. Toucher suggested limits of lower flag warning between -25 to -30,
#          and delete values less than -30. And upper limit of flag warning
#          between 52 and 60, and delete values above 60]
#         [saws 2.3.5.3, saws limits, lower: -20.0, upper: 45.0]
#  + 1.6: Error if max less than min
#         (daily step)
#         [M. Toucher suggested that this is a relic of the days of manual
#          estimation of max and min values. And that if these arise they
#          should be deleted]
#  + 1.7: ground temp not greater than max air temp
#         (daily step)
#         ?
## rule 2: humidity ----
#  + 2.1: all humidity values not greater than 100%
#         (all time steps)
#         [M. Toucher noted that these values should not be deleted as they
#          are an indicator of sensor drift. Therefore, flag values as
#          something to be inspected.]
#         [c.f. saws 2.3.5.1]
#  + 2.2: Error if more than 0.5 mm rainfall within the current (5 mins) and
#         there are no humidity values exceeding 90% in the current,
#         and next two time steps.
#         [M. Toucher: this would be a flag tht needs checking --- cross
#          validation necessary]
#         [gfw testing]
#  + 2.3: max not less than min
#         (daily)
#         [M. Toucher suggested that this is a relic of the days of manual
#          estimation of max and min values. And that if these arise they
#          should be deleted]
#  + 2.4: Error if humidity is stable but temperature is unstable. Stabiliy
#         in humidity values in window (n=7) checked if identical; variation
#         in temperature evaluted using its standard deviation in the same
#         time window. If standard deviation in air temperature is > 0.05 deg C,
#         air temperature is considered too 'unstable' for constant humidity.
#         Implemented with check2.41 and check2.42, then flagged with flag2.4.
#         (5 min time step)
#         [saws 2.3.1.1; saws used window of size n = 80 and tested for
#          identical values only]
#         [gfw testing n = 7; testing ignore 100]
#  + 2.5: Error if difference between two consecutive values exceeds 20%
#         (5 min time step)
#         [saws 2.3.2.1]
#         [M. Toucher suggested deletion of these error flag values]
#  + 2.6: Error if difference between max and min less than 5%
#         (daily step)
#         [saws 2.3.3.1]
#         [M. Toucher suggests not implementing this flag. This error may result
#          misty/drizzle days].
#  + 2.7: Spike filter window of length 3. Error if difference between
#         values 1 & 2 is greater than 10% and 2 & 3 less than 5%
#         (5 min (or highest res) time step)
#         [saws 2.3.4.1]
#  + 2.8: More conservative spike filter version of 2.7 above.
#         Error if difference between values 1 & 2 is greater than 10% and 2 & 3
#         less than 5%.
#         (5 min (or highest res) time step)
#         [saws 2.3.4.1]
## rule 3: pressure ----
#  - 3.1: Consecutive values in window (n=40) non-identical
#         (5 min time step)
#         [saws 2.3.1.2]
#  + 3.2: Consecutive values in window (n=24) non-identical
#         (hourly time step)
#         [c.f. saws 2.3.1.2 - implemented for when time res not 5 min]
#  - 3.3: Sifference between two consecutive values exceeds 2.0 hPa
#         (5 min time step)
#         [saws 2.3.2.2]
#  - 3.4: Difference between max and min less than 1.0 hPa (daily step)
#         [saws 2.3.3.2]
#         * not implemented *
#  - 3.5: Spike filter window length 3. Difference between 1 & 2 is greater than
#         1.0 hPa and 2 & 3 less than 0.5 hPa.
#         (5 min time step)
#         [saws 2.3.4.2]
#  + 3.6: Values less than 800.0 or greater than 1050.0 hPa
#         (all time steps)
#         [saws 2.3.5.2]
## rule 4: wind ----
#  + 4.1: Wind speed not less than 0 or greater than 50 m/s
#         (all time steps)
#         [M. Toucher suggests that values over 50 m/s should be flagged and
#          following the gust max observed by Kruger et al (2013) on SA wind
#          speeds. Kruger et al. (2013) showed that the 1:50 year max wind
#          speed has not exceeded 50 m/s.]
#         [saws 2.3.8.2: wind gust limit test
#          (saws use upper limit of 120 m per sec)]
#  + 4.2: wind direction not less than 0 or greater than 360 (all time steps)
#         [saws 2.3.8.1: limit test]
#  + 4.3: Error if variation (standard deviation) in wind direction less than
#         or equal to 5 deg over a 24 hour period. Low variation indicates a
#         stuck vane.
#         (5 min time step)
#         [saws 2.3.8.1: small range test]
#  + 4.4: Error flag if 40 identical wind direction readings.
#         (5 min (or highest res) time step)
#         [saws 2.3.8.1: identical tests]
#  + 4.5: Daily max and min range of wind speed must be greater or equal to
#         3.0 m per sec.
#         (5 min time step)
#         [saws 2.3.8.2: small range test]
#  + 4.6: Wind max values greater than 32.7 m per sec and less than 120
#         m per sec are flagged as a warning for technicians to evaluate.
#         (5 min time step)
#         [saws 2.3.8.2: wind gust limit warning]
#  + 4.7: 'Still' day: Error if daily average wind speed is less than
#         20.0 m per sec and the difference between the average and max is
#         greater than 20.0 m per sec.
#         'Gusty' day: Error if daily average wind speed is greater than
#         20.0 m per sec and the difference between the average and max is
#         greater than 25.0 m per sec.
#         (daily time step --- implemented as rolling function in 5 min data)
#         [saws 2.3.8.2: wind gust/average test]
## rule 5: solar radiation ----
#  + 5.1: Error flagged if solar radiation exceeds 2221 or goes below -20 w/m2.
#         (all time steps)
#         [M. Toucher suggestion c.f. (Alani et al. 2021 in IJRED; Bian et al
#         2019 in IET)]
#  + 5.2: Rate of change between 2 consecutive values does not exceed 1000 w/m2.
#         (5 min time step)
#         [M. Toucher suggestion c.f. (Alani et al. 2021 in IJRED; Bian et al
#         2019 in IET)]
## rule 6: precipitation ----
#  + 6.1: rainfall does not exceed 500 mm
#         (daily)
#         [M. Toucher suggests delete rainfall values that exceed this limit]
#  + 6.2: rainfall does exceed 12.8 mm in 5 mins
#         (5 min time step)
#         rainfall does exceed 22.9 mm in 60 mins
#         (hourly time step)
#         [Vermeulen et al. 2021 in Water SA noted 99.9 percentile thresholds
#          for: 5 min at 12.8 mm; for hourly at 22.9 mm]
#         [~saws 2.3.7]

# below are some calculations that are embedded in the pipeline as
# recommended by Matt Dowie (data.table) --- parsing text for eval.
# checks are only run on new data when running pipeline in sequential mode

# predefined objects ----
# Define column names that will be used to get flag totals.
# This must be kept up to date, that is, reflect the actual flags that have been
# implemented.

# flag_cols_5m is parsed to x12 --- !must be kept updated!
flag_cols_5m <- c("1.1", "1.21", "1.22", "1.4", "1.5", "2.1", "2.2", "2.4",
  "2.5", "2.7", "4.1", "4.2", "4.3", "4.5", "4.6", "4.7", "5.1",
  "5.2", "6.2"
)
flag_cols_5m <- paste0("\'flag", flag_cols_5m, collapse = "\',")

# flag_cols_1hr is parsed to x22 --- !must be kept updated!
flag_cols_1hr <- c("0.1", "3.2", "3.6", "6.2")
flag_cols_1hr <- paste0("\'flag", flag_cols_1hr, collapse = "\',")

# flag_cols_1d is parsed to x32 --- !must be kept updated!
flag_cols_1d <- c("1.3", "1.5", "1.6", "1.7", "2.1", "2.3", "2.6", "4.1", "4.2",
  "5.1", "6.1"
)
flag_cols_1d <- paste0("\'flag", flag_cols_1d, collapse = "\',")

# dt_calc: raw 5 min flags ----
x12 <- list(
  prep = chainer( # filter phens
    dt_syn_ac = paste0("names(dt)[names(dt) %ilike% ",
      "\'date_time|temp_|rad_solar|rain_tot|humid_rel|wind_\'],",
      " with = FALSE"
    )
  ), flag1.1 = chainer( # temperature '1'
    dt_syn_ac = paste0("flag1.1 := frollapply(temp_air_avg, 10,",
      " FUN = flag_all_equal, align = \'left\')"
    )
  ), flag1.21 = chainer(
    dt_syn_ac = paste0("flag1.21 := fifelse(",
      "c(NA, temp_air_avg[2:.N] - temp_air_avg[1:(.N - 1)]) > 5, 1, 0)"
    )
  ), flag1.22 = chainer(
    dt_syn_ac = paste0("flag1.22 := fifelse(",
      "c(NA, temp_ground_avg[2:.N] - temp_ground_avg[1:(.N - 1)]) > 5, 1, 0)"
    )
  ), flag1.4 = chainer(
    dt_syn_ac = paste0("flag1.4 := fifelse(abs(",
      "c(temp_air_avg[2:(.N - 1)] - temp_air_avg[1:(.N - 2)], NA, NA)) > 1 &",
      "abs(c(temp_air_avg[3:.N] - temp_air_avg[2:(.N - 1)], NA, NA)) < 0.5",
      ", 1, 0)"
    )
  ), check1.5 = chainer(
    dt_syn_ac = paste0("check1.5 := fcase(",
      "temp_air_avg < -25, \'low_warn\',",
      "temp_air_avg < -30, \'extreme_low_warn\',",
      "temp_air_avg > 52, \'high_warn\',",
      "temp_air_avg > 60, \'extreme_high_warn\',",
      "is.na(temp_air_avg), NA, default = \'normal\')"
    )
  ), flag2.1 = chainer( # humidity '2'
    dt_syn_ac = paste0("flag2.1 := ifelse(humid_rel > 100, 1, 0)")
  ), flag2.2 = chainer(
    dt_syn_ac = paste0("flag2.2 := fifelse(",
      "rain_tot > 0.5 & humid_rel < 90 &",
      "c(humid_rel[2:(.N)], NA) < 90 &",
      "c(humid_rel[3:(.N)], NA, NA) < 90,",
      " 1, 0)"
    )
  ), check2.41 = chainer(
    dt_syn_ac = paste0("check2.41 := frollapply(humid_rel, 7,",
      " FUN = function(x) flag_all_equal(x, ignore = 100),",
      "align = \'left\')"
    )
  ), check2.42 = chainer(
    dt_syn_ac = paste0("check2.42 := frollapply(temp_air_avg, 7,",
      " FUN = function(x) if (isTRUE(sd(x) <= 0.05)) 1 else 0,",
      "align = \'left\')"
    )
  ), flag2.4 = chainer(
    dt_syn_ac = paste0("flag2.4 := fifelse(",
      "isTRUE(.SD[[\'check2.41\']] %in% 1) & ",
      "isTRUE(.SD[[\'check2.42\']] %in% 1),",
      " 1, 0)"
    )
  ), flag2.5 = chainer(
    dt_syn_ac = paste0("flag2.5 := fifelse(",
      "c(NA, humid_rel[2:.N] - humid_rel[1:(.N - 1)]) > 20, 1, 0)"
    )
  ), flag2.7 = chainer(
    dt_syn_ac = paste0("flag2.7 := fifelse(abs(",
      "c(humid_rel[2:(.N - 1)] - humid_rel[1:(.N - 2)], NA, NA)) > 10 & ",
      "abs(c(humid_rel[3:.N] - humid_rel[2:(.N - 1)], NA, NA)) < 5",
      ", 1, 0)"
    )
  ), flag2.8 = chainer(
    dt_syn_ac = paste0("flag2.8 := fifelse(abs(",
      "c(humid_rel[2:(.N - 1)] - humid_rel[1:(.N - 2)], NA, NA)) > 15 & ",
      "abs(c(humid_rel[3:.N] - humid_rel[2:(.N - 1)], NA, NA)) < 5",
      ", 1, 0)"
    )
  ), flag4.1 = chainer( # wind '4'
    dt_syn_ac = paste0(
      "flag4.1 := fifelse(wind_speed > 50 | wind_speed < 0, 1, 0)"
    )
  ), flag4.2 = chainer(
    dt_syn_ac = paste0(
      "flag4.2 := fifelse(wind_dir > 360 | wind_dir < 0, 1, 0)"
    )
  ), flag4.3 = chainer(
    dt_syn_ac = paste0("flag4.3 := frollapply(wind_dir, 288, FUN = ",
      "function(x) sd(x) < 5, align = \'left\')"
    )
  ), flag4.4 = chainer(
    dt_syn_ac = paste0("flag4.4 := frollapply(wind_dir, 40, FUN = ",
      "flag_all_equal, align = \'left\')"
    )
  ), flag4.5 = chainer(
    dt_syn_ac = paste0("flag4.5 := frollapply(wind_dir, 288, FUN = ",
      "flag4.5, align = \'left\')"
    )
  ), flag4.6 = chainer(
    dt_syn_ac = paste0("flag4.6 := fifelse(",
      "wind_speed > 32.7 & wind_speed < 120, 1, 0)"
    )
  ), flag4.7 = chainer(
    dt_syn_ac = paste0("flag4.7 := frollapply(wind_speed, 288, FUN = ",
      "flag4.7, align = \'left\')"
    )
  ), flag5.1 = chainer( # radiation '5'
    dt_syn_ac = paste0("flag5.1 := fifelse(",
      "rad_solar_avg > 2221 | rad_solar_avg < -20, 1, 0)"
    )
  ), flag5.2 = chainer(
    dt_syn_ac = paste0("flag5.2 := fifelse(",
      "c(NA, rad_solar_avg[2:.N] - rad_solar_avg[1:(.N - 1)]) > 1000, 1, 0)"
    )
  ), flag6.2 = chainer( # precipitation '6'
    dt_syn_ac = "flag6.2 := fifelse(rain_tot > 12.8, 1, 0)"
  ), checkcalc1 = chainer( # summarise and total checks and flags
    dt_syn_ac = "flag1.5 := fifelse(.SD[[\'check1.5\']] != \'normal\', 1, 0)"
  ), flagsfilter1 = chainer(
    dt_syn_ac = paste0(
      "flag_tot := rowSums(.SD, na.rm = TRUE), ",
      ".SDcols = c(", flag_cols_5m, "\')"
    )
  )
)

# dt_calc: hourly flags ----
x22 <- list(
  prep = chainer( # filter phens
    dt_syn_ac = paste0("names(dt)[",
      "names(dt) %ilike% \'date_time|batt_|pressure_|rain_tot\'],",
      " with = FALSE"
    )
  ), check0.1 = chainer( # battery '0'
    dt_syn_ac = paste0("check0.1 := fcase(batt_min > 13.6, \'high_warn\',",
      "batt_min >= 11 & batt_min < 12.5, \'med_warn\', ",
      "batt_min >= 9.6 & batt_min < 11, \'med_low_warn\', ",
      "batt_min < 9.6, \'low_err\', is.na(batt_min), NA, default = \'normal\')"
    )
  ), flag3.2 = chainer( # pressure '3'
    dt_syn_ac = paste0("flag3.2 := frollapply(pressure_atm, 24, FUN = ",
      "flag_all_equal, align = \'left\')"
    )
  ), flag3.6 = chainer(
    dt_syn_ac = paste0("flag3.6 := fifelse(pressure_atm > 1050 | ",
      "pressure_atm < 800, 1, 0)"
    )
  ), flag6.2 = chainer( # precipitation '6'
    dt_syn_ac = "flag6.2 := fifelse(rain_tot > 22.9, 1, 0)"
  ), check_calcs = chainer(
    dt_syn_ac = "flag0.1 := fifelse(.SD[[\'check0.1\']] != \'normal\', 1, 0)"
  ), flagsfilter1 = chainer(
    dt_syn_ac = paste0(
      "flag_tot := rowSums(.SD, na.rm = TRUE), ",
      ".SDcols = c(", flag_cols_1hr, "\')"
    )
  )
)

# # dt_calc: daily flags ----
x32 <- list(
  prep = chainer( # filter phens
    dt_syn_ac = paste0("names(dt)[",
      "names(dt) %ilike% \'date_time|temp_|rad_|rain_tot|humid_rel|wind_\'],",
      " with = FALSE"
    )
  ), flag1.3 = chainer( # temperature
    dt_syn_ac = paste0(
      "flag1.3 := fifelse(abs(temp_air_max - temp_air_min) < 0.5, 1, 0)"
    )
  ), check1.5 = chainer(
    dt_syn_ac = paste0("check1.5 := fcase(",
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
  ), flag1.6 = chainer(
    dt_syn_ac = paste0("flag1.6 := fifelse(temp_air_max < temp_air_min,",
      " 1, 0)"
    )
  ), flag1.7 = chainer(dt_syn_ac =
      "flag1.7 := fifelse(temp_ground_avg > temp_air_max, 1, 0)"
  ), flag2.1 = chainer( # humidity
    dt_syn_ac = paste0("flag2.1 := fcase(humid_rel_min > 100, 1, ",
      "humid_rel_max > 100, 1, is.na(humid_rel_min) | is.na(humid_rel_max),",
      " NA, default = 0)"
    )
  ), flag2.3 = chainer(
    dt_syn_ac = "flag2.3 := fifelse(humid_rel_min > humid_rel_max, 1, 0)"
  ), flag2.6 = chainer(
    dt_syn_ac = paste0(
      "flag2.6 := fifelse(abs(humid_rel_min - humid_rel_max) < 5, 1, 0)"
    )
  ), flag4.1 = chainer( # wind
    dt_syn_ac = paste0("flag4.1 := fifelse(wind_speed > 50 | ",
      "wind_speed_max > 50, 1, 0)"
    )
  ), flag4.2 = chainer(
    dt_syn_ac = paste0("flag4.2 := fifelse(wind_dir > 360 | wind_dir < 0, ",
      "1, 0)"
    )
  ), flag5.1 = chainer( # radiation
    dt_syn_ac = paste0("flag5.1 := fifelse(",
      "rad_solar_max > 2221 | rad_solar_max < -20, 1, 0)"
    )
  ), flag6.1 = chainer(
    dt_syn_ac = "flag6.1 := fifelse(rain_tot > 500, 1, 0)"
  ), check_sum = chainer(
    dt_syn_ac = "flag1.5 := fifelse(.SD[[\'check1.5\']] != \'normal\', 1, 0)"
  ), flagsfilter1 = chainer(
    dt_syn_ac = paste0(
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
  # first work on the 5 min data
  p_step(dt_n = 1, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "raw_5_mins"),
    output_dt = "dt_flag_raw_5_mins"
  ),
  # run checks/flags and summarise
  p_step(dt_n = 1, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x12)
  ),
  #work on hourly data
  p_step(dt_n = 2, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "raw_1_hours"),
    output_dt = "dt_flag_raw_1_hours"
  ),
  # run checks/flags and summarise
  p_step(dt_n = 2, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x22)
  ),
  #first work on the daily data
  p_step(dt_n = 3, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "raw_1_days"),
    output_dt = "dt_flag_raw_1_days"
  ),
  # run checks/flags and summarise
  p_step(dt_n = 3, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x32)
  ),
  p_step(dt_n = 4, dtp_n = 1, f = "dt_harvest",
    hsf_param_eval(), output_dt = "_tmp"
  ),
  p_step(dt_n = 4, dtp_n = 2, f = "dt_agg",
    agg_param_eval(agg_intervals = c("daily", "monthly", "annual"),
      ignore_nas = TRUE
    )
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