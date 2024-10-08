library(ipayipi)
# Station processing sequence for: vasi_sci_centre_aws
# This is a processing pipeline setup customised for the vasi science centre
# The first part of the code sets up some calculations that are embedded in
# the following described pipe build sequence.
# Before code though, some comments on checks or flags, each is numbered per
# variable or phenomena type and referenced.

# check rule summary ----
# rule 0: battery
#  - 0.1: 'high_warn': voltage not greater than 13.6 volts
#         [saws warning: 2.3.9.1]
#       - 'med_warn': voltage is greater or equal to 11, but less than 12.5
#         volts
#         [saws warning: 2.3.9.2]
#       - 'med_low_warn': voltage is greater or equal to 9.6, but less than
#         11.0 volts
#         [saws warning: delete pressure values; warning for other parameters
#         2.3.9.3]
#       - 'low_err': voltge less than 9.6 volts
#         [saws delete all: 2.3.8.4]
# rule 1: temperature
#  - 1.1: consecutive values in window (n=20) non-identical (5 min time step)
#         [saws 2.3.1.3]
#  - 1.2: difference between two consecutive values exceeds 5.0 deg C
#         (5 min time step)
#         [saws 2.3.2.3]
#  - 1.3: difference between max and min less than 0.5 deg C
#         (daily time step)
#         [saws 2.3.3.3]
#  - 1.4: spike filter window length 3. Difference between 1 & 2 is greater than
#         1.0 deg C and 2 & 3 less than 0.5 deg C.
#         (5 min time step)
#         [saws 2.3.4.3]
#  - 1.5: temp (air and gournd) not greater than 45 or less than -20.0 deg C
#         (all time steps)
#         [saws 2.3.5.3]
#  - 1.6: max not smaller than min (daily)
#  - 1.7: ground temp not greater than max air temp (daily)
# rule 2: humidity
#  - 2.1: all humidity values not greater than 100% (all time steps)
#         [c.f. saws 2.3.5.1]
#  - 2.2: if raining, humidity not less than 90% (5 min time step)
#  - 2.3: max not less than min (daily)
#  - 2.4: consecutive values in window (n=80) non-identical (5 min time step)
#         [saws 2.3.1.1]
#  - 2.5: difference between two consecutive values exceeds 20%
#         (5 min time step)
#         [saws 2.3.2.1]
#  - 2.6: difference between max and min less than 5% (daily step)
#         [saws 2.3.3.1]
#         * improve by considering rainfall
#  - 2.7: spike filter window length 3. Difference between 1 & 2 is greater than
#         10% and 2 & 3 less than 5%
#         (5 min (or highest res) time step)
#         [saws 2.3.4.1]
# rule 3: pressure
#  - 3.1: consecutive values in window (n=40) non-identical (5 min time step)
#         [saws 2.3.1.2]
#  - 3.2: consecutive values in window (n=24) non-identical (daily time step)
#         [c.f. saws 2.3.1.2 - implemented for when time res not 5 min]
#  - 3.3: difference between two consecutive values exceeds 2.0 hPa
#         (5 min time step)
#         [saws 2.3.2.2]
#  - 3.4: difference between max and min less than 1.0 hPa (daily step)
#         [saws 2.3.3.2]
#         * not implemented *
#  - 3.5: spike filter window length 3. Difference between 1 & 2 is greater than
#         1.0 hPa and 2 & 3 less than 0.5 hPa.
#         (5 min time step)
#         [saws 2.3.4.2]
#         * not implemented *
#  - 3.6: Values less than 800.0 or greater than 1050.0 hPa
#         [saws 2.3.5.2]
# rule 4: wind
#  - 4.1: wind speed not less than 0 or greater than 52 m/s (all time steps)
#         [saws 2.3.8.2: wind gust limit test
#          (saws use upper limit of 120 m per sec)]
#  - 4.2: wind direction not less than 0 or greater than 360 (all time steps)
#         [saws 2.3.8.1: limit test]
#  - 4.3: there must be wind direction readings greater or equal to 5 deg over a
#         24 hour period (5 min time step)
#         [saws 2.3.8.1: small range test]
#  - 4.4: flag if 40 identical wind direction readings
#         [saws 2.3.8.1: identical tests]
#  - 4.5: daily max and min range of wind speed must be greater or equal to
#         3.0 m per sec (5 min time step)
#         [saws 2.3.8.2: small range test]
#  - 4.6: values greater than 32.7 m per sec and less than 120 m per sec are
#         flagged as a warning (5 min time step)
#         [saws 2.3.8.2: wind gust limit warning]
#  - 4.7: if daily average wind speed is less than 20.0 m per sec then the
#         difference between the average and max must not be greater than
#         20.0 m per sec. If daily wind speed is greater than 20.0 m per sec
#         then the difference between the average and max must not be greater
#         than 25.0 m per sec (daily time step)
#         [saws 2.3.8.2: wind gust/average test]
# rule 4: solar radiation
#  - 5.1: does not exceed 1600 w/m2 (all time steps)
# rule 6: precipitation
#  - 6.1: rainfall does not exceed 500 mm (daily)
#  - 6.2: rainfall does exceed 10 mm in 5 mins (5 min time step)
#         [saws 2.3.7]

# below are some calculations that are embedded in the pipeline as
# recommended by Matt Dowie (data.table).
# checks are only run on new data when running pipeline in sequential mode
# dt_calc: raw 5 min flags ----
x12 <- list(
  prep = chainer( # filter phens
    dt_syn_ac = paste0("names(dt)[names(dt) %ilike% ",
      "\'date_time|temp_|rad_solar|rain_tot|humid_rel|wind_\'],",
      " with = FALSE"
    )
  ), flag1.1 = chainer( # temperature '1'
    dt_syn_ac = paste0("flag1.1 := frollapply(temp_air_avg, 20,",
      " FUN = flag_all_equal, align = \'right\')"
    )
  ), flag1.2 = chainer(
    dt_syn_ac = paste0("flag1.2 := frollapply(temp_air_avg, 2,",
      " FUN = function(x) fifelse(abs(x[2] - x[1]) > 5, 1, 0), ",
      "align = \'right\')"
    )
  ), flag1.4 = chainer(
    dt_syn_ac = paste0("flag1.4 := frollapply(temp_air_avg, 3,",
      " FUN = function(x) fifelse(abs(x[2] - x[1]) > 1 & ",
      "abs(x[3] - x[2]) < 0.5, 1, 0), ",
      "align = \'right\')"
    )
  ), flag1.5 = chainer(
    dt_syn_ac = paste0("flag1.5 := fcase(",
      "temp_air_avg > 45 | temp_air_avg < -20, 1,",
      " temp_ground_avg > 45 | temp_ground_avg < -20, 1, default = 0)"
    )
  ), flag2.1 = chainer( # humidity '2'
    dt_syn_ac = paste0("flag2.1 := ifelse(humid_rel > 100, 1, 0)")
  ), flag2.2 = chainer(
    dt_syn_ac = paste0("flag2.2 := fifelse(rain_tot > 0 & humid_rel < 90,",
      " 1, 0)"
    )
  ), flag2.4 = chainer(
    dt_syn_ac = paste0("flag2.4 := frollapply(humid_rel, 80,",
      " FUN = flag_all_equal, align = \'right\')"
    )
  ), flag2.5 = chainer(
    dt_syn_ac = paste0("flag2.5 := frollapply(humid_rel, 2,",
      " FUN = function(x) fifelse(abs(x[1] - x[2]) > 20, 1, 0), ",
      "align = \'right\')"
    )
  ), flag2.7 = chainer(
    dt_syn_ac = paste0("flag2.7 := frollapply(humid_rel, 3, FUN = ",
      "function(x) fifelse(abs(x[1] - x[2]) > 10 & abs(x[3] - x[2]) < 5, ",
      "1, 0), align = \'right\')"
    )
  ), flag4.1 = chainer( # wind '4'
    dt_syn_ac = paste0(
      "flag4.1 := fifelse(wind_speed > 52 | wind_speed < 0, 1, 0)"
    )
  ), flag4.2 = chainer(
    dt_syn_ac = paste0(
      "flag4.2 := fifelse(wind_dir > 360 | wind_dir < 0, 1, 0)"
    )
  ), flag4.3 = chainer(
    dt_syn_ac = paste0("flag4.3 := frollapply(wind_dir, 288, FUN = ",
      "function(x) all(x < 5), align = \'right\')"
    )
  ), flag4.4 = chainer(
    dt_syn_ac = paste0("flag4.4 := frollapply(wind_dir, 40, FUN = ",
      "flag_all_equal, align = \'right\')"
    )
  ), flag4.5 = chainer(
    dt_syn_ac = paste0("flag4.5 := frollapply(wind_dir, 288, FUN = ",
      "flag4.5, align = \'right\')"
    )
  ), flag4.6 = chainer(
    dt_syn_ac = paste0("flag4.6 := fifelse(",
      "wind_speed > 32.7 & wind_speed < 120, 1, 0)"
    )
  ), flag4.7 = chainer(
    dt_syn_ac = paste0("flag4.7 := frollapply(wind_speed, 288, FUN = ",
      "flag4.7, align = \'right\')"
    )
  ), flag5.1 = chainer( # radiation
    dt_syn_ac = paste0("flag5.1 := fcase(rad_solar_avg > 1600, 1, ",
      "default = 0)"
    )
  )
)

# filter the flags to generate table with warnings
x13 <- list(flagsfilter1 = chainer(
  dt_syn_ac = paste0("flag_tot := rowSums(.SD, na.rm = TRUE), ",
    ".SDcols = names(dt)[names(dt) %ilike% \'flag\']"
  )
), flagsfilter2 = chainer(dt_syn_bc = "flag_tot > 0"))


# put pipeline steps together (also using described calculations above)
# build pipe sequence ----
pipe_seq <- pipe_seq(p = pdt(
  # first work on the 5 min data
  p_step(dt_n = 1, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "raw_5_mins"),
    output_dt = "dt_flag_raw_5_mins"
  ),
  # run checks/flags
  p_step(dt_n = 1, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x12)
  ),
  # summarise flags/checks
  p_step(dt_n = 1, dtp_n = 3, f = "dt_calc",
    f_params = calc_param_eval(x13)
  ),
  # aggregate data into level one products
  p_step(dt_n = 4, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(),
    output_dt_suffix = "_tmp"
  ),
  p_step(dt_n = 4, dtp_n = 2, f = "dt_agg",
    f_params = agg_param_eval(
      agg_intervals = c("hourly", "days", "months", "years"),
      ignore_nas = TRUE
    )
  )
))