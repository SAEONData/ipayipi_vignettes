library(ipayipi)
#' Pipeline data processing sequence
#' Last updated: 2025-02-28
#' Author: Paul J Gordijn

#' Data processing sequence for cleaning/processing rainfall data from
#'  automatic tipping-bucket rainguages. Data will be processed into the
#'  folling time interval aggregations;
#' "5 mins", "hourly", "daily", "monthly", and "yearly".
#'
#'
# rule 6: precipitation
#  + 6.1: rainfall does not exceed 500 mm
#         (daily)
#         [M. Toucher suggests delete rainfall values that exceed this limit]
#  + 6.2: rainfall does exceed 10 mm in 5 mins
#         (5 min time step)
#         [M. Toucher noted that we do have high intensity events like this
#         (120 mm/hr). Therefore, these events should be flagged, not deleted.]
#         [saws 2.3.7]

# define parameters ----
#' general tipping bucket pipeline sequence for rainfall data
rain_tip <- 0.254 #' value in mm of a single tip-bucket tip

# calc parameters -> passed to `calc_param_eval()` ----
#' filter the events data based to extract 'pseudo events' (e.g., false tips)
x12 <- list(
  pseudo_ev1 = chainer(i = "event_type == \'pseudo_events\'"),
  pseudo_ev2 = chainer(i = "stnd_title == station"),
  pseudo_ev3 = chainer(i = "qa == TRUE")
)
#' create a start and end date_time -- necessary for discontinuous data
#' this will be appended to the discontinuous data series before data
#' aggregation
x22 <- list(
  fktest = chainer(j = "fktest := fifelse(.I == 1 | .I == .N, TRUE, FALSE)",
    measure = "smp", units = "logi", var_type = "logi"
  ),
  fork_se2 = chainer(i = "fktest == TRUE"),
  rain_mm = chainer(j = "rain_mm := rain_cumm",
    measure = "tot", units = "mm", var_type = "num"
  ),
  fork_se4 = chainer(j = "rain_mm := 0"),
  fork_se5 = chainer(j = ".(date_time, rain_mm)")
)
#' repeat the above after it was written to dt_rain_se
x32 <- list(
  fktest = chainer(j = "fktest := fifelse(.I == 1 | .I == .N, TRUE, FALSE)",
    measure = "smp", units = "logi", var_type = "logi"
  ),
  fork_se2 = chainer(i = "fktest == TRUE"),
  rain_mm = chainer(j = "rain_mm := 0",
    measure = "tot", units = "mm", var_type = "num"
  ),
  fork_se5 = chainer(j = ".(date_time, rain_mm)")
)
#' remove double tips (tips one second after the previous)
x42 <- list(
  logg_remove1 = chainer(i = "!is.na(rain_cumm)"),
  rain_diff = chainer(
    j = "rain_diff := c(0, rain_cumm[2:.N] - rain_cumm[1:(.N - 1)])",
    measure = "tot", units = "mm", var_type = "num"
  ),
  rain_diff_remove = chainer(i = "rain_diff != 0"),
  t_lag = chainer(
    j = "t_lag := c(0, date_time[2:.N] - date_time[1:(.N - 1)])",
    measure = "tot", units = "sec", var_type = "num"
  )
)
#' finalise false tips
x47 <- list(
  false_tip_type = chainer(j = c("false_tip_type := fifelse(",
      "logg_interfere_type %in% \'on_site\', \'interfere\', NA_character_)"
    ), measure = "smp", units = "false_tip", var_type = "chr"
  ),
  false_tip_type2 = chainer(j = c("false_tip_type := fifelse(",
      "is.na(false_tip_type) & event_type %in% \'pseudo_events\',",
      "\'pseudo_event\', false_tip_type)"
    )
  ),
  false_tip_type3 = chainer(j = c("false_tip_type := ",
      "fifelse(t_lag == 1, \'double_tip\', false_tip_type)"
    )
  ),
  false_tip = chainer(
    j = "false_tip := fifelse(!is.na(false_tip_type), TRUE, FALSE)",
    measure = "smp", units = "false_tip", var_type = "logi"
  ),
  false_tip5 = chainer(
    j = "false_tip := fifelse(problem_gap %in% FALSE, FALSE, false_tip)"
  ),
  false_tip6 = chainer(
    j = "false_tip := fifelse(is.na(false_tip), FALSE, false_tip)"
  ),
  clean_up1 = chainer(j = c(".(date_time, rain_cumm, rain_diff, ",
    "false_tip, false_tip_type, problem_gap)"
  )),
  false_tip_table = chainer(i = "false_tip == FALSE",
    j = ".(date_time, false_tip, false_tip_type, problem_gap)",
    fork_table = "pseudo_events"
  ),
  clean_up2 = chainer(j = ".(date_time)"),
  rain_mm = chainer(j = c("rain_mm := ", rain_tip),
    measure = "tot", units = "mm", var_type = "num"
  )
)
#' remove old start end date-time values
x52 <- list(clean_se = chainer(i = "!rain_mm == 0"))
#' filter gaps before joining --- only want 'problem gaps'
x62 <- list(pgap = chainer(i = "problem_gap == TRUE",
  j = ".(gap_start, gap_end, problem_gap)"
))
#' perform standard precipitation flag checks
#' 5 min data
x82 <- list(rainflagging1 = chainer(
  j = "flag6.2 := fifelse(rain_tot > 10, 1, 0)"
))
x83 <- list(rainflagging1 = chainer(i = "flag6.2 %in% 1"))


# build pipe sequence
#' this builds the table from the described parameters that will be evaluated
#' to generate parameters for processing the data
pipe_seq <- pipe_seq(p = pdt(
  # extract pseudo events
  p_step(dt_n = 1, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "meta_events"),
    output_dt = "dt_pseudo_event", time_interval = "discnt"
  ),
  p_step(dt_n = 1, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x12),
    output_dt = "dt_pseudo_event", time_interval = "discnt"
  ),
  # create 'fork_se'
  # the fork start and end date-time will be appended to data before
  # time interval aggregations---NB for event data
  p_step(dt_n = 2, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "raw_rain"),
    output_dt = "dt_rain_se", time_interval = "discnt"
  ),
  p_step(dt_n = 2, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x22),
    output_dt = "dt_rain_se", time_interval = "discnt"
  ),
  # join 'fork_se' with extant
  p_step(dt_n = 3, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_rain_se"),
    output_dt = "dt_rain_se", time_interval = "discnt"
  ),
  p_step(dt_n = 3, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x32),
    output_dt = "dt_rain_se", time_interval = "discnt"
  ),
  # find 'double tips'
  p_step(dt_n = 4, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "raw_rain"),
    output_dt = "dt_rain", time_interval = "discnt"
  ),
  p_step(dt_n = 4, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x42),
    output_dt = "dt_rain", time_interval = "discnt"
  ),
  # get interference eventse
  p_step(dt_n = 4, dtp_n = 3, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "logg_interfere"),
    output_dt = "dt_rain", time_interval = "discnt"
  ),
  p_step(dt_n = 4, dtp_n = 4, f = "dt_join",
    f_params = join_param_eval(join = "left_join", fuzzy = c(0, 600)),
    output_dt = "dt_rain", time_interval = "discnt"
  ),
  # add pseudo events
  p_step(dt_n = 4, dtp_n = 5, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_pseudo_event"),
    output_dt = "dt_rain", time_interval = "discnt"
  ),
  p_step(dt_n = 4, dtp_n = 6, f = "dt_join",
    f_params = join_param_eval(join = "left_join", fuzzy = 0,
      y_key = c("start_dttm", "end_dttm")
    ), output_dt = "dt_rain", time_interval = "discnt"
  ),
  p_step(dt_n = 4, dtp_n = 7, f = "dt_calc",
    f_params = calc_param_eval(x47),
    output_dt = "dt_rain", time_interval = "discnt"
  ),
  p_step(dt_n = 5, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(
      hsf_table = "dt_rain", phen_gaps = list(rain_cumm = "rain_mm")
    ),
    output_dt = "dt_rain", time_interval = "discnt"
  ),
  p_step(dt_n = 5, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x52),
    output_dt = "dt_rain", time_interval = "discnt"
  ),
  p_step(dt_n = 5, dtp_n = 3, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_rain_se"),
    output_dt = "dt_rain", time_interval = "discnt"
  ),
  p_step(dt_n = 5, dtp_n = 4, f = "dt_join",
    join_param_eval(), output_dt = "dt_rain", time_interval = "discnt"
  ),
  # harvest gap info then left join onto dt_rain TBI
  p_step(dt_n = 6, dtp_n = 1, f = "dt_harvest",
    hsf_param_eval(hsf_table = "gaps"),
    output_dt = "dt_gaps_tmp", time_interval = "discnt"
  ),
  p_step(dt_n = 6, dtp_n = 2, f = "dt_calc",
    calc_param_eval(x62), output_dt = "dt_gaps_tmp",
    time_interval = "discnt"
  ),
  p_step(dt_n = 7, dtp_n = 1, f = "dt_harvest",
    hsf_param_eval(hsf_table = "dt_rain"),
    output_dt = "dt_rain", time_interval = "discnt"
  ),
  p_step(dt_n = 7, dtp_n = 2, f = "dt_agg",
    agg_param_eval(
      agg_intervals = c("5 mins"),
      ignore_nas = FALSE, all_phens = FALSE,
      agg_parameters = aggs(
        rain_mm = agg_params(units = "mm", phen_out_name = "rain_tot")
      )
    )
  ),
  # flag tests
  p_step(dt_n = 8, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_5_mins_agg"),
    output_dt = "dt_5_mins_agg_flag"
  ),
  p_step(dt_n = 8, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x82),
    output_dt = "dt_5_mins_agg_flag"
  ),
  p_step(dt_n = 8, dtp_n = 3, f = "dt_calc",
    f_params = calc_param_eval(x83),
    output_dt = "dt_5_mins_agg_flag"
  ),
  p_step(dt_n = 9, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_5_mins_agg")
  ),
  p_step(dt_n = 9, dtp_n = 2, f = "dt_agg",
    f_params = agg_param_eval(
      agg_intervals = c("hourly"),
      ignore_nas = TRUE,
      all_phens = FALSE,
      agg_parameters = aggs(
        rain_tot = agg_params(units = "mm")
      )
    )
  ),
  p_step(dt_n = 10, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_1_hours_agg")
  ),
  p_step(dt_n = 10, dtp_n = 2, f = "dt_agg",
    agg_param_eval(
      agg_offset = "8 hours",
      agg_intervals = "daily",
      ignore_nas = TRUE,
      all_phens = FALSE,
      agg_parameters = aggs(
        rain_tot = agg_params(units = "mm")
      ),
      agg_dt_suffix = "_agg_saws"
    )
  ),
  p_step(dt_n = 11, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_1_hours_agg")
  ),
  p_step(dt_n = 11, dtp_n = 2, f = "dt_agg",
    f_params = agg_param_eval(
      agg_intervals = c("daily", "monthly", "yearly"),
      ignore_nas = TRUE
    )
  )
))
