library(ipayipi)
#' Pipeline rainfall data processing sequence
#' v1.00
#' Last updated 2024-07-30
#'
#' Summary
#' The pipe sequence is documented as `pipe_seq` below. First calculation
#'  parameters are defined as consequtively numbered 'x' variables. These
#'  are used in the `pipe_seq` object.
#' This `pipe_seq` cleans/processes rainfall data from automatic tipping-
#' bucket rainguages. Data processed into the folling time
#' interval aggregations; "5 mins", "hourly", "daily", "monthly", and "yearly".
#' For compatibility with SAWS an eight hour offest is provided in addition to
#' the standard daily aggregation.

# define constants ----
#' general tipping bucket pipeline sequence for rainfall data
rain_tip <- 0.254 #' value in mm of a single tip-bucket tip

# calc parameters -> passed to `calc_param_eval()` ----
#' filter the events data based to extract 'pseudo events' (i.e., false tips)
#' Pseudo events
x12 <- list(
  pseudo_ev1 = chainer(dt_syn_bc = "event_type == \'pseudo_events\'"),
  pseudo_ev2 = chainer(dt_syn_bc = "stnd_title == station"),
  pseudo_ev3 = chainer(dt_syn_bc = "qa == TRUE")
)
#' create a start and end date_time -- necessary for discontinuous data
#' this will be appended to the discontinuous data series before data
#' aggregation
x22 <- list(
  fork_se1 = chainer(
    dt_syn_ac = "fktest := fifelse(.I == 1 | .I == .N, TRUE, FALSE)"
  ),
  fork_se2 = chainer(dt_syn_bc = "fktest == TRUE"),
  fork_se3 = chainer(dt_syn_ac = "rain_mm := rain_cumm",
    measure = "tot", units = "mm", var_type = "num"
  ),
  fork_se4 = chainer(dt_syn_ac = "rain_mm := 0"),
  fork_se5 = chainer(dt_syn_ac = ".(date_time, rain_mm)")
)
#' repeat the above after it was written to dt_rain_se in chunked data
x32 <- list(
  fork_se1 = chainer(
    dt_syn_ac = "fktest := fifelse(.I == 1 | .I == .N, TRUE, FALSE)"
  ),
  fork_se2 = chainer(dt_syn_bc = "fktest == TRUE"),
  fork_se4 = chainer(dt_syn_ac = "rain_mm := 0"),
  fork_se5 = chainer(dt_syn_ac = ".(date_time, rain_mm)")
)
#' remove double tips (tips one second after the previous)
x42 <- list(
  logg_remove1 = chainer(dt_syn_bc = "!is.na(rain_cumm)"),
  rain_diff = chainer(
    dt_syn_ac = "rain_diff := c(0, rain_cumm[2:.N] - rain_cumm[1:(.N - 1)])",
    measure = "tot", units = "mm", var_type = "num"
  ),
  rain_diff_remove = chainer(dt_syn_bc = "rain_diff != 0"),
  t_lag = chainer(
    dt_syn_ac = "t_lag := c(0, date_time[2:.N] - date_time[1:(.N - 1)])",
    measure = "tot", units = "sec", var_type = "num"
  )
)
#' summarise pseudo events--false tips
x47 <- list(
  false_tip_type1 = chainer(dt_syn_ac = paste0("false_tip_type := fifelse(",
      "logg_interfere_type %in% \'on_site\', \'interfere\', NA_character_)"
    ), temp_var = FALSE, measure = "smp", units = "false_tip", var_type = "chr"
  ),
  false_tip_type2 = chainer(dt_syn_ac = paste0("false_tip_type := fifelse(",
    "is.na(false_tip_type) & event_type %in% \'pseudo_events\',",
    "\'pseudo_event\', false_tip_type)"
  )),
  false_tip_type3 = chainer(dt_syn_ac = paste0("false_tip_type := ",
    "fifelse(t_lag == 1, \'double_tip\', false_tip_type)"
  )),
  false_tip4 = chainer(
    dt_syn_ac = "false_tip := fifelse(!is.na(false_tip_type), TRUE, FALSE)",
    temp_var = FALSE, measure = "smp", units = "false_tip", var_type = "logi"
  ),
  false_tip5 = chainer(dt_syn_ac =
      "false_tip := fifelse(problem_gap %in% FALSE, FALSE, false_tip)"
  ),
  false_tip6 = chainer(
    dt_syn_ac = "false_tip := fifelse(is.na(false_tip), FALSE, false_tip)"
  ),
  clean_up1 = chainer(dt_syn_ac = paste0(".(date_time, rain_cumm, rain_diff, ",
    "false_tip, false_tip_type, problem_gap)"
  )),
  false_tip_table = chainer(dt_syn_bc = "false_tip == FALSE",
    dt_syn_ac = ".(date_time, false_tip, false_tip_type, problem_gap)",
    fork_table = "pseudo_events"
  ),
  clean_up2 = chainer(dt_syn_ac = paste0(".(date_time)")),
  rain_mm = chainer(dt_syn_ac = paste0("rain_mm := ", rain_tip),
    measure = "tot", units = "mm", var_type = "num"
  )
)
#' remove old start end date-time values
x52 <- list(clean_se = chainer(dt_syn_bc = "!rain_mm == 0"))
#' filter gaps before joining --- only want 'problem gaps'
x62 <- list(pgap = chainer(dt_syn_bc = "problem_gap == TRUE",
  dt_syn_ac = ".(gap_start, gap_end, problem_gap)"
))

# build pipe sequence ----
#' this builds the table from the described parameters that will be evaluated
#' to generate parameters for processing the data
pipe_seq <- pipe_seq(p = pdt(
  # extract pseudo events
  p_step(dt_n = 1, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "meta_events"),
    output_dt = "dt_pseudo_event"
  ),
  p_step(dt_n = 1, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x12),
    output_dt = "dt_pseudo_event"
  ),
  #' create 'fork_se'
  #' the fork start and end date-time will be appended to data before
  #' time interval aggregations---NB for event data
  #' this feeds the aggregation function the full range of data to
  #' be aggregated. Without this the leading and training time where events
  #' aren't logged will be cut short from the aggregation
  p_step(dt_n = 2, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "raw_rain"),
    output_dt = "dt_rain_se"
  ),
  p_step(dt_n = 2, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x22),
    output_dt = "dt_rain_se"
  ),
  # join 'fork_se' with old 'fork_se'
  p_step(dt_n = 3, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_rain_se"),
    output_dt = "dt_rain_se"
  ),
  p_step(dt_n = 3, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x32),
    output_dt = "dt_rain_se"
  ),
  # find 'double tips'
  p_step(dt_n = 4, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "raw_rain"),
    output_dt = "dt_rain"
  ),
  p_step(dt_n = 4, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x42),
    output_dt = "dt_rain"
  ),
  # get interference events
  p_step(dt_n = 4, dtp_n = 3, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "logg_interfere"),
    output_dt = "dt_rain"
  ),
  p_step(dt_n = 4, dtp_n = 4, f = "dt_join",
    f_params = join_param_eval(join = "left_join", fuzzy = c(0, 600))
  ),
  # add pseudo events
  p_step(dt_n = 4, dtp_n = 5, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_pseudo_event"),
    output_dt = "dt_rain"
  ),
  p_step(dt_n = 4, dtp_n = 6, f = "dt_join",
    f_params = join_param_eval(join = "left_join", fuzzy = 0,
      y_key = c("start_dttm", "end_dttm")
    ), output_dt = "dt_rain"
  ),
  p_step(dt_n = 4, dtp_n = 7, f = "dt_calc",
    f_params = calc_param_eval(x47),
    output_dt = "dt_rain"
  ),
  p_step(dt_n = 5, dtp_n = 1, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_rain"),
    output_dt = "dt_rain"
  ),
  p_step(dt_n = 5, dtp_n = 2, f = "dt_calc",
    f_params = calc_param_eval(x52),
    output_dt = "dt_rain"
  ),
  p_step(dt_n = 5, dtp_n = 3, f = "dt_harvest",
    f_params = hsf_param_eval(hsf_table = "dt_rain_se"),
    output_dt = "dt_rain"
  ),
  p_step(dt_n = 5, dtp_n = 4, f = "dt_join",
    join_param_eval(), output_dt = "dt_rain"
  ),
  p_step(dt_n = 6, dtp_n = 1, f = "dt_harvest",
    hsf_param_eval(hsf_table = "gaps"),
    output_dt = "dt_gaps_tmp"
  ),
  p_step(dt_n = 6, dtp_n = 2, f = "dt_calc",
    calc_param_eval(x62), output_dt = "dt_gaps_tmp"
  ),
  # aggregate data
  p_step(dt_n = 7, dtp_n = 1, f = "dt_harvest",
    hsf_param_eval(hsf_table = "dt_rain"),
    output_dt = "dt_rain"
  ),
  p_step(dt_n = 7, dtp_n = 2, f = "dt_agg",
    agg_param_eval(
      agg_intervals = c("5 mins", "hourly", "daily"),
      ignore_nas = TRUE, all_phens = FALSE,
      agg_parameters = aggs(
        rain_mm = agg_params(units = "mm", phen_out_name = "rain_tot")
      )
    )
  ),
  # saws daily data aggregation
  p_step(dt_n = 7, dtp_n = 3, f = "dt_agg",
    agg_param_eval(
      agg_offset = "8 hours",
      agg_intervals = c("daily"),
      ignore_nas = TRUE,
      all_phens = FALSE,
      agg_parameters = aggs(
        rain_mm = agg_params(units = "mm", phen_out_name = "rain_tot")
      ),
      agg_dt_suffix = "_agg_saws"
    )
  ),
  p_step(dt_n = 8, dtp_n = 1, f = "dt_harvest",
    hsf_param_eval(hsf_table = "dt_1_days_agg")
  ),
  p_step(dt_n = 8, dtp_n = 2, f = "dt_agg",
    agg_param_eval(
      agg_intervals = c("monthly", "yearly"),
      ignore_nas = TRUE
    )
  )
))