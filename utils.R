#' @importFrom twitchr get_users
#' @importFrom dplyr filter
#' @noRd 
get_twitch_id <- function(user_name) {
  user <- twitchr::get_users(login = user_name)
  message(glue::glue("user_name: {user_name} - id: {x}", x = user$id))
  res <- dplyr::select(user, id, description, profile_image_url)
  return(res)
}

parse_duration <- function(x, 
                           time_unit = c("seconds", "minutes", "hours"), 
                           dur_regex = "([0-9]{1,2}h)?([0-9]{1,2}m)?([0-9]{1,2}(\\.[0-9]{1,3})?s)?") {
  # process to reverse engineer the starting time of each video
  # clever regex found at https://stackoverflow.com/a/11293491
  # we get a matrix back with 2 rows (second row is meaningless)
  # columns are the following
  # - col 1: the raw duration string
  # - col 2: the "hour" part of the duration string (ex "1h")
  # - col 3: the "minute" part of the duration string (ex "1m")
  # - col 4: the "second" part of the duration string (ex "1s")
  # - col 5: meaningless
  time_unit <- match.arg(time_unit)
  dur_parsed <- stringr::str_match_all(x, dur_regex)[[1]]

  # set up final duration
  dur_final <- 0

  # extract relevant parts of duration matrix
  dur_vec <- readr::parse_number(dur_parsed[1, c(2,3,4)])
  names(dur_vec) <- c("hours", "minutes", "seconds")
  time_mult <- switch(
    time_unit,
    hours = c(1, (1/60), (1/3600)),
    minutes = c(60, 1, (1/60)),
    seconds = c(3600, 60, 1)
  )
  dur_vec <- sum(dur_vec * time_mult)
  return(dur_vec)
}

# https://stackoverflow.com/questions/27397332/find-round-the-time-to-nearest-half-an-hour-from-a-single-file
round_time <- function(x) {
  x <- as.POSIXlt(x)
  x$min <- round(x$min / 30) * 30
  x$sec <- floor(x$sec / 60)
  x <- as.POSIXct(x)
  return(x)
}

shift_week <- function(x, convert = TRUE, which = "next") {
  if (!convert) {
    return(x)
  } else {
    # get current day of week from supplied date
    x_weekday <- clock::as_year_month_weekday(x) %>% clock::get_day()
    res <- clock::date_shift(x, target = clock::weekday(x_weekday), which = which, boundary = "advance", ambiguous = "earliest")
    return(res)
  }
}

compute_end_clock <- function(start_clock, stream_length, precision = "hour") {
  # truncate the stream length to floor of nearest hour
  new_length <- clock::duration_round(clock::duration_seconds(stream_length), precision = precision)
  end_clock <- clock::add_hours(start_clock, new_length)
  return(end_clock)
}

time_parser <- function(x, orig_zone = "UTC", new_zone = "America/New_York", format = "%Y-%m-%dT%H:%M:%SZ", convert_to_char = TRUE) {
  if (is.character(x)) {
    x <- clock::date_time_parse(x, orig_zone, format = format)
  }
  
  x_z <- clock::as_zoned_time(x)

  # change to the desired time zone
  x_final <- clock::zoned_time_set_zone(x_z, new_zone) %>% clock::as_naive_time()
  
  if (convert_to_char) {
    x_final <- as.character(x_final)
  }
  return(x_final)
}


get_twitch_schedule <- function(id) {
  message(glue::glue("parsing schedule for {id}"))
  
  safe_schedule <- purrr::safely(twitchr::get_schedule, otherwise = NULL)
  
  r <- safe_schedule(broadcaster_id = id, clean_json = TRUE)
  status <- is.null(r$result)

  if (status) {
    warning(glue::glue("User {id} does not have valid schedule data. Proceeding to infer a schedule based on videos uploaded (status code {status})"))
    r <- httr::GET("https://api.twitch.tv/helix/videos", query = list(user_id = id, period = "week"))
    status_vid <- httr::status_code(r)

    if (status_vid != 200) {
      warning(glue::glue("User {id} does not have any videos! Skipping schedule parsing..."))
      return(NULL)
    } else {
      current_weekday <- clock::date_now("America/New_York") %>%  
        clock::as_year_month_weekday() %>% 
        clock::get_day()

      prev_week_date <- clock::date_now("America/New_York") %>% 
        clock::date_shift(target = clock::weekday(current_weekday), which = "previous", boundary = "advance")

      current_sunday <- clock::date_now("America/New_York") %>% 
        clock::date_shift(target = clock::weekday(clock::clock_weekdays$sunday), which = "previous")

      res <- httr::content(r, "parsed")
      if (length(res$data) < 1) {
        warning(glue::glue("User {id} does not have any videos! Skipping schedule parsing..."))
        return(NULL)
      }
      
      res <- httr::content(r, "parsed") %>%
        purrr::pluck("data") %>%
        tibble::tibble() %>%
        tidyr::unnest_wider(1)

      res_int <- res %>%
        mutate(start = purrr::map(created_at, ~time_parser(.x, convert_to_char = FALSE))) %>%
        mutate(start = purrr::map(start, ~clock::as_date_time(.x, zone = "America/New_York"))) %>%
        mutate(duration2 = purrr::map_dbl(duration, ~parse_duration(.x, "seconds"))) %>%
        tidyr::unnest(cols = c(start)) %>%
        mutate(start = purrr::map(start, ~round_time(.x))) %>%
        mutate(end = purrr::map2(start, duration2, ~compute_end_clock(.x, .y))) %>%
        mutate(category = "time", 
               recurrenceRule = "Every week",
               start_time = NA, 
               end_time = NA) %>%
        tidyr::unnest(cols = c(start, end)) %>%
        filter(start > prev_week_date)
        
      if (nrow(res_int) < 1) {
        return(NULL)
      } else {
        res_final <- res_int %>%
          mutate(before_week_ind = start < current_sunday) %>%
          mutate(start = purrr::map2(start, before_week_ind, ~shift_week(.x, .y))) %>%
          mutate(end = purrr::map2(end, before_week_ind, ~shift_week(.x, .y))) %>%
          tidyr::unnest(cols = c(start, end)) %>%
          mutate(start = as.character(start), end = as.character(end)) %>%
          dplyr::select(start_time, start, end_time, end, title, category, recurrenceRule)
      }
    }
  } else {
    r <- r$result
    res <- r$data

    res_int <- res %>%
      mutate(start = purrr::map(start_time, ~time_parser(.x, convert_to_char = FALSE)),
             end = purrr::map(end_time, ~time_parser(.x, convert_to_char = FALSE)),
             category = "time",
             recurrenceRule = "Every week") %>%
      dplyr::select(start_time, start, end_time, end, title, category, recurrenceRule) 

    # grab the first records of each unique stream     
    res_first <- res_int %>%
      dplyr::group_by(title) %>%
      dplyr::arrange(title, start) %>%
      dplyr::slice(1) %>%
      dplyr::ungroup() %>%
      mutate(start = purrr::map(start, ~clock::as_date_time(.x, zone = "America/New_York")),
              end = purrr::map(end, ~clock::as_date_time(.x, zone = "America/New_York"))) %>%
      mutate(start = purrr::map(start, ~shift_week(.x, which = "previous")),
              end = purrr::map(end, ~shift_week(.x, which = "previous"))) %>%
      mutate(start = purrr::map(start, ~clock::as_naive_time(.x)),
             end = purrr::map(end, ~clock::as_naive_time(.x)))
    
    # bind back together
    res_final <- dplyr::bind_rows(
      tidyr::unnest(res_first, c("start", "end")), 
      tidyr::unnest(res_int, c("start", "end"))
    )
    
    # perform additional filtering if vacation is defined
    vacation_ind <- !is.null(r$vacation)
    
    if (vacation_ind) {
      message(glue::glue("applying vacation rules for {id}"))
      # obtain the vacation data frame and get clock version of date/time
      res_vacation <- r$vacation %>%
        mutate(start = purrr::map(start_time, ~time_parser(.x, convert_to_char = FALSE)),
               end = purrr::map(end_time, ~time_parser(.x, convert_to_char = FALSE))) %>%
        tidyr::unnest(c("start", "end")) %>%
        select(start, end)
      
      start_vacation <- res_vacation %>% pull(start)
      end_vacation <- res_vacation$end
      
      # remove records in vacation start/end window
      res_final <- res_final %>%
        mutate(in_vac_window = ((start > start_vacation) & (end < end_vacation))) %>%
        filter(!in_vac_window) %>%
        select(., -in_vac_window)
    }
    
    # convert start and end back to character for use with shinycal
    res_final <- mutate(res_final, start = as.character(start), end = as.character(end))
  }

  # remove any records with missing start or end
  missing_start_ind <- any(is.na(res_final$start_time))
  missing_end_ind <- any(is.na(res_final$end_time))

  if (any(missing_start_ind, missing_end_ind)) {
    res_final <- res_final %>%
      filter(!is.na(start_time) & !is.na(end_time))

    if (nrow(res_final) < 1) return(NULL)
  }
  
  return(res_final)
}


get_twitch_videos <- function(id) {
  message(glue::glue("twitch id {id}"))
  
  # calculate current cutoff for recent videos
  cutoff <- clock::add_months(clock::date_now(zone = "UTC"), -2) %>%
    time_parser(orig_zone = "UTC", convert_to_char = FALSE)

  videos <- twitchr::get_videos(user_id = id, first = 100) 

  if (is.null(videos)) {
    # try getting clips instead
    videos <- twitchr::get_all_clips(broadcaster_id = id)
    if (is.null(videos)) {
      warning(glue::glue("There are no videos for user {id}"))
      return(NA)
    } else {
      videos_play <- videos %>%
        dplyr::mutate(video_id = purrr::map_chr(url, ~{
          tmp <- stringr::str_split(.x, "/")
          n_items <- length(tmp[[1]])
          res <- tmp[[1]][n_items]
          return(res)
        })) %>%
        dplyr::slice(1)

        video_dt <- videos_play %>%
          pull(created_at) %>%
          time_parser(orig_zone = "UTC", convert_to_char = FALSE)
          
        video_recent <- video_dt > cutoff
      
        res <- list(
          video_id = dplyr::pull(videos_play, video_id),
          video_recent = video_recent
        )

      return(res)
    }
  }

  videos_play <- videos$data %>%
    dplyr::mutate(video_id = purrr::map_chr(url, ~{
      tmp <- stringr::str_split(.x, "/")
      n_items <- length(tmp[[1]])
      res <- tmp[[1]][n_items]
      return(res)
    })) %>%
    dplyr::slice(1)

    video_dt <- videos_play %>%
      pull(created_at) %>%
      time_parser(orig_zone = "UTC", convert_to_char = FALSE)
      
    video_recent <- video_dt > cutoff

    res <- list(
      video_id = dplyr::pull(videos_play, video_id),
      video_recent = video_recent
    )    

  return(res)
}
