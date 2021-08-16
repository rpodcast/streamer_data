# load packages ----
library(dplyr)
library(tidyr)
library(purrr)
library(aws.s3)

# import config settings ----
config <- config::get()

# set up authentication via environment variables
twitchr::twitch_auth()

# load scripts ----
source("utils.R")

# import yaml files ----
yml_files <- fs::dir_ls("yaml_files")
yml_data <- purrr::map(yml_files, ~yaml::read_yaml(file = .x)) %>%
  tibble::tibble() %>%
  tidyr::unnest_wider(1)

streamer_data <- yml_data %>%
    dplyr::filter(platform == "twitch") %>%
    dplyr::mutate(user_data = purrr::map(user_id, ~get_twitch_id(user_name = .x))) %>%
    tidyr::unnest(cols = user_data) %>%
    dplyr::mutate(schedule_data = purrr::map(id, ~get_twitch_schedule(.x)),
            videos_data = purrr::map(id, ~get_twitch_videos(.x)))

if (config::is_active("production")) {
  s3saveRDS(
    x = streamer_data,
    object = "streamer_data_current.rds",
    acl = "public-read",
    bucket = Sys.getenv("DO_SPACE"),
    key = Sys.getenv("DO_ACCESS_KEY_ID"),
    secret = Sys.getenv("DO_SECRET_ACCESS_KEY"),
    region = Sys.getenv("DO_DATACENTER"),
    base_url = "linodeobjects.com"
  )
} else {
  saveRDS(streamer_data, file = "data/streamer_data_current.rds")
}
