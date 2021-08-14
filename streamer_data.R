# load packages ----
library(dplyr)
library(tidyr)
library(purrr)

# load scripts ----
source("utils.R")

# yml_file <- "data-raw/streamers.yml"
# yml_data <- yaml::read_yaml(file = yml_file)

yml_files <- fs::dir_ls("yaml_files")

yml_data <- purrr::map(yml_files, ~yaml::read_yaml(file = .x))

#tibble::tibble(yml_data2) %>% tidyr::unnest_wider(1)

# set up authentication via environment variables
twitchr::twitch_auth()

streamer_data <- tibble::tibble(yml_data) %>%
    tidyr::unnest_wider(1) %>%
    dplyr::filter(platform == "twitch") %>%
    dplyr::mutate(user_data = purrr::map(user_id, ~get_twitch_id(user_name = .x))) %>%
    tidyr::unnest(cols = user_data) %>%
    dplyr::mutate(schedule_data = purrr::map(id, ~get_twitch_schedule(.x)),
            videos_data = purrr::map(id, ~get_twitch_videos(.x)))
