source("utils.R")
# ?get_targeting
# get_targeting("41459763029", timeframe = "LAST_90_DAYS")
# debugonce(get_targeting)

library(httr)
library(tidyverse)

walk_progress <- function(.x, .f, ...) {
  .f <- purrr::as_mapper(.f, ...)
  pb <- progress::progress_bar$new(
    total = length(.x), 
    format = " (:spin) [:bar] :percent | :current / :total | eta: :eta",
    # format = " downloading [:bar] :percent eta: :eta",
    force = TRUE)
  
  f <- function(...) {
    pb$tick()
    .f(...)
  }
  purrr::walk(.x, f, ...)
}

map_progress <- function(.x, .f, ...) {
  .f <- purrr::as_mapper(.f, ...)
  pb <- progress::progress_bar$new(
    total = length(.x), 
    format = " (:spin) [:bar] :percent | :current / :total | eta: :eta",
    # format = " downloading [:bar] :percent eta: :eta",
    force = TRUE)
  
  f <- function(...) {
    pb$tick()
    .f(...)
  }
  purrr::map(.x, f, ...)
}

map_dfr_progress <- function(.x, .f, ...) {
  .f <- purrr::as_mapper(.f, ...)
  pb <- progress::progress_bar$new(
    total = length(.x), 
    format = " (:spin) [:bar] :percent | :current / :total | eta: :eta",
    # format = " downloading [:bar] :percent eta: :eta",
    force = TRUE)
  
  f <- function(...) {
    pb$tick()
    .f(...)
  }
  purrr::map_dfr(.x, f, ...)
}

map_chr_progress <- function(.x, .f, ...) {
  .f <- purrr::as_mapper(.f, ...)
  pb <- progress::progress_bar$new(
    total = length(.x), 
    format = " (:spin) [:bar] :percent | :current / :total | eta: :eta",
    # format = " downloading [:bar] :percent eta: :eta",
    force = TRUE)
  
  f <- function(...) {
    pb$tick()
    .f(...)
  }
  purrr::map_chr(.x, f, ...)
}



tstamp <- Sys.time()

write_lines(lubridate::as_date(tstamp), "tstamp.txt")

dir.create(paste0("historic/", lubridate::as_date(tstamp)), recursive = T)



# tstamp <- Sys.time()

more_data <- dir("data/reports", full.names = T) %>%
  map_dfr(~read_csv(.x) %>% mutate(path = .x)) %>%
  mutate(date_produced = str_remove_all(path, "data/reports/FacebookAdLibraryReport_|_NL_yesterday_advertisers\\.csv")) %>%
  mutate(date_produced = lubridate::ymd(date_produced)) %>%
  janitor::clean_names()%>% #rename(advertiser_id = page_id) %>%
  mutate(spend = readr::parse_number(amount_spent_eur)) %>%
  mutate(spend = ifelse(spend == 100, 50, spend)) %>%
  distinct(page_id, .keep_all = T)  %>%
  mutate(party1 = case_when(
    str_detect(page_name, "VVD") ~ "VVD",
    str_detect(page_name, "\\bCDA\\b") ~ "CDA",
    str_detect(page_name, "PvdA|Jonge Socialisten") ~ "PvdA",
    str_detect(page_name, "D66|Jonge Democraten") ~ "D66",
    str_detect(page_name, "GroenLinks|GL") ~ "GroenLinks",
    str_detect(page_name, "ChristenUnie|CU") ~ "ChristenUnie",
    str_detect(page_name, "\\bSP\\b") ~ "SP",
    str_detect(page_name, "FvD|FVD|Forum voor Democratie") ~ "FvD",
    str_detect(page_name, "50Plus|50PLUS") ~ "50PLUS",
    str_detect(page_name, "\\bSGP\\b") ~ "SGP",
    str_detect(page_name, "PvdD|Partij voor de Dieren") ~ "PvdD",
    str_detect(page_name, "PVV") ~ "PVV",
    str_detect(page_name, "DENK") ~ "DENK",
    str_detect(page_name, "Volt|VOLT") ~ "Volt Nederland",
    str_detect(page_name, "BIJ1|BiJ") ~ "BIJ1",
    str_detect(page_name, "BVNL") ~ "BVNL",
    str_detect(page_name, "Ja21") ~ "JA21",
    str_detect(page_name, "Alliantie") ~ "Alliantie",
    str_detect(page_name, "BBB") ~ "BBB",
    T ~ NA_character_
  )) %>%
  mutate(party2 = case_when(
    str_detect(disclaimer, "VVD") ~ "VVD",
    str_detect(disclaimer, "\\bCDA\\b") ~ "CDA",
    str_detect(disclaimer, "PvdA|Jonge Socialisten") ~ "PvdA",
    str_detect(disclaimer, "D66|Jonge Democraten") ~ "D66",
    str_detect(disclaimer, "GroenLinks|GL") ~ "GroenLinks",
    str_detect(disclaimer, "ChristenUnie|CU") ~ "ChristenUnie",
    str_detect(disclaimer, "\\bSP\\b") ~ "SP",
    str_detect(disclaimer, "FvD|FVD|Forum voor Democratie") ~ "FvD",
    str_detect(disclaimer, "50Plus|50PLUS") ~ "50PLUS",
    str_detect(disclaimer, "\\bSGP\\b") ~ "SGP",
    str_detect(disclaimer, "PvdD|Partij voor de Dieren") ~ "PvdD",
    str_detect(disclaimer, "PVV") ~ "PVV",
    str_detect(disclaimer, "DENK") ~ "DENK",
    str_detect(disclaimer, "Volt|VOLT") ~ "Volt Nederland",
    str_detect(disclaimer, "BIJ1|BiJ") ~ "BIJ1",
    str_detect(disclaimer, "BVNL") ~ "BVNL",
    str_detect(disclaimer, "Ja21") ~ "JA21",
    str_detect(disclaimer, "BBB") ~ "BBB",
    T ~ NA_character_
  )) %>%
  mutate(party = ifelse(is.na(party1), party2, party1)) %>%
  drop_na(party) %>%
  distinct(page_id, .keep_all = T) %>%
  filter(str_detect(page_name, "Global Space Conference on Climate Change|de Alliantie|PvdA - GroenLinks", negate = T)) %>%
  mutate(page_id = as.character(page_id))


source("utils.R")

unlink("targeting/7", recursive = T, force = T)
unlink("targeting/30", recursive = T, force = T)

dir.create("targeting/7")
dir.create("targeting/30")

# internal_page_ids <- read_csv("data/nl_advertisers.csv") %>%
#   mutate(page_id = as.character(page_id))

internal_page_ids <- read_csv("https://raw.githubusercontent.com/favstats/ProvincialeStatenverkiezingen2023/main/data/nl_advertisers.csv") %>%
  mutate(page_id = as.character(page_id))

# internal_page_ids %>%
#     count(party, sort = T) %>% View

wtm_data <- read_csv("data/wtm-advertisers-nl-2023-07-18T21_09_43.051Z.csv") %>% #names
  select(page_id = advertisers_platforms.advertiser_platform_ref,
         page_name = name, party = entities.short_name)  %>%
  mutate(page_id = as.character(page_id)) %>%
  # filter(party == "And") %>% #View
  # count(party, sort = T)  %>%
  mutate(party = case_when(
    str_detect(party, "VVD") ~ "VVD",
    str_detect(party, "\\bCDA\\b") ~ "CDA",
    str_detect(party, "PvdA|Jonge Socialisten") ~ "PvdA",
    str_detect(party, "D66|Jonge Democraten") ~ "D66",
    str_detect(party, "GroenLinks|GL") ~ "GroenLinks",
    str_detect(party, "ChristenUnie|CU") ~ "ChristenUnie",
    str_detect(party, "\\bSP\\b") ~ "SP",
    str_detect(party, "FvD|FVD|Forum voor Democratie") ~ "FvD",
    str_detect(party, "50Plus|50PLUS") ~ "50PLUS",
    str_detect(party, "\\bSGP\\b") ~ "SGP",
    str_detect(party, "PvdD|Partij voor de Dieren") ~ "PvdD",
    str_detect(party, "PVV") ~ "PVV",
    str_detect(party, "DENK") ~ "DENK",
    str_detect(party, "Volt|VOLT") ~ "Volt Nederland",
    str_detect(party, "BIJ1|BiJ") ~ "BIJ1",
    str_detect(party, "BVNL") ~ "BVNL",
    str_detect(party, "Ja21") ~ "JA21",
    str_detect(page_name, "Alliantie") ~ "Alliantie",
    str_detect(page_name, "Partij voor de Dieren") ~ "PvdD",
    str_detect(page_name, "Christine Govaert") ~ "BBB",
    str_detect(page_name, "BVNL|Belang van Nederland") ~ "BVNL",
    T ~ party
  ))

rep <- read_csv("data/FacebookAdLibraryReport_2023-07-15_NL_last_90_days_advertisers.csv") %>% janitor::clean_names()  %>%
  mutate(page_id = as.character(page_id)) %>%
  mutate(party1 = case_when(
    str_detect(page_name, "VVD") ~ "VVD",
    str_detect(page_name, "\\bCDA\\b") ~ "CDA",
    str_detect(page_name, "PvdA|Jonge Socialisten") ~ "PvdA",
    str_detect(page_name, "D66|Jonge Democraten") ~ "D66",
    str_detect(page_name, "GroenLinks|GL") ~ "GroenLinks",
    str_detect(page_name, "ChristenUnie|CU") ~ "ChristenUnie",
    str_detect(page_name, "\\bSP\\b") ~ "SP",
    str_detect(page_name, "FvD|FVD|Forum voor Democratie") ~ "FvD",
    str_detect(page_name, "50Plus|50PLUS") ~ "50PLUS",
    str_detect(page_name, "\\bSGP\\b") ~ "SGP",
    str_detect(page_name, "PvdD|Partij voor de Dieren") ~ "PvdD",
    str_detect(page_name, "PVV") ~ "PVV",
    str_detect(page_name, "DENK") ~ "DENK",
    str_detect(page_name, "Volt|VOLT") ~ "Volt Nederland",
    str_detect(page_name, "BIJ1|BiJ") ~ "BIJ1",
    str_detect(page_name, "BVNL") ~ "BVNL",
    str_detect(page_name, "Ja21") ~ "Ja21",
    str_detect(page_name, "Alliantie") ~ "Alliantie",
    str_detect(page_name, "BBB") ~ "BBB",
    T ~ NA_character_
  )) %>%
  mutate(party2 = case_when(
    str_detect(disclaimer, "VVD") ~ "VVD",
    str_detect(disclaimer, "\\bCDA\\b") ~ "CDA",
    str_detect(disclaimer, "PvdA|Jonge Socialisten") ~ "PvdA",
    str_detect(disclaimer, "D66|Jonge Democraten") ~ "D66",
    str_detect(disclaimer, "GroenLinks|GL") ~ "GroenLinks",
    str_detect(disclaimer, "ChristenUnie|CU") ~ "ChristenUnie",
    str_detect(disclaimer, "\\bSP\\b") ~ "SP",
    str_detect(disclaimer, "FvD|FVD|Forum voor Democratie") ~ "FvD",
    str_detect(disclaimer, "50Plus|50PLUS") ~ "50PLUS",
    str_detect(disclaimer, "\\bSGP\\b") ~ "SGP",
    str_detect(disclaimer, "PvdD|Partij voor de Dieren") ~ "PvdD",
    str_detect(disclaimer, "PVV") ~ "PVV",
    str_detect(disclaimer, "DENK") ~ "DENK",
    str_detect(disclaimer, "Volt|VOLT") ~ "Volt Nederland",
    str_detect(disclaimer, "BIJ1|BiJ") ~ "BIJ1",
    str_detect(disclaimer, "BVNL") ~ "BVNL",
    str_detect(disclaimer, "Ja21") ~ "Ja21",
    str_detect(disclaimer, "BBB") ~ "BBB",
    T ~ NA_character_
  )) %>%
  mutate(party = ifelse(is.na(party1), party2, party1)) %>%
  drop_na(party) %>%
  distinct(page_id, .keep_all = T) %>%
  filter(str_detect(page_name, "Global Space Conference on Climate Change|de Alliantie|PvdA - GroenLinks", negate = T))

# 338750440106782

all_dat <- #read_csv("nl_advertisers.csv") %>%
  # mutate(page_id = as.character(page_id)) %>%
  bind_rows(internal_page_ids) %>%
  bind_rows(wtm_data) %>%
  bind_rows(rep) %>%
  bind_rows(more_data %>% mutate(source = "new")) %>%
  distinct(page_id, .keep_all = T) %>%
  add_count(page_name, sort  =T) %>%
  mutate(remove_em = n >= 2 & str_ends(page_id, "0")) %>%
  filter(!remove_em) %>%
  # filter(n >= 2) %>%
  # filter(n >= 2 & str_ends(page_id, "0", negate = T)) %>%
  select(-n)  %>%
  mutate(party = case_when(
    str_detect(party, "VVD") ~ "VVD",
    str_detect(party, "\\bCDA\\b") ~ "CDA",
    str_detect(party, "PvdA|Jonge Socialisten") ~ "PvdA",
    str_detect(party, "D66|Jonge Democraten") ~ "D66",
    str_detect(party, "GroenLinks|GL") ~ "GroenLinks",
    str_detect(party, "ChristenUnie|CU") ~ "ChristenUnie",
    str_detect(party, "\\bSP\\b") ~ "SP",
    str_detect(party, "FvD|FVD|Forum voor Democratie") ~ "FvD",
    str_detect(party, "50Plus|50PLUS") ~ "50PLUS",
    str_detect(party, "\\bSGP\\b") ~ "SGP",
    str_detect(party, "PvdD|Partij voor de Dieren") ~ "PvdD",
    str_detect(party, "PVV") ~ "PVV",
    str_detect(party, "DENK") ~ "DENK",
    str_detect(party, "Volt|VOLT") ~ "Volt Nederland",
    str_detect(party, "BIJ1|BiJ") ~ "BIJ1",
    str_detect(party, "BVNL") ~ "BVNL",
    str_detect(party, "Ja21|JA21") ~ "Ja21",
    str_detect(party, "Alliantie") ~ "Alliantie",
    str_detect(party, "BBB") ~ "BBB",
    T ~ party
  ))



scraper <- function(.x, time = "7") {
  
  # print(paste0(.x$page_name,": ", round(which(internal_page_ids$page_id == .x$page_id)/nrow(internal_page_ids)*100, 2)))
  
  yo <- get_targeting(.x$page_id, timeframe = glue::glue("LAST_{time}_DAYS")) %>%
    mutate(tstamp = tstamp)
  
  if(nrow(yo)!=0){
    path <- paste0(glue::glue("provincies/{time}/"),.x$page_id, ".rds")
    # if(file.exists(path)){
    #   ol <- read_rds(path)
    #
    #   saveRDS(yo %>% bind_rows(ol), file = path)
    # } else {
    
    saveRDS(yo, file = path)
    # }
  }
  
  # print(nrow(yo))
  # })
  return(yo)
  
}

scraper <- possibly(scraper, otherwise = NULL, quiet = F)


# if(F){
#     # dir("provincies/7", full.names
# }
# da30 <- readRDS("data/election_dat30.rds")
# da7 <- readRDS("data/election_dat7.rds")

### save seperately
yo <- all_dat %>%
  split(1:nrow(.)) %>%
  map_dfr_progress(scraper, 7)

yo <- all_dat %>% 
  split(1:nrow(.)) %>%
  map_dfr_progress(scraper, 30)

# saveRDS(yo, file = )
library(tidyverse)
da30  <- dir("provincies/30", full.names = T) %>%
  map_dfr_progress(readRDS)  %>%
  mutate(total_spend_formatted = parse_number(total_spend_formatted)) %>%
  rename(page_id = internal_id) %>%
  left_join(all_dat)

# da30 %>%
#     count(party, sort = T) %>% View


da7  <- dir("provincies/7", full.names = T) %>%
  map_dfr_progress(readRDS) %>%
  mutate(total_spend_formatted = parse_number(total_spend_formatted)) %>%
  rename(page_id = internal_id) %>%
  left_join(all_dat)

saveRDS(da30, "data/election_dat30.rds")
saveRDS(da7, "data/election_dat7.rds")
