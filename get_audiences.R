# Get command-line arguments
# tf <- commandArgs(trailingOnly = TRUE)

tf <- "30"

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

source("utils.R")
# ?get_targeting
# get_targeting("41459763029", timeframe = "LAST_90_DAYS")
# debugonce(get_targeting)

library(httr)
library(tidyverse)
library(lubridate)

# tf <- Sys.getenv("TIMEFRAME")
# # tf <- "7"
# print(tf)

jb <- get_targeting("7860876103", timeframe = glue::glue("LAST_90_DAYS"))

new_ds <- jb %>% arrange(ds) %>% slice(1) %>% pull(ds)

latest_elex <- readRDS(paste0("data/election_dat", tf, ".rds"))

if("ds" %in% names(latest_elex) ){
  
latest_ds <- latest_elex %>% arrange(ds) %>% slice(1) %>% pull(ds)

} else {
  latest_ds <- as_date(new_ds)-1
}

tstamp <- Sys.time()

write_lines(lubridate::as_date(tstamp), "tstamp.txt")

# - name: Set timeframe 
# run: |
#   echo "::set-env name=TIMEFRAME::30 Timeframe"




# tstamp <- Sys.time()

# last90days <- read_csv("data/FacebookAdLibraryReport_2023-07-20_ES_last_90_days_advertisers.csv")
last30days <- read_csv("data/FacebookAdLibraryReport_2023-07-20_ES_last_30_days_advertisers.csv")

wtm_data <- read_csv("data/wtm-advertisers-es-2023-07-22T22_31_30.452Z.csv") %>% #names
  select(page_id = advertisers_platforms.advertiser_platform_ref,
         page_name = name, party = entities.short_name)  %>%
  mutate(page_id = as.character(page_id)) %>% 
  filter(page_id %in% last30days$`Page ID`)
# filter(party == "And") %

all_dat <-   bind_rows(wtm_data) %>%
  #read_csv("nl_advertisers.csv") %>%
  # mutate(page_id = as.character(page_id)) %>%
  # bind_rows(internal_page_ids) %>%
  # bind_rows(rep) %>%
  # bind_rows(more_data %>% mutate(source = "new")) %>%
  distinct(page_id, .keep_all = T) %>%
  add_count(page_name, sort  =T) %>%
  mutate(remove_em = n >= 2 & str_ends(page_id, "0")) %>%
  filter(!remove_em) 



scraper <- function(.x, time = tf) {
  
  # print(paste0(.x$page_name,": ", round(which(internal_page_ids$page_id == .x$page_id)/nrow(internal_page_ids)*100, 2)))
  
  fin <- get_targeting(.x$page_id, timeframe = glue::glue("LAST_{time}_DAYS")) %>%
    mutate(tstamp = tstamp)
  
  if(nrow(fin)!=0){
    path <- paste0(glue::glue("targeting/{time}/"),.x$page_id, ".rds")
    # if(file.exists(path)){
    #   ol <- read_rds(path)
    #
    #   saveRDS(fin %>% bind_rows(ol), file = path)
    # } else {
    
    saveRDS(fin, file = path)
    # }
  } else {
   fin <- tibble(internal_id = .x$page_id, no_data = T) %>%
      mutate(tstamp = tstamp) %>% 
     mutate(total_spend_formatted = NA)
  }
  
  # print(nrow(fin))
  # })
  return(fin)
  
}

scraper <- possibly(scraper, otherwise = NULL, quiet = F)


# if(F){
#     # dir("provincies/7", full.names
# }
# da30 <- readRDS("data/election_dat30.rds")
# da7 <- readRDS("data/election_dat7.rds")

if(new_ds == latest_ds){
  print(glue::glue("New DS: {new_ds}: Old DS: {latest_ds}"))
  
  ### save seperately
  enddat <- all_dat %>% 
    arrange(page_id) %>%
    # slice(1:50) %>%
    # sample_n(509) %>%
    filter(!(page_id %in% latest_elex$page_id)) %>% 
    split(1:nrow(.)) %>%
    map_dfr_progress(scraper)
  
  glimpse(enddat)
  
  if(nrow(enddat)==0){
    election_dat <- latest_elex
  } else {
    election_dat  <- enddat %>%
      mutate_at(vars(contains("total_spend_formatted")), ~parse_number(as.character(.x))) %>% 
      # mutate(total_spend_formatted = parse_number(as.character(total_spend_formatted))) %>%
      rename(page_id = internal_id) %>%
      left_join(all_dat) %>% 
      bind_rows(latest_elex)    
    
    current_date <- paste0("historic/",  as.character(new_ds), "/", tf)
    
    saveRDS(election_dat, file = paste0(current_date, ".rds"))
  }
  

  } else {
    
    print(glue::glue("New DS: {new_ds}: Old DS: {latest_ds} 2"))
    
  
  ### save seperately
  election_dat <- all_dat %>% 
    arrange(page_id) %>%
    # slice(1:50) %>%
    # sample_n(509) %>%
    split(1:nrow(.)) %>%
    map_dfr_progress(scraper)  
  
  glimpse(election_dat)
  
  election_dat <- election_dat %>% 
    mutate_at(vars(contains("total_spend_formatted")), ~parse_number(as.character(.x))) %>% 
    # mutate(total_spend_formatted = parse_number(as.character(total_spend_formatted))) %>%
    rename(page_id = internal_id)  
  
  dir.create(paste0("historic/",  as.character(new_ds)), recursive = T)
  current_date <- paste0("historic/",  as.character(new_ds), "/", tf)
  
  saveRDS(election_dat, file = paste0(current_date, ".rds"))
  
  election_dat <- election_dat%>% left_join(all_dat)
  }



saveRDS(election_dat, paste0("data/election_dat", tf, ".rds"))

##### combinations ####


minimum_date <- dir("historic", recursive = T) %>%
  keep(~str_detect(.x, paste0(tf, "\\.rds"))) %>% 
  str_remove("/.*") %>%
  as.Date() %>%
  min(na.rm = T)

if("ds" %in% names(election_dat) ){
  
  latest_ds <- election_dat %>% arrange(ds) %>% slice(1) %>% pull(ds) %>% as.Date()
  
  begintf <- as.Date(latest_ds) - lubridate::days(tf)
  
  date_vector <- vector()
  current_date <- latest_ds
  index <- 1
  
  while(current_date > minimum_date) {
    
    date_vector[index] <- current_date
    
    current_date <- current_date - lubridate::days(tf)
    
    index <- index + 1
    
  }
  
  if(length(date_vector != 0)){
    
    
    
    combined_dat <- paste0("historic/", as_date(date_vector), "/", tf, ".rds") %>%
      map_dfr(~{
        if(!file.exists(.x)){
          return(tibble(ds = as.character(begintf), missing_report = T))
        } else {
          readRDS(.x)
        }
        
      })
    
    saveRDS(combined_dat, file= paste0("data/combined_dat", tf,  ".rds"))
    
    if("total_spend_formatted" %in% names(combined_dat) ){
      
    
    aggr <- combined_dat  %>%
      mutate_at(vars(contains("total_spend_formatted")), ~parse_number(as.character(.x))) %>% 
      # mutate(total_spend = readr::parse_number(as.character(total_spend_formatted))) %>%
      mutate(total_spend = ifelse(total_spend_formatted == 50, 50, total_spend_formatted)) %>%
      mutate(total_spend = total_spend * total_spend_pct) %>%
      group_by(page_id, value, type, location_type, detailed_type, custom_audience_type, is_exclusion) %>%
      summarize(total_spend = sum(total_spend),
                num_ads = sum(num_ads),
                num_obfuscated = sum(num_obfuscated)) %>%
      ungroup()
    
    saveRDS(aggr, file = paste0("data/election_dat_aggr", tf,  ".rds"))
    
    }
    
  }
  
  
  
  if(new_ds == latest_ds){
    
    unlink(paste0("targeting/", tf), recursive = T, force = T)
    
    dir.create(paste0("targeting/", tf))
    
    write_lines("_", paste0("targeting/", tf, "/", "_"))
    
  }
}
