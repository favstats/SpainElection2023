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

latest_ds <- latest_elex %>% arrange(ds) %>% slice(1) %>% pull(ds)



tstamp <- Sys.time()

write_lines(lubridate::as_date(tstamp), "tstamp.txt")

# - name: Set timeframe 
# run: |
#   echo "::set-env name=TIMEFRAME::30 Timeframe"




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


internal_page_ids <- read_csv("data/nl_advertisers.csv") %>%
  mutate(page_id = as.character(page_id))


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
      mutate(tstamp = tstamp)
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
  
  ### save seperately
  enddat <- all_dat %>% 
    arrange(page_id) %>%
    # slice(1:150) %>% 
    filter(!(page_id %in% latest_elex$page_id)) %>% 
    split(1:nrow(.)) %>%
    map_dfr(scraper)
  
  if(nrow(enddat)==0){
    election_dat <- latest_elex
  } else {
    election_dat  <- enddat %>%
      mutate(total_spend_formatted = parse_number(as.character(total_spend_formatted))) %>%
      rename(page_id = internal_id) %>%
      left_join(all_dat) %>% 
      bind_rows(latest_elex)    
    
    current_date <- paste0("historic/",  as.character(new_ds), "/", tf)
    
    saveRDS(election_dat, file = paste0(current_date, ".rds"))
  }
  

  } else {
  
  ### save seperately
  election_dat <- all_dat %>% 
    arrange(page_id) %>%
    # slice(1:50) %>% 
    split(1:nrow(.)) %>%
    map_dfr(scraper)  %>%
    mutate(total_spend_formatted = parse_number(total_spend_formatted)) %>%
    rename(page_id = internal_id)  
  
  dir.create(paste0("historic/",  as.character(new_ds)), recursive = T)
  current_date <- paste0("historic/",  as.character(new_ds), "/", tf)
  
  saveRDS(election_dat, file = paste0(current_date, ".rds"))
  
  
}

saveRDS(election_dat, paste0("data/election_dat", tf, ".rds"))

##### combinations ####


minimum_date <- dir("historic", recursive = T) %>%
  keep(~str_detect(.x, paste0(tf, "\\.rds"))) %>% 
  str_remove("/.*") %>%
  as.Date() %>%
  min(na.rm = T)

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
  
  saveRDS(combined_dat, data = paste0("data/combined_dat", tf,  ".rds"))
  
  aggr <- combined_dat  %>%
    mutate(total_spend = readr::parse_number(total_spend_formatted)) %>%
    mutate(total_spend = ifelse(total_spend == 50, 50, total_spend)) %>%
    mutate(total_spend = total_spend * total_spend_pct) %>%
    group_by(internal_id, value, type, location_type, detailed_type, custom_audience_type, is_exclusion) %>%
    summarize(total_spend = sum(total_spend),
              num_ads = sum(num_ads),
              num_obfuscated = sum(num_obfuscated)) %>%
    ungroup()
  
  saveRDS(aggr, data = paste0("data/election_dat_aggr", tf,  ".rds"))
  

  
  
}



if(new_ds == latest_ds){
  
  unlink(paste0("targeting/", tf), recursive = T, force = T)
  
  dir.create(paste0("targeting/", tf))
  
  write_lines("_", paste0("targeting/", tf, "/", "_"))
  
}