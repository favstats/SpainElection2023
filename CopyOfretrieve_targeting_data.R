source("utils.R")
# ?get_targeting
# get_targeting("41459763029", timeframe = "LAST_90_DAYS")
# debugonce(get_targeting)

library(httr)
library(tidyverse)

tstamp <- Sys.time()

# read_csv("data/brem")

more_data <- dir("data/reports", full.names = T) %>%
  
    map_dfr(~{print(.x)
        yo <- read.csv(.x) %>% mutate(path = .x)
        return(yo)
            }) %>%
    mutate(date_produced = str_remove_all(path, "data/reports/FacebookAdLibraryReport_|_ME_last_30_days\\.csv|_ME_yesterday_advertisers|\\.csv")) %>%
    mutate(date_produced = lubridate::ymd(date_produced)) %>%
    janitor::clean_names()%>% #rename(advertiser_id = page_id) %>%
    mutate(spend = readr::parse_number(amount_spent_eur)) %>%
    mutate(spend = ifelse(spend == 100, 50, spend)) %>%
    distinct(page_id, .keep_all = T)  %>%
  mutate(party1 = case_when(
    str_detect(page_name, "\\bDrugo\\b") ~ "Drugo",
    str_detect(page_name, "\\bDCG\\b") ~ "DCG",
    str_detect(page_name, "\\bDF\\b") ~ "DF",
    str_detect(page_name, "\\bURA\\b") ~ "URA",
    str_detect(page_name, "\\bDPS\\b") ~"DPS",
    str_detect(page_name, "\\bSDP\\b") ~ "SDP",
    str_detect(page_name, "\\bUCG\\b") ~ "UCG",
    str_detect(page_name, "\\bVlada Crne Gore\\b") ~ "Vlada Crne Gore",
    str_detect(page_name, "\\bDEMOS\\b") ~ "Demos",
    str_detect(page_name, "\\bHGI\\b") ~ "HGI",
    str_detect(page_name, "\\bLP\\b") ~ "LP",
    str_detect(page_name, "\\bPrava Crna Gora\\b") ~ "Prava Crna Gora",
    T ~ NA_character_
  ))%>%
    mutate(party2 = case_when(
      str_detect(disclaimer, "\\bDrugo\\b") ~ "Drugo",
      str_detect(disclaimer, "\\bDCG\\b") ~ "DCG",
      str_detect(disclaimer, "\\bDF\\b") ~ "DF",
      str_detect(disclaimer, "\\bURA\\b") ~ "URA",
      str_detect(disclaimer, "\\bDPS\\b") ~"DPS",
      str_detect(disclaimer, "\\bSDP\\b") ~ "SDP",
      str_detect(disclaimer, "\\bUCG\\b") ~ "UCG",
      str_detect(disclaimer, "\\bVlada Crne Gore\\b") ~ "Vlada Crne Gore",
      str_detect(disclaimer, "\\bDEMOS\\b") ~ "Demos",
      str_detect(disclaimer, "\\bHGI\\b") ~ "HGI",
      str_detect(disclaimer, "\\bLP\\b") ~ "LP",
      str_detect(disclaimer, "\\bPrava Crna Gora\\b") ~ "Prava Crna Gora",
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

# rawadvertisers <- read_csv("data/advertisers - advertisers.csv")  %>%
#   mutate(party_lab = case_when(
#     str_detect(advertiser_name, "VVD") ~ "VVD",
#     str_detect(advertiser_name, "\\bCDA\\b") ~ "CDA",
#     str_detect(advertiser_name, "PvdA|Jonge Socialisten") ~ "PvdA",
#     str_detect(advertiser_name, "D66|Jonge Democraten") ~ "D66",
#     str_detect(advertiser_name, "GroenLinks") ~ "GroenLinks",
#     str_detect(advertiser_name, "ChristenUnie") ~ "ChristenUnie",
#     str_detect(advertiser_name, "\\bSP\\b") ~ "SP",
#     str_detect(advertiser_name, "FvD|FVD|Forum voor Democratie") ~ "FvD",
#     str_detect(advertiser_name, "50PLUS") ~ "50PLUS",
#     str_detect(advertiser_name, "\\bSGP\\b") ~ "SGP",
#     str_detect(advertiser_name, "PvdD|Partij voor de Dieren") ~ "PvdD",
#     str_detect(advertiser_name, "PVV") ~ "PVV",
#     str_detect(advertiser_name, "DENK") ~ "DENK",
#     str_detect(advertiser_name, "Volt") ~ "Volt Nederland",
#     str_detect(advertiser_name, "BIJ1") ~ "BIJ1",
#     str_detect(advertiser_name, "BVNL") ~ "BVNL",
#     str_detect(advertiser_name, "Ja21") ~ "Ja21",
#     T ~ ""
#   ))



# internal_page_ids <- read_csv("data/nl_advertisers.csv") %>%
#   mutate(page_id = as.character(page_id))

# internal_page_ids <- read_csv("https://raw.githubusercontent.com/favstats/ProvincialeStatenverkiezingen2023/main/data/nl_advertisers.csv") %>%
#     mutate(page_id = as.character(page_id))

# internal_page_ids %>%
#     count(party, sort = T) %>% View
wtm_data <- read_csv("data/wtm-advertisers-gr-2023-06-11T21_31_07.105Z.csv") %>% #names
    select(page_id = advertisers_platforms.advertiser_platform_ref,
           page_name = name, party = entities.short_name)  %>%
    mutate(page_id = as.character(page_id)) 


wtm_data <- wtm_data %>% 
  mutate(party = case_when(
    str_detect(party, "ΠΑΣΟΚ")  ~ "PASOK",
    str_detect(party, "ΝΔ")  ~ "ND",
    str_detect(party, "Συνασπισμός Ριζοσπαστικής Αριστεράς")  ~ "SYRIZA",
    str_detect(party, "ΕΛ")  ~ "EL",
    str_detect(party, "25")  ~ "MeRA25",
    T ~ party  )) %>% 
  mutate(party = case_when(
    str_detect(page_name, "Νίκος Συρμαλένιος") ~ "SYRIZA",
    # str_detect(page_name, "Stefanos Tyros") ~ "Volt",
    # str_detect(page_name, "Πατριωτική Ένωση - Πρόδρομος Εμφιετζόγλου") ~ "Patriotic Union",
    T ~ party 
  )) %>% 
  filter(party %in% c("EL", "ND", "PASOK", "MeRA25", "SYRIZA"))


wtm_data %>% count(party, sort = T) #%>% pull(party) %>% clipr::write_clip()

    # filter(party == "And") %>% #View
  #   # count(party, sort = T)  %>%
  # mutate(party = case_when(
  #   str_detect(party, "\\bKOK\\b") ~ "KOK",
  #   str_detect(party, "\\bVIH\\b") ~ "VIH",
  #   str_detect(party, "\\bVAS\\b") ~ "VAS",
  #   str_detect(party, "\\bPER\\b") ~ "PER",
  #   str_detect(party, "\\bMUUT\\b") ~ "MUUT",
  #   str_detect(party, "\\bKES\\b") ~ "KES",
  #   str_detect(party, "\\bSDP\\b") ~ "SDP",
  #   str_detect(party, "\\bRKP\\b") ~ "KD",
  #   str_detect(party, "\\bKOR\\b") ~ "KOR",
  #   T ~ party
  # )) #%>% #View
    # count(party, sort = T)

# wtm_data %>% 
#   filter(party == "And") %>% View

# wtm_data %>% count(party)

# 338750440106782

all_dat <- wtm_data %>%
    bind_rows(more_data) %>% 
    distinct(page_id, .keep_all = T) %>%
    add_count(page_name, sort  =T) %>%
    mutate(remove_em = n >= 2 & str_ends(page_id, "0")) %>%
    filter(!remove_em) %>%
    # filter(n >= 2) %>%
    # filter(n >= 2 & str_ends(page_id, "0", negate = T)) %>%
    select(-n)  

all_dat %>% count(party, sort = T)
all_dat %>% nrow

write_lines(all_dat %>% count(page_id, sort = T) %>% nrow, "n_advertisers.txt")

# all_dat %>% filter(source == "new") %>% View
# 
# more_data %>% 
#   select(-party) %>% 
#   left_join(wtm_data %>% select(page_id, party)) %>% 
#   openxlsx::write.xlsx("data/tolabel.xlsx")


# all_dat %>%
#     filter(party == "And") %>% View
#     count(party, sort = T) %>% View

#
# all_dat %>% View
#     filter(page_id == 1519997944789250)
#
# all_dat %>%
#     add_count(page_name, sort  =T) %>%
#     filter(n >= 2) %>% View
#     filter(str_ends(page_id, "0", negate = T)) %>% View

# all_dat %>%
#     mutate(source = "already_there") %>%
#     # filter(str_detect(party, "FvD")) %>% View
#     bind_rows(more_data) %>%
#     distinct(page_id, .keep_all = T) %>%
#     # filter(page_id == 609816282477420) %>%
#     # filter(page_id == 609816282477420) %>% View
#
#     filter(is.na(source)) %>% View

# all_dat %>%
#     count(party, sort  =T)

# all_dat %>%
#     bind_rows(rep %>% select(page_name, page_id, disclaimer, party))  %>%
#     distinct(page_id, .keep_all = T) %>%
#     filter(!(page_id %in% all_dat$page_id)) %>%
#     filter(str_detect(page_name, "Global Space Conference on Climate Change|de Alliantie|PvdA - GroenLinks", negate = T)) %>% View

# all_dat %>% filter(str_detect(page_name, "BBB")) %>% View

# write_csv(all_dat, file = "data/me_advertisers.csv")

# janitor::clean_names() %>%
# arrange(desc(amount_spent_usd)) %>%
# mutate(spend_upper = amount_spent_usd %>% as.numeric()) %>%
# arrange(-spend_upper) %>%
# mutate_all(as.character)C

# internal_page_ids %>% count(party, sort =T) %>% slice(11:17)
#
# internal_page_ids %>%
#   filter(party == "Politiek Op Maat")
#
# rawadvertisers %>%
#   # filter(category == "Political Organization") %>% View
#   # filter(str_detect(category, "Party|Politician|Candidade")) %>%
#   rename(page_id = advertiser_id) %>%
#   select(page_id, page_name = advertiser_name, party = party_lab)
#   left_join(internal_page_ids) %>%
#   # drop_na(party) %>%
#   filter(!is.na(party) | party_lab != "") %>%
#   # filter(party == "PvdA" & party_lab == "")
#   count(party, party_lab, sort = T)  %>% View
#
#
#
#   internal_page_ids %>%
#     bind_rows(
#       rawadvertisers %>%
#         rename(page_id = advertiser_id) %>%
#         select(page_id, page_name = advertiser_name, party = party_lab) %>%
#         filter(party != "") %>%
#         filter(str_starts(page_id, "AR", negate = T)) %>%
#         mutate(source = "yo")
#     ) %>%
#     distinct(page_id, .keep_all = T) %>%
#     write_csv("data/nl_advertisers.csv")


# georgia_wtm <- readr::read_csv("data/wtm-advertisers-us-2022-11-28T14_22_01.338Z.csv") %>%
#   select(page_name = name,
#          page_id = advertisers_platforms.advertiser_platform_ref) %>%
#   mutate(page_id = as.character(page_id))

# options(scipen = 999999)

# georgia_wtm

# internal_page_ids <- georgia_wtm %>%
#   mutate_all(as.character) %>%
#   bind_rows(last90days)  %>%
#   distinct(page_id, .keep_all = T)

# get_targeting(internal_page_ids$page_id[1], timeframe = "LAST_30_DAYS")
# debugonce(get_targeting)
# get_targeting("121264564551002", timeframe = "LAST_30_DAYS")
# get_targeting("1420588184916754", timeframe = "LAST_30_DAYS")



scraper <- function(.x, time = "7") {

  # print(paste0(.x$page_name,": ", round(which(internal_page_ids$page_id == .x$page_id)/nrow(internal_page_ids)*100, 2)))

  yo <- get_targeting(.x$page_id, timeframe = glue::glue("LAST_{time}_DAYS")) %>%
    mutate(tstamp = tstamp)

  if(nrow(yo)!=0){
    path <- paste0(glue::glue("targeting/{time}/"),.x$page_id, ".rds")
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

}

scraper <- possibly(scraper, otherwise = NULL, quiet = F)


# if(F){
#     # dir("targeting/7", full.names
# }
# da30 <- readRDS("data/election_dat30.rds")
# da7 <- readRDS("data/election_dat7.rds")

already_there <- dir("targeting/7", full.names = T) %>% 
  str_remove_all("targeting/7/|\\.rds")

### save seperately
yo <- all_dat %>% #count(cntry, sort  =T) %>%
    # filter(!(page_id %in% already_there)) %>%
  # filter(!(page_id %in% unique(da7$page_id))) %>%
  # filter(cntry == "GB") %>%
  # slice(1:10) %>%
  split(1:nrow(.)) %>%
  map_dfr_progress(scraper, 7)

already_there <- dir("targeting/30", full.names = T) %>% 
  str_remove_all("targeting/30/|\\.rds")

yo <- all_dat %>% #count(cntry, sort  =T) %>%
    # filter(!(page_id %in% already_there)) %>%
    # filter(!(page_id %in% unique(da30$page_id))) %>%
    # filter(cntry == "GB") %>%
    # slice(1:10) %>%
    split(1:nrow(.)) %>%
    map_dfr_progress(scraper, 30)

# saveRDS(yo, file = )
library(tidyverse)
da30  <- dir("targeting/30", full.names = T) %>%
  map_dfr_progress(readRDS)  %>%
    mutate(total_spend_formatted = parse_number(total_spend_formatted)) %>%
    rename(page_id = internal_id) %>%
    left_join(all_dat) 



# da30 %>%
#     count(party, sort = T) %>% View


da7  <- dir("targeting/7", full.names = T) %>%
    map_dfr_progress(readRDS) %>%
    mutate(total_spend_formatted = parse_number(total_spend_formatted)) %>%
    rename(page_id = internal_id) %>%
    left_join(all_dat)


saveRDS(da30, "data/election_dat30.rds")
saveRDS(da7, "data/election_dat7.rds")

# da30 %>%
#   # filter(party == "OTHER") %>% 
#   # # distinct(page_id, page_name) %>% 
#   # dput()
#   count(party)
# da7 %>% count(party)
# da30 %>% count(page_id)
# da30 %>% count(page_id)
# da7 %>% count(ds)
# 
# da30 %>% 
#   # filter(party == "Diğ") 
#   filter(str_detect(page_name, "\\bDEVA\\b|\\bDeva\\b"))

# bbb %>% filter(str_detect(funding_, "Strijker"))

# da7 %>%
#   distinct(internal_id, .keep_all = T) %>%
#   mutate(total_spend = parse_number(total_spend_formatted)) %>%
#   rename(page_id = internal_id) %>%
#   left_join(internal_page_ids) %>%
#   group_by(party) %>%
#   summarize(total_spend = sum(total_spend))
#
#
# amgna <- da7 %>%
#   mutate(total_spend = parse_number(total_spend_formatted)) %>%
#   rename(page_id = internal_id) %>%
#   left_join(internal_page_ids)
#
#
# amgna %>%
#   filter(type == "gender") %>%
#   filter(value == "Women") %>%
#   # mutate(total_spend = total_spend*total_spend_pct) %>%
#   ggplot(aes(party, total_spend_pct)) +
#   geom_boxplot() #+
#   # scale_y_log10()
#
#
#
# amgna %>%
#   filter(type == "detailed")

