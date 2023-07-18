### ---- Load Packages and Functions  ####


pacman::p_load(tidyverse, janitor, highcharter, httr, furrr, lubridate, tidytext)

setwd(here::here())

color_dat <- tibble(colors = c("#5493ce", "#1b5cc7", "#01783d", "#ef3f24", "#ee808f"),
                    party = c("EL", "ND", "PASOK", "MeRA25", "SYRIZA"))

if(!dir.exists("data")) dir.create("data")

get_mid <- function(spend_upper_bound, spend_lower_bound) {
  # (spend_upper_bound-spend_lower_bound)/2+spend_lower_bound
  (spend_upper_bound+spend_lower_bound)/2
}



wtm_data <- read_csv("data/wtm-advertisers-gr-2023-06-11T21_31_07.105Z.csv") %>% #names
  select(advertiser_id = advertisers_platforms.advertiser_platform_ref,
         advertiser_name = name, party = entities.short_name)  %>%
  mutate(advertiser_id = as.character(advertiser_id))  %>% 
  mutate(party = case_when(
    str_detect(party, "ΠΑΣΟΚ")  ~ "PASOK",
    str_detect(party, "ΝΔ")  ~ "ND",
    str_detect(party, "Συνασπισμός Ριζοσπαστικής Αριστεράς")  ~ "SYRIZA",
    str_detect(party, "ΕΛ")  ~ "EL",
    str_detect(party, "25")  ~ "MeRA25",
    T ~ party  ))
  # mutate(party = case_when(
  #   str_detect(advertiser_id, "638633769597292") ~ "ASh",
  #   T ~ party 
  # )) %>% 
  # filter(!(party %in% c("Vlada Crne Gore", "Drugo"))) %>% 
  # left_join(party_dict) %>% 
  # mutate(party = coalition)


assign_colors <- function(dat, n = 12) {
  
  color_sample <- colorspace::divergingx_hcl(n)
  
  lenght <- dat$color[is.na(dat$color)] %>% length
  
  if(lenght==0) return(invisible())
  
  cols <- sample(color_sample, lenght, replace = T)
  
  dat$color[is.na(dat$color)] <- cols
  
  return(dat)
  
}

unnest_geos <- function(x) {
  # cat(glue::glue("{x$row_number} out of {nrow(age_gender_targeted_raw)} ({round(100*x$row_number/nrow(age_gender_targeted_raw), 2)}%)\n\n"))
  x %>% 
    dplyr::pull(region_distribution) %>% 
    flatten() %>% 
    map_dfr(flatten) %>% 
    mutate(id = x$id)%>% 
    mutate(start_time = x$start_time)%>% 
    mutate(advertiser_name = x$advertiser_name)
}

unnest_dems <- function(x) {
  # cat(glue::glue("{x$row_number} out of {nrow(age_gender_targeted_raw)} ({round(100*x$row_number/nrow(age_gender_targeted_raw), 2)}%)\n\n"))
  x %>% 
    dplyr::pull(demographic_distribution) %>% 
    flatten() %>% 
    map_dfr(flatten) %>% 
    mutate(id = x$id)
}


last_updated_time <- as.character(Sys.time())

### ---- Get Facebook data  ####


cat("\n\nFB Data: Getting it\n\n")  


# get_fb_ads <- function() {

# readRenviron(".Renviron")

token <- Sys.getenv("fb_token")

#link to fb api
my_link<- "https://graph.facebook.com"

#define fields you are interested in
search_fields=c("ad_creation_time", 
                "ad_delivery_start_time",
                "ad_delivery_stop_time",
                "ad_creative_link_caption",
                "ad_creative_link_description",
                "ad_creative_link_title",
                "currency",
                "ad_creative_body", 
                "page_id",
                "page_name",
                "spend",
                "ad_snapshot_url",
                "demographic_distribution",
                "funding_entity",
                "potential_reach",
                "publisher_platforms",
                "impressions",
                "region_distribution") %>% 
  stringr::str_c(., collapse=", ")

min_date <- "2023-05-26"

#get the data from the first 'page' of data the api provides
page_one_response <- GET(my_link,
                         path = "/ads_archive",
                         query = list(access_token = token,
                                      limit=100,
                                      ad_active_status="ALL",
                                      search_terms="''",
                                      ad_delivery_date_min = min_date,
                                      fields=search_fields,
                                      ad_reached_countries="GR"))
page_one_content<- content(page_one_response)

x <- tibble(data=page_one_content$data)
df_imp <- x %>% 
  unnest_wider(data) 

#get the link refering to the next page
next_link <- page_one_content$paging$`next`

page <- 1

#iterate over all pages until there is no further page
while(length(next_link)>0) {
  # while(T) {
  
  print(page)
  
  next_response <- GET(next_link)
  next_content<- content(next_response)
  
  y <- tibble(data=next_content$data)
  df_next <- y %>% 
    unnest_wider(data) 
  
  df_imp <- bind_rows(df_imp, df_next)  
  
  next_link <- next_content$paging$`next`
  
  page <- page + 1
  
}

# dutch_parties <- c("VVD", "D66", "FvD", "SP", "GroenLinks", "Volt Nederland", "PvdA", "CDA", "PvdD", "ChristenUnie", "SGP", "DENK", "50PLUS")

cat("\n\nFB Data: Read in old data\n\n")  

fb_dat <- readRDS("data/fb_dat.rds")


# fb_dat <- fb_dat %>% 
#   mutate(advertiser_id = ifelse(is.na(advertiser_id), page_id, advertiser_id))


# saveRDS(df_imp, "data/df_imp.rds")
# df_imp <- readRDS("data/df_imp.rds")

cat("\n\nFB Data: Merge data\n\n")  


fb_dat <- df_imp %>% 
  rename(advertiser_name = page_name) %>% 
  rename(advertiser_id = page_id) %>% 
  bind_rows(fb_dat %>% select(-party)) %>%
  distinct(id, .keep_all = T) %>%
  left_join(wtm_data %>% select(-advertiser_name)) #%>% 
  # mutate(advertiser_name = case_when(
  #   advertiser_name == 'Partij voor de Dieren' ~ "PvdD",
  #   advertiser_name == 'Partij van de Arbeid (PvdA)' ~ "PvdA",
  #   advertiser_name == 'Forum voor Democratie -FVD' ~ "FvD",
  #   advertiser_name == '50PLUSpartij' ~ "50PLUS",
  #   T ~ advertiser_name
  # ))

saveRDS(fb_dat, "data/fb_dat.rds")

# fb_dat %>% dplyr::filter(advertiser_name == "BIJ1")

# saveRDS(fb_dat, "fb_dat/fb_dat_old.rds")


cat("\n\nFB Data: Save data\n\n")  

fb_dat <- readRDS("data/fb_dat.rds")

rm(df_imp)
gc()

cat("\n\nGarbage collected\n\n")  


# rstudioapi::jobRunScript("get_fb.R")


# fb_dat_parties <- fb_dat %>% 
#   mutate(ad_delivery_start_time = as.Date(ad_delivery_start_time)) %>% 
#   filter(ad_delivery_start_time >= as.Date("2020-09-01")) %>% 
#   filter(advertiser_name %in% dutch_parties) 
# 
# saveRDS(fb_dat_parties, "fb_dat/fb_dat_parties.rds")

#   return(fb_dat)
#   
# }

# fb_ads <- get_fb_ads()

# color_dat <- tibble(
#   color = c("#00b13d", "#80c31c", "#cd503e", "#008067", "#6f2421", "#e3101c", "#e01003", "#036b2c", "#02a6e9", "#562883", "#eeaa00", "#34c1c4", "#92107d", "#202122", "#242b57"),
#   advertiser_name = c("D66", "GroenLinks", "VVD", "CDA", "FvD", "PvdA", "SP", "PvdD", "ChristenUnie", "Volt Nederland", "SGP", "DENK", "50PLUS", "BIJ1", "Ja21"))

cat("\n\nFB Data: Get totals\n\n")  


total_times <- fb_dat %>% 
  mutate(date_range_start = as.Date(ad_delivery_start_time)) %>%
  filter(date_range_start >= as.Date(min_date)) %>% 
  unnest_wider(spend, names_sep = "_") %>%
  unnest_wider(impressions, names_sep = "_") %>% 
  unnest_wider(potential_reach , names_sep = "_") %>%
  mutate_at(vars(spend_lower_bound, spend_upper_bound, 
                 impressions_lower_bound, impressions_upper_bound, 
                 potential_reach_lower_bound, potential_reach_upper_bound), as.numeric) %>% 
  # drop_na(spend_lower_bound, spend_upper_bound, impressions_lower_bound, impressions_upper_bound) %>% 
  mutate(impressions_lower_bound = case_when(
    # is.na(impressions_upper_bound) ~ 0, 
    # is.na(impressions_lower_bound) ~ 0,
    impressions_lower_bound == 0 ~ 1, 
    T ~ impressions_lower_bound)) %>% 
  mutate(spend_lower_bound = case_when(
    spend_lower_bound == 0 ~ 1, 
    T ~ spend_lower_bound)) %>% 
  drop_na(impressions_lower_bound, impressions_upper_bound)  
# batch_id_dat <- total_times  %>% 
#   # filter(is.na(advertiser_id)) %>% View
#   mutate(unique_advertiser_id = as.numeric(as.factor(advertiser_name))) %>% 
#   group_by(ad_creative_body, ad_creative_link_title, advertiser_name, advertiser_id) %>% 
#   mutate(batch_id = paste0(unique_advertiser_id, "_", n(), "_", sample(10000:10000000000, size = 1)))%>% 
#   ungroup() %>% 
#   select(id, batch_id)

facebook_id_dat <- total_times  %>% 
  distinct(advertiser_name, .keep_all = T) %>% 
  select(advertiser_name, advertiser_id)

# total_times %>% filter(advertiser_name == "AeroTime") %>% View

fb_total <- total_times  %>% 
  # left_join(batch_id_dat) %>% 
  group_by(party) %>% 
  summarise(potential_reach_min = sum(potential_reach_lower_bound),
            # potential_reach_max = median(potential_reach_upper_bound) ,
            # potential_reach_mid = median(get_mid(potential_reach_lower_bound, potential_reach_upper_bound)),
            spend_range_min = sum(spend_lower_bound) ,
            spend_range_max = sum(spend_upper_bound) ,
            spend_range_mid = sum(get_mid(spend_lower_bound, spend_upper_bound)) ,
            impressions_range_min = sum(impressions_lower_bound) ,
            impressions_range_max = sum(impressions_upper_bound) ,
            impressions_range_mid = sum(get_mid(impressions_lower_bound, impressions_upper_bound)),
            n_ids = n()) %>% 
  ungroup() %>% 
  left_join(color_dat)
# left_join(total_spend_fb)

cat("\n\nFB Data: Get times\n\n")  


# tidytemplate::save_it(fb_total)

fb_times <- total_times %>% 
  # left_join(batch_id_dat) %>% 
  # group_by(batch_id) %>% 
  # mutate(date_range_start = min(date_range_start)) %>% 
  # ungroup() %>% 
  group_by(party, date_range_start) %>% 
  # group_by(date_range_start, ad_creative_body, ad_creative_link_title, advertiser_name) %>% 
  summarise(potential_reach_min = sum(potential_reach_lower_bound) ,
            # potential_reach_max = sum(potential_reach_upper_bound) ,
            # potential_reach_mid = sum(get_mid(potential_reach_lower_bound, potential_reach_upper_bound)),
            spend_range_min = sum(spend_lower_bound) ,
            spend_range_max = sum(spend_upper_bound) ,
            spend_range_mid = sum(get_mid(spend_lower_bound, spend_upper_bound)) ,
            impressions_range_min = sum(impressions_lower_bound) ,
            impressions_range_max = sum(impressions_upper_bound) ,
            impressions_range_mid = sum(get_mid(impressions_lower_bound, impressions_upper_bound)) ,
            n_ids = n()) %>% 
  ungroup() %>% 
  complete(party, date_range_start = seq.Date(min(date_range_start), max(date_range_start), by="day"), fill = list(n = 0, spend_range_min = 0, spend_range_mid = 0, spend_range_max = 0, impressions_range_min = 0, impressions_range_mid = 0, impressions_range_max = 0, potential_reach_min = 0)) %>% 
  filter(party != 0) %>% 
  left_join(color_dat)

cat("\n\nFB Data: Get age/gender I\n\n")  


age_gender_targeted_raw <- fb_dat %>% 
  mutate(start_time = lubridate::as_datetime(ad_delivery_start_time) %>% lubridate::floor_date("day")) %>% 
  mutate(start_time = as.Date(start_time)) %>% 
  mutate(date_range_start = as.Date(ad_delivery_start_time)) %>%
  filter(date_range_start >= as.Date(min_date)) 

cat("\n\nFB Data: Get age/gender II\n\n")  

unnest_dems <- possibly(unnest_dems, otherwise = NULL, quiet = F)

age_gender_targeted <- age_gender_targeted_raw %>% 
  # slice(1) %>% 
  mutate(row_number = 1:n()) %>% 
  split(1:nrow(.)) %>% 
  map_dfr(unnest_dems) %>% 
  right_join(age_gender_targeted_raw)


cat("\n\nFB Data: Get age/gender III\n\n")  

### Facebook setup
# dutch_parties_fb <- c("VVD", "D66", "FvD", "SP", "GroenLinks", "Volt Nederland", "PvdA", "CDA", "PvdD", "ChristenUnie", "SGP", "DENK", "50PLUS", "BIJ1", "Ja21")


fb_gender <- age_gender_targeted  %>% 
  # filter(advertiser_id %in% dutch_parties_fb) %>%
  mutate(percentage = as.numeric(percentage)) %>% 
  mutate(gender_age = paste(gender, age)) %>% 
  filter(!str_detect(gender_age, "unknown")) %>% 
  filter(!str_detect(gender_age, "13-17")) %>% 
  filter(!str_detect(gender_age, "All")) %>% 
  drop_na(gender) %>% 
  group_by(advertiser_id) %>% 
  complete(id, gender, age, fill = list(percentage = 0)) %>% 
  ungroup() %>% 
  group_by(id, gender, advertiser_id) %>% 
  summarize(percentage = sum(percentage)) %>% 
  ungroup() #%>% 
  # mutate(percentage = round(percentage * 100, 2)) %>% 
  # left_join(facebook_id_dat)  #%>% 
# filter(advertiser_name == "PvdA")

cat("\n\nFB Data: Get age/gender IV\n\n")  



fb_age <- age_gender_targeted  %>% 
  mutate(percentage = as.numeric(percentage)) %>% 
  mutate(gender_age = paste(gender, age)) %>% 
  filter(!str_detect(gender_age, "unknown")) %>% 
  filter(!str_detect(gender_age, "13-17")) %>% 
  filter(!str_detect(gender_age, "All")) %>% 
  group_by(advertiser_id) %>% 
  complete(id, gender, age, fill = list(percentage = 0)) %>% 
  ungroup() %>% 
  group_by(id, age, advertiser_id) %>% 
  summarize(percentage = sum(percentage)) %>% 
  ungroup() #%>% 
  # mutate(percentage = round(percentage * 100, 2)) %>% 
  # left_join(facebook_id_dat) 


fb_reach <- total_times  %>% 
  # left_join(batch_id_dat) %>% 
  group_by(advertiser_id) %>% 
  summarise(potential_reach_min = sum(potential_reach_lower_bound),
            n_ids = n()) %>% 
  ungroup() 

fb_total <- fb_total %>%
  rename(advertiser_name = party)

fb_times <- fb_times %>%
  rename(advertiser_name = party)

fb_gender <- fb_gender %>% 
  left_join(wtm_data) %>% 
  left_join(color_dat) #%>%
  # rename(advertiser_name = party)

fb_age <- fb_age %>% 
  left_join(wtm_data) %>% 
  left_join(color_dat) #%>%
  # rename(advertiser_name = party)

fb_reach <- fb_reach %>% 
  left_join(wtm_data) %>% 
  left_join(color_dat) #%>%
  # rename(advertiser_name = party)


fb_aggr <- list(total = fb_total, times = fb_times,
                gender = fb_gender,
                age = fb_age, reach = fb_reach)

### create graph ######

# color_dat <- tibble(
#   color = c("#00b13d", "#80c31c", "#cd503e", "#008067", "#6f2421", "#e3101c", "#e01003", "#036b2c", "#02a6e9", "#562883", "#eeaa00", "#34c1c4", "#92107d", "#202122", "#242b57"),
#   advertiser_name = c("D66", "GroenLinks", "VVD", "CDA", "FvD", "PvdA", "SP", "PvdD", "ChristenUnie", "Volt Nederland", "SGP", "DENK", "50PLUS", "BIJ1", "Ja21")) %>% 
#   mutate(advertiser_name = as.factor(advertiser_name))


# fb_aggr$report_spending <- spending
# fb_aggr$report_spending_loc <- spending_loc


saveRDS(fb_aggr, file = "data/fb_aggr.rds")




cat("\n\nFB Data: Done\n\n") 