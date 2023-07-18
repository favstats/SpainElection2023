color_dat <- tibble(
  colors = c("#e6194B", "#f5911d", "#00008b", "#00FF00", "#524fa1", "#ff1a1a", "#f1c851", "#f04949", "#6495ed", "#2598d4", "#89cff0", "#89cff0", "#9A6324", "#fffac8", "#800000", "#00008b"),
  party = c("Drugo", "DCG", "DF", "URA", "DPS", "SDP", "ES!", "SD", "SNP", "UCG", "Vlada Crne Gore", "Demos", "HGI", "LP", "Prava Crna Gora
", "ASh"))

party_dict <- tibble(coalition = c("Count Bravely! (Democrats-URA)", "Count Bravely! (Democrats-URA)", "ZBCG (NSD–DNP-RP)", "PES!", "Together! (DPS-SD-LP-UDSh)", "Together! (DPS-SD-LP-UDSh)", "Together! (DPS-SD-LP-UDSh)", "SDP", "For You! (SNP-Demos)", "For You! (SNP-Demos)", "UCG", "HGI", "People's coalition (Prava-DHP)", "Albanian Forum Coalition"),
                     party = c("URA", "DCG", "DF", "ES!", "DPS", "LP", "SD", "SDP", "SNP", "Demos", "UCG", "HGI", "Prava Crna Gora", "ASh"))

# 638633769597292
# Albanian Forum Coalition

color_dat <- party_dict %>% 
  distinct(coalition) %>% 
  mutate(colors = case_when(
    str_detect(coalition, "URA") ~ "#69bd44",
    str_detect(coalition, "ZBCG") ~ "#0037a0",
    str_detect(coalition, "PES") ~ "#f1c851",
    str_detect(coalition, "Together") ~ "#524fa1",
    str_detect(coalition, "SDP") ~ "#ff1a1a",
    str_detect(coalition, "For You") ~ "#00a2e4",
    str_detect(coalition, "UCG") ~ "#6585e7",
    str_detect(coalition, "HGI") ~ "#f08080",
    str_detect(coalition, "DHP") ~ "#0172bb",
    str_detect(coalition, "Albanian Forum Coalition") ~ "#00008b",
  ))


# # A tibble: 10 × 1
# coalition                     
# <chr>                         
#   1 Count Bravely! (Democrats-URA)
# 2 ZBCG (NSD–DNP-RP)             
#   4 Together! (DPS-SD-LP-UDSh)    
# 5 SDP                           
# 7 UCG                           
# 
# # A tibble: 10 × 10
#    advertiser_name                potent…¹ spend…² spend…³ spend…⁴ impre…⁵ impre…⁶ impre…⁷ n_ids colors
#    <chr>                             <dbl>   <dbl>   <dbl>   <dbl>   <dbl>   <dbl>   <dbl> <int> <chr> 
#  2 Count Bravely! (Democrats-URA) 21685463   15772   51536  33654   1.55e7  1.84e7  1.70e7   364 #69bd…
# 7 SDP                             8790097     594   10103   5348.  2.18e6  2.61e6  2.40e6    97 #ff1a…
# 8 Together! (DPS-SD-LP-UDSh)      5056057     552    6143   3348.  1.71e6  2.07e6  1.89e6    57 #524f…
# 9 ZBCG (NSD–DNP-RP)               6000061     259    6239   3249   1.28e6  1.55e6  1.42e6    61 #0037…
# 10 NA                                   NA