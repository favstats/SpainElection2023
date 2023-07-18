
hc_plotter <- function(plot_dat, filters, plot_type, plot_type_sub, mapdata = NULL,
                       platform, trans_internal = trans, last_updated, minmax = "Minimum") {
  
  # trans_internal <- trans
  
  if (plot_type %in% unlist_it(trans_internal$choices, 1:3)) {
    if(plot_type_sub == unlist_it(trans_internal$total_text, 1)){
      plot_dat_fin <- plot_dat$total
    } else if (plot_type_sub == unlist_it(trans_internal$total_text, 2)){
      plot_dat_fin <- plot_dat$times %>%
        group_by(advertiser_name) %>%
        arrange(date_range_start) %>%
        mutate_if(is.numeric, ~cumsum(.x)) %>% 
        ungroup()
    } else {
      return(invisible())
    }
  } else if (plot_type == unlist_it(trans_internal$choices, 4)){
    if(plot_type_sub == unlist_it(trans_internal$targeted_ads_choices, 3)){
      plot_dat_fin <- plot_dat$geo
    } else if (plot_type_sub == unlist_it(trans_internal$targeted_ads_choices, 1)){
      plot_dat_fin <- plot_dat$gender
    } else if (plot_type_sub == unlist_it(trans_internal$targeted_ads_choices, 2)){
      plot_dat_fin <- plot_dat$age
    } else {
      return(invisible())
    }   
  } else {
    return(invisible())
  }  
  
  if(platform == "Facebook"){
    credits_text <- glue::glue(trans_internal$plot_credits_text_fb)
    href_text <- "https://www.facebook.com/ads/library/"
  } else if (platform == "Google"){
    credits_text <- glue::glue(trans_internal$plot_credits_text_ggl)
    href_text <- "https://transparencyreport.google.com/political-ads/region/nl"   
  }
  
  upper_or_lower_bound <- minmax
  
  
  # if(!is.null(plot_dat_fin$page_name)){
  #   plot_dat_fin <- plot_dat_fin %>% rename(advertiser_name = page_name)
  # }
  
  hc_data <- plot_dat_fin %>% 
    filter(advertiser_name %in% filters) 
  
  if(plot_type == unlist_it(trans_internal$choices, 1)){
    
    title_text <- glue::glue(trans_internal$plot_title_number_of_ads)
    subtitle_text <- trans_internal$plot_subtitle_number_of_ads
    
    if(plot_type_sub == unlist_it(trans_internal$total_text, 1)){
      
      
      lvls <- hc_data %>% 
        mutate(advertiser_name = fct_reorder(advertiser_name, n)) %>% 
        dplyr::pull(advertiser_name) %>% 
        levels() %>% 
        rev()
      
      hc_plot <- hc_data %>% 
        hchart(
          type = "bar",
          hcaes(
            x = advertiser_name,
            y = n, 
            color = colors),
          tooltip = list(pointFormat = trans_internal$plot_tooltip_number_of_ads)) %>%
        hc_yAxis(title = list(text = trans_internal$plot_yaxis_number_of_ads))  %>% 
        hc_xAxis(categories = lvls, title = list(text = "")) 
    }
    
    if(plot_type_sub == unlist_it(trans_internal$total_text, 2)){
      hc_plot <- hc_data %>%
        hchart("line", hcaes(x = date_range_start,
                             y = n,
                             group = advertiser_name#,
                             #color = colors
        )) %>%
        hc_title(
          text = title_text
        ) %>%
        hc_yAxis(
          align = "left",
          title = list(text = trans_internal$plot_yaxis_number_of_ads)
        ) %>%
        hc_xAxis(
          align = "left",
          title = list(text = trans_internal$plot_xaxis_number_of_ads)
        ) %>%
        hc_colors(unique(hc_data$colors)) %>% 
        hc_plotOptions(line = list(
          marker = F#list(radius = 3)
        )) %>%
        hc_tooltip(
          crosshairs = TRUE,
          backgroundColor = "#F0F0F0",
          shared = FALSE
        )
      
      # fin <- hc_data %>% 
      #   apex(type = "line", mapping = aes(x = date_range_start, 
      #                                     y = n, 
      #                                     group = advertiser_name,
      #                                     color = colors))  %>% 
      #   ax_colors(unique(hc_data$color)) %>% 
      #   ax_labs(
      #     title = title_text,
      #     subtitle = subtitle_text
      #   ) 
      # 
      # # #print(hc_data)
      # 
      # return(fin)
    }
  } else if (plot_type == unlist_it(trans_internal$choices, 2)) {
    ## if option is spend
    
    if(platform == "Facebook"){
      #print("Facebook")
      
      spend_tooltip <- trans_internal$plot_tooltip_spend_fb
      
      title_text <- glue::glue(trans_internal$plot_title_spend_fb)
      subtitle_text <- glue::glue(trans_internal$plot_subtitle_spend_fb)
      
      if(minmax == "Minimum"){
        #print("Minimum")
        lvls <- hc_data %>% 
          mutate(advertiser_name = fct_reorder(advertiser_name, spend_range_min)) %>% 
          dplyr::pull(advertiser_name) %>% 
          levels() %>% 
          rev()      
        
        hc_data <- hc_data %>% 
          mutate(value = spend_range_min)
        
      } else if (minmax == "Maximum"){
        #print("Maximum")
        lvls <- hc_data %>% 
          mutate(advertiser_name = fct_reorder(advertiser_name, spend_range_max)) %>% 
          dplyr::pull(advertiser_name) %>% 
          levels() %>% 
          rev()      
        
        hc_data <- hc_data %>% 
          mutate(value = spend_range_max)
        
        
      }
    } else if(platform == "Google"){
      #print("Google")
      
      upper_or_lower_bound <- "Totaal"
      
      spend_tooltip <- trans_internal$plot_tooltip_spend_ggl
      
      title_text <- glue::glue(trans_internal$plot_title_spend_ggl)
      subtitle_text <- glue::glue(trans_internal$plot_subtitle_spend_ggl)      
      
      lvls <- hc_data %>% 
        mutate(advertiser_name = fct_reorder(advertiser_name, spend_eur)) %>% 
        dplyr::pull(advertiser_name) %>% 
        levels() %>% 
        rev()      
      
      hc_data <- hc_data %>% 
        mutate(value = spend_eur)
      
    }
    
    if(plot_type_sub == unlist_it(trans_internal$total_text, 1)){
      #if option is Total
      #print("total")
      
      # js_scrip <- "function() { return '<a target=\"_top\" href=\"https://www.france-politique.fr/election-presidentielle-1965.htm\">' + this.value + '</a>';}"
      
      
      # var obj = {
      #   key1: "xd",
      #   key2: "xw",
      #   GroenLinks: "AR148211418645135360"
      # };
      # 
      # var getProperty = function (propertyName) {
      #   return obj[propertyName];
      # };
      # 
      # document.write(getProperty("GroenLinks"));
      # 
      # getProperty("key1");
      # getProperty("key2");
      
      
      hc_plot <- hc_data %>% 
        hchart(
          type = "bar",
          hcaes(
            x = advertiser_name,
            y = value,
            # low = spend_range_min,
            # high = spend_range_max,
            color = colors),
          tooltip = list(pointFormat = spend_tooltip)) %>%
        # hchart(
        #   type = "errorbar",
        #   hcaes(
        #     x = advertiser_name,
        #     low = spend_range_min, 
        #     high = spend_range_max,
        #     color = colors),im
        #   tooltip = list(pointFormat = "<b>Lower bound:</b> {point.spend_range_min}€<br><b>Mid point:</b> {point.spend_range_mid}€<br><b>Upper bound:</b> {point.spend_range_max}€")) %>%
        # hc_add_series(
        #  data = hc_data,
        #   type = "bar",
      #   hcaes(
      #     x = advertiser_name, 
      #     y = spend_range_mid,
      #     color = colors
      #   ),
      #   name = "Euros spent",
      #   tooltip = list(pointFormat = "<b>Lower bound:</b> {point.spend_range_min}€<br><b>Mid point:</b> {point.spend_range_mid}€<br><b>Upper bound:</b> {point.spend_range_max}€<br><br><b>Number of Ads:</b> {point.n}"))%>%
      hc_yAxis(reversed = F, min = 0, title = list(text = glue::glue(trans_internal$plot_yaxis_spend)))  %>% 
        hc_xAxis(categories = lvls, title = list(text = "")#,  
                 # labels = list(
                 #   formatter = JS(js_scrip)
                 # )
        ) %>% 
        hc_chart(inverted = TRUE)  
    }
    
    
    # return '<a target="_top" href=" '+ categoryLinks[this.value] + '">' + this.value + '</a>';
    # 
    # JS("function(){
    #   
    #                   return '<a  href=" '+ categoryLinks[this.value] + '">' + this.value + '</a>';
    #                   }")
    # 
    if(plot_type_sub == unlist_it(trans_internal$total_text, 2)){
      # if option is over time
      #print("over time")
      hc_plot <- hc_data %>%
        hchart("line", hcaes(x = date_range_start, 
                             y = value, 
                             group = advertiser_name),
               tooltip = list(pointFormat = paste0("{point.advertiser_name}<br><br>", spend_tooltip))) %>%
        hc_title(
          text = title_text
        ) %>%
        hc_yAxis(
          align = "left",
          title = list(text = glue::glue(trans_internal$plot_yaxis_spend))
        ) %>%
        hc_xAxis(
          align = "left",
          title = list(text = trans_internal$plot_xaxis_spend)
        ) %>% 
        hc_colors(unique(hc_data$colors)) %>% 
        hc_plotOptions(line = list(
          marker = F#list(radius = 3)
        )) %>%
        hc_tooltip(
          crosshairs = TRUE,
          backgroundColor = "#F0F0F0",
          shared = FALSE
        )
    }
    
  } else if (plot_type == unlist_it(trans_internal$choices, 3)){
    #print("impressions")
    title_text <- glue::glue(trans_internal$plot_title_impressions)
    subtitle_text <- glue::glue(trans_internal$plot_subtitle_impressions)
    
    if(plot_type_sub == unlist_it(trans_internal$total_text, 1)){
      
      if(minmax == "Minimum"){
        lvls <- hc_data %>% 
          mutate(advertiser_name = fct_reorder(advertiser_name, impressions_range_min)) %>% 
          dplyr::pull(advertiser_name) %>% 
          levels() %>% 
          rev()      
        
        hc_data <- hc_data %>% 
          mutate(value = impressions_range_min)
        
      } else if (minmax == "Maximum"){
        lvls <- hc_data %>% 
          mutate(advertiser_name = fct_reorder(advertiser_name, impressions_range_max)) %>% 
          dplyr::pull(advertiser_name) %>% 
          levels() %>% 
          rev()      
        
        hc_data <- hc_data %>% 
          mutate(value = impressions_range_max)
      }
      
      
      hc_plot <- hc_data %>% 
        drop_na(impressions_range_min, impressions_range_mid, impressions_range_max) %>% 
        hchart(
          type = "bar",
          hcaes(
            x = advertiser_name,
            y = value,
            color = colors),
          tooltip = list(pointFormat =  trans_internal$plot_tooltip_impressions)) %>%
        # hchart(
        #   type = "errorbar",
        #   hcaes(
        #     x = advertiser_name,
        #     low = impressions_range_min, 
        #     high = impressions_range_max,
        #     color = colors),
        #   tooltip = list(pointFormat = "<b>Lower bound:</b> {point.impressions_range_min}<br><b>Mid point:</b> {point.impressions_range_mid}<br><b>Upper bound:</b> {point.impressions_range_max}")) %>%
        # hc_add_series(
        #   data = hc_data, 
        #   type = "point",
      #   hcaes(
      #     x = advertiser_name, 
      #     y = impressions_range_mid,
      #     color = colors
      #   ),
      #   name = "Impressions",
      #   tooltip = list(pointFormat = "<b>Lower bound:</b> {point.impressions_range_min}<br><b>Mid point:</b> {point.impressions_range_mid}<br><b>Upper bound:</b> {point.impressions_range_max}<br><br><b>Number of Ads:</b> {point.n}"))%>%
      hc_yAxis(reversed = F, min = 0, title = list(text = glue::glue(trans_internal$plot_yaxis_impressions)))  %>% 
        hc_xAxis(categories = lvls, title = list(text = "")) %>% 
        hc_chart(inverted = TRUE)    %>%
        hc_size(width=700)        
    }
    if(plot_type_sub == unlist_it(trans_internal$total_text, 2)){
      
      if(minmax == "Minimum"){
        lvls <- hc_data %>% 
          mutate(advertiser_name = fct_reorder(advertiser_name, impressions_range_min)) %>% 
          dplyr::pull(advertiser_name) %>% 
          levels() %>% 
          rev()      
        
        hc_data <- hc_data %>% 
          mutate(value = impressions_range_min)
        
      } else if (minmax == "Maximum"){
        lvls <- hc_data %>% 
          mutate(advertiser_name = fct_reorder(advertiser_name, impressions_range_max)) %>% 
          dplyr::pull(advertiser_name) %>% 
          levels() %>% 
          rev()      
        
        hc_data <- hc_data %>% 
          mutate(value = impressions_range_max)
      }
      
      
      
      hc_plot <- hc_data %>%
        hchart("line", hcaes(x = date_range_start, 
                             y = value, 
                             group = advertiser_name),
               tooltip = list(pointFormat = trans_internal$plot_tooltip_impressions)) %>%
        hc_title(
          text = title_text
        ) %>%
        hc_yAxis(
          align = "left",
          title = list(text = glue::glue(trans_internal$plot_yaxis_impressions))
        ) %>%
        hc_xAxis(
          align = "left",
          title = list(text = trans_internal$plot_xaxis_impressions)
        ) %>% 
        hc_colors(unique(hc_data$colors))  %>% 
        hc_plotOptions(line = list(
          marker = F#list(radius = 3)
        )) %>%
        hc_tooltip(
          crosshairs = TRUE,
          backgroundColor = "#F0F0F0",
          shared = FALSE
        )     %>%
        hc_size(width=700)    
    }
  } else if (plot_type == unlist_it(trans_internal$choices, 4)){
    
    if(plot_type_sub == unlist_it(trans_internal$targeted_ads_choices, 3)){
      
      if(platform == "Facebook"){
        
        hc_fin <- hc_data %>% 
          group_split(advertiser_name) %>% 
          map(~{chart_maps(.x, F, mapdata, trans_internal)}) %>% hw_grid(ncol = 3) %>% 
          htmltools::browsable()
        
        return(hc_fin)             
      } else if (platform == "Google"){
        
        title_text <- glue::glue(trans_internal$plot_title_geo)
        subtitle_text <- glue::glue("")
        
        
        hc_plot <- hc_data %>% 
          filter(n != 0) %>% 
          mutate(geo_targeting_included = ifelse(geo_targeting_included == "Not targeted", trans_internal$not_targeted, geo_targeting_included)) %>% 
          hchart("bar", hcaes(x = advertiser_name, y = perc, group = geo_targeting_included),
                 tooltip = list(pointFormat = "<b>{point.geo_targeting_included}: </b> {point.perc}%"))  %>%
          hc_xAxis(
            align = "left",
            title = list(text = trans_internal$plot_yaxis_geo)
          ) %>%
          hc_yAxis(
            align = "left",
            title = list(text = trans_internal$plot_xaxis_geo)
          ) 
        
      }
    } else if (plot_type_sub == unlist_it(trans_internal$targeted_ads_choices, 1)){
      
      
      title_text <- glue::glue(trans_internal$plot_title_gender)
      subtitle_text <- glue::glue("")
      
      
      if(platform == "Google"){
        
        hc_plot <- hc_data %>% 
          # filter(n != 0) %>% 
          mutate(gender_targeting = ifelse(gender_targeting == "Not targeted", trans_internal$not_targeted, gender_targeting)) %>% 
          hchart("bar", hcaes(x = advertiser_name, y = perc, group = gender_targeting),
                 tooltip = list(pointFormat = "<b>{point.gender_targeting}: </b> {point.perc}%"))  %>%
          hc_xAxis(
            align = "left",
            title = list(text = trans_internal$plot_yaxis_gender_ggl)
          ) %>%
          hc_yAxis(
            align = "left",
            title = list(text = trans_internal$plot_xaxis_gender)
          ) 
      } else if (platform == "Facebook"){
        
        
        hc_data <- hc_data %>% 
          filter(gender %in% c("male", "female")) %>% 
          mutate(gender = ifelse(gender == "male", trans_internal$gender_male, trans_internal$gender_female)) %>% 
          data_to_boxplot(percentage, advertiser_name, gender)
        
        hc_plot <-  highchart() %>%
          hc_xAxis(type = "category") %>%
          hc_add_series_list(hc_data) %>%
          hc_yAxis(reversed = F, min = 0, title = list(text =  trans_internal$plot_yaxis_gender_fb))%>%
          hc_size(width=700)  
        # hc_chart(inverted = TRUE)
        
      }
    } else if (plot_type_sub == unlist_it(trans_internal$targeted_ads_choices, 2)){
      
      ## this needs to change
      title_text <- glue::glue("{unlist_it(trans_internal$targeted_ads_choices, 2)} met {platform} advertenties")
      subtitle_text <- glue::glue("")
      
      if(platform == "Google"){
        
        hc_plot <- hc_data %>% 
          mutate(age_targeting2 = case_when(
            age_targeting2 == "18-24, 25-34, 35-44, 45-54, 55-64" ~ "18-65",
            age_targeting2 == "18-24, 35-44, 45-54, 55-64, ≥65" ~ "18-24, 35-65+",
            age_targeting2 == "18-24, 45-54, 55-64, ≥65" ~ "18-24, 45-65+",
            age_targeting2 == "25-34, 35-44, 45-54, 55-64, ≥65" ~ "25-65+",
            T ~ age_targeting2
          )) %>% 
          mutate(age_targeting2 = ifelse(age_targeting2 == "Not targeted", trans_internal$not_targeted, age_targeting2)) %>% 
          # filter(n != 0) %>% 
          hchart("bar", hcaes(x = advertiser_name, y = perc, group = age_targeting2),
                 tooltip = list(pointFormat = "<b>{point.age_targeting2}: </b> {point.perc}%"))  %>%
          hc_xAxis(
            align = "left",
            title = list(text = trans_internal$plot_yaxis_age_ggl)
          ) %>%
          hc_yAxis(
            align = "left",
            title = list(text = trans_internal$plot_xaxis_age)
          ) 
      } else if (platform == "Facebook"){
        
        hc_data <- hc_data %>% 
          drop_na(age, advertiser_name) %>% 
          data_to_boxplot(percentage, advertiser_name, age)
        
        
        hc_plot <-  highchart() %>%
          hc_xAxis(type = "category") %>%
          hc_add_series_list(hc_data)%>%
          hc_yAxis(reversed = F, min = 0, title = list(text = trans_internal$plot_yaxis_age_fb)) %>%
          hc_size(width=700) %>%
          hc_chart(events = list(load = JS("function() {
  var chart = this;
  chart.series[1].setVisible(false)
  chart.series[2].setVisible(false)
  chart.series[3].setVisible(false)
  chart.series[4].setVisible(false)
  }"))) 
        
      }
    }
    
    
  }
  
  title_text <- str_replace(title_text, "targeting with", "impressions of")
  
  hc_plot %>% 
    hc_title(
      text = str_replace(title_text, "targeting met Facebook advertenties", "impressions of Facebook ads")
    ) %>%
    hc_subtitle(
      text = subtitle_text
    ) %>% 
    hc_credits(
      enabled = T,
      text = credits_text,
      href = href_text
    )  %>%
    hc_exporting(
      enabled = TRUE
    ) %>% hw_grid(ncol = 1)  
}


chart_maps <- function(x, download_data = T, mapdata, trans_internal, value_to_be_plotted = "percentage") {
  hc <- hcmap2(
    "https://code.highcharts.com/mapdata/countries/nl/nl-all.js",
    custom_map = mapdata,
    data = x,
    download_map_data = F,
    value = value_to_be_plotted,
    joinBy = c("name", "name"),
    name = trans_internal$plot_tooltip_geo,
    dataLabels = list(enabled = TRUE, format = "{point.name}"),
    borderColor = "#FAFAFA",
    borderWidth = 0.1,
    tooltip = list(
      valueDecimals = 2,
      valueSuffix = "%"
    )
  ) %>% 
    hc_colorAxis(
      minColor = "white",
      maxColor = unique(x$colorful),
      min = 0,
      max = max(x$percentage)+0.1
    )%>% 
    hc_title(
      text = unique(x$advertiser_name)
    ) %>%
    hc_exporting(
      enabled = TRUE
    )
  
  # download_data <<- F
  
  return(hc)
}

chart_maps2 <- function(x, download_data = T, mapdata, trans_internal, value_to_be_plotted = "percentage", max) {
  hc <- hcmap2(
    "https://code.highcharts.com/mapdata/countries/nl/nl-all.js",
    custom_map = mapdata,
    data = x,
    download_map_data = F,
    value = value_to_be_plotted,
    joinBy = c("name", "name"),
    name = "Share of Budget spent in Region",
    dataLabels = list(enabled = TRUE, format = "{point.name}"),
    borderColor = "#FAFAFA",
    borderWidth = 0.1,
    tooltip = list(
      valueDecimals = 2,
      valueSuffix = "%"
    )
  ) %>% 
    hc_colorAxis(
      minColor = "white",
      maxColor = unique(x$colorful),
      min = 0,
      max = max(x$percentage)+0.1
    )%>% 
    hc_title(
      text = unique(x$advertiser_name)
    ) %>%
    hc_exporting(
      enabled = TRUE
    )
  
  # download_data <<- F
  
  return(hc)
}

hcmap2 <- function(map = "custom/world",
                   data = NULL, joinBy = "hc-key", value = NULL,
                   download_map_data = FALSE, custom_map = NULL, ...) {
  
  url <- "https://code.highcharts.com/mapdata"
  map <- str_replace(map, "\\.js", "")
  map <- str_replace(map, "https://code\\.highcharts\\.com/mapdata/", "")
  mapfile <- sprintf("%s.js", map)
  
  hc <- highchart(type = "map")
  
  if(download_map_data) {
    
    mapdata <- download_map_data(file.path(url, mapfile))
    
  } else {
    
    mapdata <- custom_map
    
  }
  
  if(is.null(data)) {
    
    hc <- hc %>% 
      highcharter:::hc_add_series.default(
        mapData = mapdata, ...)
    
  } else {
    
    stopifnot(joinBy %in% names(data))
    data <- mutate_(data, "value" = value)
    
    hc <- hc %>% 
      highcharter:::hc_add_series.default(
        mapData = mapdata,
        data = list_parse(data), joinBy = joinBy, ...) %>% 
      hc_colorAxis(auxpar = NULL)
    
  }
  
  hc
  
}


radioTooltip <- function(id, choice, title, placement = "bottom", trigger = "hover", options = NULL){
  
  options = shinyBS:::buildTooltipOrPopoverOptionsList(title, placement, trigger, options)
  options = paste0("{'", paste(names(options), options, sep = "': '", collapse = "', '"), "'}")
  bsTag <- shiny::tags$script(shiny::HTML(paste0("
    $(document).ready(function() {
      setTimeout(function() {
        $('input', $('#", id, "')).each(function(){
          if(this.getAttribute('value') == '", choice, "') {
            opts = $.extend(", options, ", {html: true});
            $(this.parentElement).tooltip('destroy');
            $(this.parentElement).tooltip(opts);
          }
        })
      }, 500)
    });
  ")))
  htmltools::attachDependencies(bsTag, shinyBS:::shinyBSDep)
}



unlist_it <- function(x, index) {
  
  unlisted <-  x %>% str_split(",") %>% unlist %>% str_trim()
  
  if(missing(index)){
    return( x %>% str_split(",") %>% unlist)
  } else {
    return(unlisted[index])
  }
  
}


hc_plot_boxes <- function(x, trans_internal, subtitle = subtitle_text, credits = credits_text, href = href_text) {
  hc_plot <-  highchart() %>%
    hc_xAxis(type = "category") %>%
    hc_add_series_list(x) %>%
    hc_yAxis(reversed = F, min = 0, max = 100, title = list(text =  trans_internal$plot_yaxis_gender_fb)) %>% 
    hc_title(
      text = x[["data"]][[1]][[1]][["name"]]) %>%
    hc_subtitle(
      text = subtitle
    ) %>% 
    hc_credits(
      enabled = T,
      text = credits,
      href = href
    )  %>%
    hc_exporting(
      enabled = TRUE
    )           
  
  return(hc_plot)
}