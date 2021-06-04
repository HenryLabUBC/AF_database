library(tidyverse)
library(lubridate)
library(zoo)
library(rootSolve)
library(birk)


compile_snow_depth <- function(){
  input_dir <- getwd()
  output_dir <- paste0(input_dir, "/compiled_data")
  if (!dir.exists(output_dir)){dir.create(output_dir)}
  compiled_snow_depth <- gather_data(input_dir, output_dir)
  compiled_clean_data <- clean_data(compiled_snow_depth, output_dir)
  #compiled_clean_data <- remove_outliers(compiled_snow_depth, output_dir)
  compiled_processed_data <- process_raw_data(compiled_clean_data)
  compiled_processed_data <- remove_non_zeros_upper(compiled_processed_data)
  compiled_processed_data <- process_raw_data(compiled_processed_data)
  compiled_processed_data <- remove_non_zeros(compiled_processed_data)
  compiled_processed_data <- process_raw_data(compiled_processed_data)
  make_plots(compiled_snow_depth, compiled_clean_data, compiled_processed_data, output_dir)
  create_csvs(compiled_processed_data, output_dir)
  compiled_processed_data
}

create_csvs <- function(compiled_processed_data, output_dir){
  compiled_processed_data <- ungroup(compiled_processed_data)
  max_summary <- compiled_processed_data %>% group_by(Year, Site) %>%
    summarise(max(Calibrated_snow_depth, na.rm = TRUE))
  full_data <- compiled_processed_data %>% select(Date,
                                                  Year,
                                                  Day,
                                                  Site,
                                                  Snow_depth_raw,
                                                  Calibrated_snow_depth,
                                                  rolling_snow_depth,
                                                  Snow_melt_day,
                                                  First_snow_day,
                                                  file)
  write_csv(full_data, paste0(output_dir, "/compiled_cassiope_snowdepth_data.csv"))
  write_csv(max_summary, paste0(output_dir, "/max_snowdepth_summary.csv"))
}

remove_non_zeros <- function(compiled_snow_data){
  compiled_snow_data <- compiled_snow_data %>% 
    mutate(Snow_depth_raw = ifelse(!is.na(Snow_melt_day) &
                                     !is.na(First_snow_day) &
                                     Day > Snow_melt_day &
                                     Day < First_snow_day &
                                     abs(Calibrated_snow_depth) > 40,
                                   NA, Snow_depth_raw))
  compiled_snow_data
}

remove_non_zeros_upper <- function(compiled_snow_data){
  compiled_snow_data <- compiled_snow_data %>% 
    mutate(Snow_depth_raw = ifelse(!is.na(Snow_melt_day) &
                                   !is.na(First_snow_day) &
                                   Day > Snow_melt_day &
                                  Day < First_snow_day &
                                    Calibrated_snow_depth > 40,
                                  NA, Snow_depth_raw))
  compiled_snow_data
}

make_plots <- function(compiled_snow_depth, compiled_clean_data, compiled_processed_data, output_dir){
  sites <- unique(sort(as.character(compiled_clean_data$Site)))
  years <- unique(sort(as.integer(compiled_clean_data$Year)))
  
  for (site in sites){
    for (year in years){
      year_data_with_outliers <- filter(compiled_clean_data, Year == year, Site == site)
      year_data_no_outliers <- filter(compiled_processed_data, Year == year, Site == site)
      
      if (length(!is.na(year_data_no_outliers$Snow_depth_raw)) == 0) {next}
      
      jpeg(paste0(output_dir, "/", year, "_", site, "_raw.jpeg"), width = 1400, height = 1400)
      op <- par(mfcol = c(2,1))
      plot(year_data_with_outliers$Day,
           year_data_with_outliers$Snow_depth_raw,
           xlab = "Day",
           ylab = "Snow_depth_raw",
           main = paste(year, site, "outliers"),
           col = "red",
           pch = 18,
           cex = 1.5,
           frame = FALSE)
      
      points(year_data_no_outliers$Day, year_data_no_outliers$Snow_depth_raw,
             col = "black", pch = 18, cex = 2)
      
      if (length(year_data_no_outliers$rolling_snow_depth[!is.na(year_data_no_outliers$rolling_snow_depth)]) > 0){
      
      plot(year_data_no_outliers$Day,
           year_data_no_outliers$rolling_snow_depth,
           xlab = "Day",
           ylab = "Callibrated_snow_depth",
           main = paste(year, site, "snow melt and first snow"),
           frame = FALSE)
      lines(c(year_data_no_outliers$Snow_melt_day[1], year_data_no_outliers$Snow_melt_day[1]), c(0, 500))
      lines(c(year_data_no_outliers$First_snow_day[1], year_data_no_outliers$First_snow_day[1]), c(0, 500))
      }
      par(op)
      dev.off()
    }
  }
      
}

gather_data <- function(input_dir, output_dir){
  files <- list.files(input_dir, "cassiope_snow_depth_[12]", full.names = TRUE)
  compiled_snow_depth <- tibble(Identifier=vector("integer"),
                                Year=vector("integer"),
                                Day=vector("integer"),
                                Cas.c.c.13=vector("numeric"),
                                Cas.o.c.9=vector("numeric"),
                                Cas.o.c.4=vector("numeric"),
                                Cas.c.c.9=vector("numeric"),
                                file=vector("character"))
  colnames <- (c("Identifier", "Year", "Day", "Cas.c.c.13", "Cas.o.c.9",
             "Cas.o.c.4", "Cas.c.c.9", "file"))
  coltypes <- c("iiinnnnc")
  for (file in files){
    file_check <- read_csv(file, col_names = FALSE, n_max = 1)
    if (length(file_check) != 8){
      write_lines(x = file,
                  file = paste0(output_dir, "/files_missing_data.txt"), 
                  append = TRUE,
                  sep = "\n")
      print(paste(file, "was missing data"))
      next
    }
    if (file_check[[1,1]] == 239){skip = 0} else {skip = 1}
    current_snow_depth <- read_csv(file = file,
                                   col_names = colnames,
                                   trim_ws = TRUE,
                                   skip = skip,
                                   col_types = coltypes)
    compiled_snow_depth <- bind_rows(compiled_snow_depth, current_snow_depth)                              
  }
  compiled_snow_depth
}

clean_data <- function(compiled_snow_depth, output_dir){
  compiled_snow_depth <- compiled_snow_depth %>% pivot_longer(cols = starts_with("Cas"),
                                      names_to = "Site",
                                      values_to = "Snow_depth_raw") %>%
    select("Identifier", "Year", "Day", "Site", "Snow_depth_raw", "file") %>%
    mutate(Snow_depth_raw = ifelse(Snow_depth_raw < 0 |
                                     Snow_depth_raw == 6999 |
                                     Snow_depth_raw < 1000, NA, Snow_depth_raw)) %>%
    mutate(Year = as.integer(Year),
           Day = as.integer(Day),
           Site = as.factor(Site)) %>% 
    filter(!grepl("willow", file, ignore.case = TRUE)) %>%
    group_by(Year, Day, Site) %>%
    mutate(Snow_depth_std = sd(Snow_depth_raw, na.rm = TRUE)) %>%
    filter(Snow_depth_std == 0) %>%
    arrange(Year, Day) %>% 
    distinct(Year, Day, .keep_all = TRUE)
    
  
  compiled_snow_depth <- add_date(compiled_snow_depth)
  
  compiled_snow_depth
}


remove_outliers <- function(compiled_snow_depth, output_dir){
  sites <- unique(sort(as.character(compiled_snow_depth$Site)))
  years <- unique(sort(as.integer(compiled_snow_depth$Year)))
  
  for (site in sites){
    for (year in years){
    #  outliers_found <- TRUE
     # count <- 0
      #while(outliers_found && count < 100){
       # outliers_found <- FALSE
      year_snow_depth <- filter(compiled_snow_depth, Year == year &&
                                  Site == site &&
                                  !is.na(Snow_depth_raw))
      if (length(year_snow_depth$Snow_depth_raw[!is.na(year_snow_depth$Snow_depth_raw)]) <= 100){
        write_lines(paste("Year:", year, "Site:", site, "insufficent data"), 
                    file = paste0(output_dir, "/outlier_errors.txt"), 
                    sep = "\n", append = TRUE
                    )
        next
      }
      roll_average_snow <- rollmean(rollmean(rollmean(rollmean(rollmean(
        year_snow_depth$Snow_depth_raw, 5), 5), 5), 5), 6)
      days <- year_snow_depth$Day[11:(length(year_snow_depth$Day) - 11)]
      snow_depth_function <- splinefun(days, roll_average_snow)
      first_derivative <- splinefun(days, snow_depth_function(days, 1))
      second_derivative <- splinefun(days, snow_depth_function(days, 2))
      
      roots_1D <- try(uniroot.all(first_derivative, interval = c(days[1], days[length(days)])))
      if (!length(roots_1D) > 0){
        print(paste(year, site))
        next
      }
      
      if (length(year_snow_depth$Day) < 35){next}
      
      for (root in roots_1D){
        if (is.na(roots_1D[which(roots_1D == root) + 2])){next}
        end_root <- roots_1D[which(roots_1D == root) + 2]
        confirmed_outliers <- confirm_outlier(year_snow_depth, root, end_root, snow_depth_function, first_derivative)
        
        if (!confirmed_outliers){next}
        outlier_days <- year_snow_depth$Day[confirmed_outliers]
        compiled_snow_depth <- compiled_snow_depth %>% mutate(
          Snow_depth_raw = replace(Snow_depth_raw,
                                   Year == year &&
                                     Site == site &&
                                     Day %in% outlier_days,
                                   NA))
      }
        
        
        #if (first_derivative(day) > 25){
         # confirm_outlier <- FALSE
         # first_day_index <- which(year_snow_depth$Day == day)
          #for (last_day in year_snow_depth$Day[(first_day_index + 2):(first_day_index+20)]){
           # confirmed_outliers <- confirm_outlier(year_snow_depth, day, last_day, snow_depth_function, first_derivative)
            #if (confirmed_outliers){break}
         # }
          #last_day_index <- which(year_snow_depth$Day >= day &
           #                       year_snow_depth$Day <= (day + 15) &
            #        first_derivative(year_snow_depth$Day) < -25)
          #if (length(last_day_index) == 0){next}
        
          #last_day <- year_snow_depth$Day[last_day_index[1]]
          
            
            #constant_entry <- check_constant_slope(first_derivative(day),
          #       snow_depth_function(year_snow_depth$Day[which(year_snow_depth$Day <= day &
           #                                              year_snow_depth$Day >= day - 10)]),
            #     year_snow_depth$Day[which(year_snow_depth$Day <= day &
             #                        year_snow_depth$Day >= day - 10)])
          #days_left <- year_snow_depth$Day[which(year_snow_depth$Day >= day)]
          #exit_day_index <- which(first_derivative(days_left) < -25)
          #constant_exit <- check_constant_slope(first_derivative(days_left[exit_day_index[1]]),
           #                                     first_derivative(days_left[exit_day_index[1]:(exit_day_index[1] + 10)]),
            #                                    days_left[exit_day_index[1]:(exit_day_index[1] + 10)])
          #if(constant_entry | constant_exit){next}
          #last_day <- return_root(15, day, -25, first_derivative)
        #  if (!confirmed_outliers){next}
         # outlier_days <- year_snow_depth$Day[confirmed_outliers]
          #compiled_snow_depth <- compiled_snow_depth %>% mutate(
           # Snow_depth_raw = replace(Snow_depth_raw,
            #                         Year == year &&
             #                          Site == site &&
              #                         Day %in% outlier_days,
               #                      NA))
          #next}
        #  outliers_found <- TRUE
         # break
       #   } else if (first_derivative(day) < -25){
        #    confirm_outlier <- FALSE
         #   first_day_index <- which(year_snow_depth$Day == day)
        #    for (last_day in year_snow_depth$Day[first_day_index:(first_day_index+20)]){
         #     confirmed_outlier <- confirm_outlier(year_snow_depth, day, last_day, snow_depth_function, first_derivative)
        #      if (confirmed_outlier){break}
         #   }
              
              #  last_day_index <- which(year_snow_depth$Day >= day &
               #                     year_snow_depth$Day <= (day + 15) &
                #                    first_derivative(year_snow_depth$Day) > 25)
            #if (length(last_day_index) == 0){next}
            
            #last_day <- year_snow_depth$Day[last_day_index[1]]
            
              
              #constant_entry <- check_constant_slope(first_derivative(day),
             #                snow_depth_function(year_snow_depth$Day[which(year_snow_depth$Day <= day &
              #                                                 year_snow_depth$Day >= day - 10)]),
               #              year_snow_depth$Day[which(year_snow_depth$Day <= day &
                #                                     year_snow_depth$Day >= day - 10)])
            #days_left <- year_snow_depth$Day[which(year_snow_depth$Day >= day)]
            #exit_day_index <- which(first_derivative(days_left) > 25)
            #constant_exit <- check_constant_slope(first_derivative(days_left[exit_day_index[1]]),
             #                                     first_derivative(days_left[exit_day_index[1]:(exit_day_index[1] + 10)]),
              #                                    days_left[exit_day_index[1]:(exit_day_index[1] + 10)])
            #if(constant_entry | constant_exit){next}
            # last_day <- return_root(15, day, 25, first_derivative)
      #       if (!confirmed_outlier){next}
       #     compiled_snow_depth <- compiled_snow_depth %>% mutate(
        #      Snow_depth_raw = replace(Snow_depth_raw,
        #                               Year == year &&
         #                                Site == site &&
          #                               Day >= day && Day < last_day,
             #                          NA))
        #    outliers_found <- TRUE
         #   break
            #   }
     # }
    #  count <- count + 1
     # if (count > 99){
      #  write_lines(paste("Year:", year, "Site:", site, "caught in infinite loop"), 
       #             file = paste0(output_dir, "/outlier_errors.txt"), 
        #            sep = "\n", append = TRUE
      #  )
      #}
      #}
      }
  }
  compiled_snow_depth
}

confirm_outlier <- function(year_snow_data, start_day, end_day, snow_depth_function, first_derivitive_function){
  start_day_index <- which.closest(year_snow_data$Day, start_day)
  end_day_index <- which.closest(year_snow_data$Day, end_day)
  previous_days <- year_snow_data$Day[(start_day_index - 5):(start_day_index)]
  
  future_days <- year_snow_data$Day[(end_day_index)]
  
  outlier_days <- year_snow_data$Day[(start_day_index + 1):(end_day_index - 1)]
  
  linear_model <- lm(snow_depth_function(previous_days) ~ previous_days)
  
  m <- unname(coef(linear_model))[2]
  c <- unname(coef(linear_model))[1]
  
  back_model <- lm(snow_depth_function(year_snow_data$Day[end_day_index:(end_day_index + 2)]) ~
                     year_snow_data$Day[end_day_index:(end_day_index + 2)])
  
  m2 <- unname(coef(back_model))[2]
  c2 <- unname(coef(back_model))[1]
  
  start_y <- snow_depth_function(start_day)
  
  predicted_start <- m*start_day + c
  predicted_start2 <- m2*start_day + c2
  
  future_y <- median(snow_depth_function(future_days))
  y_predicted <- median(m*future_days + c)
  y_predicted2 <- m2*future_days + c2
  
  current_y <- snow_depth_function(outlier_days)
  predicted_current <- m*outlier_days + c
  predicted_current2 <- m2*outlier_days + c2
  current_outlie_indicies <- which((predicted_current < 0.97*current_y | predicted_current > 1.03*current_y) &
                                     (predicted_current2 < 0.97*current_y | predicted_current2 > 1.03*current_y))
  
  confirm_outlier <- FALSE
  
  if (y_predicted > 0.95*future_y & y_predicted < 1.05*future_y &
      y_predicted2 > 0.95*future_y & y_predicted2 < 1.05*future_y &
      predicted_start > 0.95*start_y & predicted_start < 1.05*start_y &
      predicted_start2 > 0.95*start_y & predicted_start2 < 1.05*start_y &
      (length(current_outlie_indicies) > 0)){
    
    
    confirm_outlier <- start_day_index + current_outlie_indicies
    
  }
  confirm_outlier
}

check_constant_slope <- function(gradient, data, days){
  day_offset = 1
  count = 0
  while(day_offset %% 2 != 0){
    roll_data <- rollmedian(data, (length(data)/2 - count))
    day_offset <- (length(data) - length(roll_data))/2
    count = count + 1
  }
  offset_days <- days[(day_offset + 1):(length(days) - day_offset)]
  gradient_function <- splinefun(offset_days, roll_data)
  median_gradient <- median(gradient_function(offset_days, 1))
  if ((gradient > 0 & median_gradient > 5) |
      (gradient < 0 & median_gradient < -5)){
    consistent_slope = TRUE
  } else {
    consistent_slope = FALSE
  }
  consistent_slope
}


return_root <- function(range, lower_limit, magnatude, fun){
  return_root <- NA
  for (day in lower_limit:(lower_limit + range)){
    if ((magnatude > 0 && fun(day) > magnatude) ||
        (magnatude < 0 && fun(day) < magnatude)){
      return_root <- day
      break
    }
  }
  return_root
}


Calculate_rough_snow_depth <- function(compiled_snow_depth, year, site, year_snow_depth){

      annual_raw_max <- quantile(year_snow_depth$Snow_depth_raw,
                                 probs = 0.9, 
                                 na.rm = TRUE,
                                 names = FALSE)
      compiled_snow_depth <- compiled_snow_depth %>% mutate(annual_max_raw = replace(annual_max_raw,
                                                                                 Year == year &&
                                                                                   Site == site,
                                                                                 annual_raw_max),
                                                            rough_calc_snow_depth = replace(rough_calc_snow_depth,
                                                                                            Year == year &&
                                                                                              Site == site,
                                                                                            annual_max_raw - Snow_depth_raw))
        
      
    
  
  compiled_snow_depth
}

process_raw_data <- function(compiled_snow_depth){
  compiled_snow_depth <-  compiled_snow_depth %>%
    mutate(annual_max_raw = 0,
           rough_calc_snow_depth = -1000,
           Snow_melt_day = NA,
           First_snow_day = NA,
           Calibrated_snow_depth = NA,
           rolling_snow_depth = NA)
  
  sites <- unique(sort(as.character(compiled_snow_depth$Site)))
  years <- unique(sort(as.integer(compiled_snow_depth$Year)))
  
  for (site in sites){
    for (year in years){
      year_snow_depth <- filter(compiled_snow_depth, Year == year && Site == site)
      if (length(year_snow_depth$Snow_depth_raw[!is.na(year_snow_depth$Snow_depth_raw)]) > 180){
      compiled_snow_depth <- Calculate_rough_snow_depth(compiled_snow_depth,
                                                        year,
                                                        site,
                                                        year_snow_depth)
      year_snow_depth <- filter(compiled_snow_depth, Year == year && Site == site)
      
      compiled_snow_depth <- calculate_snow_min_max(compiled_snow_depth,
                                                    year,
                                                    site,
                                                    year_snow_depth)
      }
    }
  }
   compiled_snow_depth   
}

next_root <- function(root_list, reference_root){
  next_root <- 0
  for (root in root_list){
    if (root > reference_root){
      next_root <- root
      break
    }
  }
  next_root
}

last_root <- function(root_list, reference_root){
  last_root <- root_list[1]
  for (root in root_list){
    if (root > reference_root){
      break
    }
    last_root <- root
  }
  last_root
}

index_extractor <- function(vector, reference_value){
  count = 1
  for (value in vector[1:length(vector)]){
    if(value == reference_value){
      index = count
      break
    }
    count = count + 1
  }
  index
}
  
calculate_snow_min_max <- function(compiled_snow_depth, year, site, year_snow_depth){

  roll_average_snow <- rollmean(rollmean(rollmean(rollmean(
    rollmean(year_snow_depth$rough_calc_snow_depth, 3), 3), 3),4),3)
  days <- year_snow_depth$Day[6:(length(year_snow_depth$Day) - 6)]
  raw_snow_function <- splinefun(days, roll_average_snow)
  first_derivative <- splinefun(days, raw_snow_function(days, 1))
  second_derivative <- splinefun(days, raw_snow_function(days, 2))
  roots_1D <- uniroot.all(first_derivative, interval = c(days[1], days[length(days)]))
  roots_2D <- uniroot.all(second_derivative, interval = c(days[1], days[length(days)]))
  snow_melt_day <- NA
  first_snow_day <- NA
  calibrated_zero_snow <- 0
  for (root_2D in roots_2D[roots_2D > 120 & roots_2D < 200]){
    if (first_derivative(root_2D) < -1.5){
      next_1D_root <- roots_1D[which(roots_1D > root_2D)][1]
      if (raw_snow_function(next_1D_root) < 50){
       snow_melt_day <- days[which.closest(days, next_1D_root)]
       break
      }
    }
  }
  for (root_2D in roots_2D[roots_2D > 220 & roots_2D < 280]){
    if (first_derivative(root_2D) > 1){
      last_1D_root <- last(roots_1D[which(roots_1D < root_2D)])
      first_snow_day <- days[which.closest(days, last_1D_root)]
      calibrated_zero_snow <- mean(raw_snow_function(
        days[which(days >= snow_melt_day & days <= first_snow_day)]))
      next_1D_root <- roots_1D[which(roots_1D > root_2D)][1]
      post_melt_period <- raw_snow_function(days[which(days >= next_1D_root)])
      if (length(post_melt_period[post_melt_period < calibrated_zero_snow]) == 0){
      break
      }
    }
  }
  if (is.na(snow_melt_day) | is.na(first_snow_day)){
    snow_melt_day <- NA
    first_snow_day <- NA
    calibrated_zero_snow <- 0
  }
  
  rolling_snow <- rep(NA, times = 10)
  rolling_snow <- append(rolling_snow, roll_average_snow)
  rolling_snow <- append(rolling_snow, rep(NA, times = 11))
  
  compiled_snow_depth <- mutate(compiled_snow_depth, Snow_melt_day = replace(Snow_melt_day,
                                                             Year == year &&
                                                               Site == site,
                                                             snow_melt_day),
                                First_snow_day = replace(First_snow_day,
                                                        Year == year &&
                                                          Site == site,
                                                        first_snow_day),
                                Calibrated_snow_depth = replace(Calibrated_snow_depth,
                                                        Year == year &&
                                                          Site == site,
                                                        rough_calc_snow_depth - calibrated_zero_snow),
                                rolling_snow_depth = ifelse(Year == year &
                                                              Site == site &
                                                              Day %in% days,
                                                            raw_snow_function(Day),
                                                            rolling_snow_depth))
  # for (i in seq_along(days)){
  #    compiled_snow_depth <-mutate(compiled_snow_depth, rolling_snow_depth = replace(rolling_snow_depth,
  #                                                            Year == year &&
  #                                                              Site == site &&
  #                                                              Day == days[i],
  #                                                            roll_average_snow[i]))
  # }
  compiled_snow_depth
}


add_date <- function(dataframe){
  dataframe <- dataframe %>% rowwise() %>% mutate(Date = get_date(Year, Day)) %>%
    select(Date, 1:(length(dataframe)))
  dataframe
}

get_date <- function(year, day){
  date <- paste0(year, "0101")
  date <- ymd(date)
  yday(date) <- day
  date
}

determine_step_change <- function(compiled_snow_depth){
  snow_depth_with_step_change <- tibble(Identifier=vector("integer"),
                                Year=vector("integer"),
                                Day=vector("integer"),
                                Site=vector("character"),
                                Snow_depth_raw=vector("numeric"),
                                file=vector("character"),
                                Snow_depth_std=vector("numeric"),
                                step_change=vector("numeric"))
  site_list <- c("Cas.c.c.13", "Cas.o.c.9", "Cas.o.c.4", "Cas.c.c.9")
  for (site in site_list){
    site_table <- compiled_snow_depth %>% filter(grepl(site, Site))
    snow_depth <- site_table[[5]]
    days <- site_table[[3]]
    step_change <- NA
    day_change <- NA
    day_change <- append(day_change, days[2:length(snow_depth)] - days[1:(length(snow_depth) - 1)])
    step_change <-  append(step_change, snow_depth[2:length(snow_depth)] - snow_depth[1:(length(snow_depth) - 1)])
    steps <- tibble(step_change = step_change, day_change = day_change, step_per_day = step_change/day_change)
    site_table <- bind_cols(site_table, steps)
    snow_depth_with_step_change <- bind_rows(snow_depth_with_step_change, site_table)
  } 
  snow_depth_with_step_change
}