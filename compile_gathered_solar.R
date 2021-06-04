library(tidyverse)
library(lubridate)
library(insol)
library(zoo)
library(birk)
library(tundra)

#' A function designed to clean and compile the data collated by the collate_solar function.
#' It takes as input the name of the site, an abbreviation for the site, an input directory
#' (the directory containing all the files produced by the collate_solar function), and an
#' output directory.
#' The data from all the csv files in the input directory are combined into one file. Any days
#' with multiple entries which are in agreement with each other are condensed to a single entry. Any
#' days with multiple entries that disagree with each other are excluded and written to a separate file
#' in the output directory for investigation (if there are two options for a given day and one option
#' is NA, then the non-NA value is maintained in the compiled data). Data outwith a given acceptable
#' range are converted to NA.
#' Model data is produced for both mean and max solar output for a full calender year. For each year
#' of data, and for each column of solar data, the data is compared against each model and identified
#' as either mean or max data based on which it matches more closely.
#' The appropriate model is then selected and scaled to more closely fit the data. This scaled model
#' is then used to identify which days were sunny, cloudy, or had snow obscuring the sensor.
#' Finally, the data is written to file and a series of plots are generated for each solar column for
#' each year of data.
#' @param site A character vector identifying the name of the site for which data is being compiled
#' @param site_abreviation The site abbreviation is used to construct a unique climate ID
#' @param input_dir The directory where all data to be compiled can be found
#' @param output_dir The directory to which all output shall be written
#' @param W_or_KW Either "W" or "KW", to identify the units the solar data will be found in
#' @export
compile_solar <- function(site, site_abreviation, input_dir, output_dir, W_or_KW, fix_cols = FALSE){
  # Create output directory if it does not already exist
  if (!dir.exists(output_dir)){dir.create(output_dir)}
  # load all of the CSV files in input_dir into a single dataframe
  compiled_solar <- gather_data(input_dir)
  # Set the upper and lower limits for the solar data (outwith which data will be converted to NA)
  if (W_or_KW == "W"){
    upper_limit <- 1000
    lower_limit <- -1
  } else {
    upper_limit <- 1
    lower_limit <- -0.01
  }
  # Remove outliers and duplicates; create file containing any disagreeing data
  compiled_clean_data <- clean_data(compiled_solar, upper_limit, lower_limit, "solar", output_dir)
  # Create model data and use it to label data and determine if day was sunny, cloudy, or had snow
  # on sensor
  is_hour <- hour_identifier(compiled_clean_data, return_fractional_day = FALSE)
  if (sum(is_hour) > 0){
    daily_data <- compiled_clean_data %>% ungroup() %>% group_by(Year, Day) %>%
      mutate(Solar = mean(Solar)) %>% select(!c(Hour, fractional_day, date))
    daily_data <- add_date(daily_data)
    daily_data <- clean_data(daily_data, upper_limit, lower_limit, "solar", output_dir)
    daily_data <- process_data(daily_data, site_abreviation, W_or_KW, output_dir, fix_cols)
    daily_data_simplified <- daily_data %>% select(Year, Day, cloudy_solar_mean_W, fractional_year,
                                        calc_max_solar, calc_mean_solar, calc_mean_adjusted)
    compiled_processed_data <- merge(compiled_clean_data, daily_data_simplified, by = c("Year", "Day"))
    compiled_processed_data <- compiled_processed_data %>%
      mutate(climate_ID = paste0(site_abreviation, "_", date)) %>%
      rename("solar_mean_{W_or_KW}" := Solar, calc_mean_adjusted = calc_mean_adjusted)
    # Write compiled data to file
    create_csvs(compiled_processed_data, output_dir, site)
    # Create plots for each column and year of solar data
    make_plots(daily_data, output_dir, site, summary = FALSE)
    } else {
  compiled_processed_data <- process_data(compiled_clean_data, site_abreviation, W_or_KW)

  # Write compiled data to file
  create_csvs(compiled_processed_data, output_dir, site)
  # Create plots for each column and year of solar data
  make_plots(compiled_processed_data, output_dir, site)
    }
  compiled_processed_data
}

create_csvs <- function(compiled_processed_data, output_dir, site){
  # remove any previous grouping
  compiled_processed_data <- ungroup(compiled_processed_data)
  # Find all column names starting with "solar"
  solar_names <- grep("^solar", names(compiled_processed_data), value = TRUE)
  # For each column with name starting "solar", create a summary file counting all sunny, cloudy, and
  # snow obscured days. Also, produce a file containing all relevant data for that column. Write both
  # files to a sub_directory within the output_dir, identified by the name of the column beginning with
  # "solar".
  for (name in solar_names){
    # Create summary file
  cloud_summary <- compiled_processed_data %>% group_by(Year, .data[[paste0("cloudy_", name)]], .drop = FALSE) %>%
    summarise(total = n()) %>%
    pivot_wider(names_from = .data[[paste0("cloudy_", name)]], values_from = total) %>%
    mutate(total = sunny + cloudy + v.cloudy + snow)
  # Create sub-directory if it doesn't already exist
  if (!dir.exists(paste0(output_dir, "/", name))){
    dir.create(paste0(output_dir, "/", name))
  }
    # Write summary file
    write_csv(cloud_summary, paste0(output_dir, "/", name,"/", site, "_", name, "_annual_summary.csv"))
  }
  # Select all appropriate columns that pair with the identified solar column
  full_data <- compiled_processed_data %>% select(climate_ID,
                                                  Identifier,
                                                  date,
                                                  Year,
                                                  fractional_year,
                                                  Day,
                                                  starts_with("solar"),
                                                  starts_with("calc"),
                                                  starts_with("cloudy"),
                                                  file)
  # write selected compiled data to file
  write_csv(full_data, paste0(output_dir, "/compiled_", site, "_solar_data.csv"))

}


# Create plots for all columns containing either mean or max solar data, for every year data is
# available
make_plots <- function(compiled_processed_data, output_dir, site, summary = TRUE){
  # Find all column names starting with solar
  solar_names <- grep("^solar", names(compiled_processed_data), ignore.case = TRUE, value = TRUE)
  # For every column containing either mean or max solar data, create a plot for every year
  # displaying all sunny, cloudy, and snow obscured days, as well as the appropriate model data
  for (name in solar_names){
    # extract either mean or max from the solar column name
    mean_or_max <- str_split(name, "_", simplify = TRUE)[2]
    # determine column index of the solar data
    solar_index <- which(names(compiled_processed_data) == name, arr.ind = TRUE)
    # determine column index of the appropriate model data
    model_index <- which(names(compiled_processed_data) ==
                           paste0("calc_", mean_or_max, "_solar"), arr.ind = TRUE)
    # determine column index of the appropriate scaled model data
    adjusted_index <- which(names(compiled_processed_data) ==
                           paste0("calc_", mean_or_max, "_adjusted"), arr.ind = TRUE)
    # subset solar data by cloud/snow status
  sunny <- compiled_processed_data %>% filter(.data[[paste0("cloudy_", name)]] == "sunny")
  cloudy <- compiled_processed_data %>% filter(.data[[paste0("cloudy_", name)]] == "cloudy")
  v.cloudy <- compiled_processed_data %>% filter(.data[[paste0("cloudy_", name)]] == "v.cloudy")
  snow <- compiled_processed_data %>% filter(.data[[paste0("cloudy_", name)]] == "snow")
  # Obtain list of all years for which data is available
  years <- unique(compiled_processed_data$Year)
  # create plots for every available year
  for (year in years){
    # Subset all data by year
      year_data_sunny <- filter(sunny, Year == year)
      year_data_cloudy <- filter(cloudy, Year == year)
      year_data_v.cloudy <- filter(v.cloudy, Year == year)
      year_data_snow <- filter(snow, Year == year)
      year_data <- filter(compiled_processed_data, Year == year)

      # Skip this year if there are no non_NA sunny day data
      if (length(!is.na(year_data_sunny[[solar_index]])) == 0) {next}

      # Create jpeg file to contain data
      jpeg(paste0(output_dir, "/",name, "/",
                  year, site, name, ".jpeg"), width = 1400, height = 1400)
     # op <- par(mfcol = c(2,1))

      # Create plot
      plot(year_data_sunny$Day,
           year_data_sunny[[solar_index]],
           xlab = "Day",
           ylab = name,
           main = paste(year, name),
           pch = 18,
           cex = 1.5,
           frame = FALSE)
      # Overlay each data type on plot with appropriate colour if such data type is present
      if(length(!is.na(year_data_cloudy[[solar_index]])) != 0){
        points(year_data_cloudy$Day, year_data_cloudy[[solar_index]], col = "blue", pch = 18, cex = 2)
      }
      if(length(!is.na(year_data_v.cloudy[[solar_index]])) != 0){
        points(year_data_v.cloudy$Day, year_data_v.cloudy[[solar_index]], col = "red", pch = 18, cex = 2)
      }
      if(length(!is.na(year_data_snow[[solar_index]])) != 0){
        points(year_data_snow$Day, year_data_snow[[solar_index]], col = "purple", pch = 18, cex = 2)
      }
      # Add the appropriate model and scaled model data to the plot
      lines(year_data$Day, year_data[[model_index]], col="green")
      lines(year_data$Day, year_data[[adjusted_index]], col="black")

      # plot(year_data$Day,
      #      year_data$roll_mean_max_solar,
      #      xlab = "Day",
      #      ylab = "Max_solar_rolling_mean",
      #      main = paste(year, "Rolling mean"),
      #      pch = 18,
      #      cex = 1.5,
      #      frame = FALSE)
      # lines(year_data$Day, year_data$calc_max_solar, col="green")
      # solar_gradient <- splinefun(year_data$Day, year_data$roll_mean_max_solar)
      # plot(year_data$Day,
      #      solar_gradient(year_data$Day, 1),
      #      xlab = "Day",
      #      ylab = "first derivitive of rolling mean",
      #      main = paste(year, "First derivitive of max_solar"),
      #      pch = 18,
      #      cex = 1.5,
      #      frame = FALSE)
      #par(op)

      # Close jpeg file
      dev.off()
  }
  if (summary){
  # Read summary data for solar data produced by create_csvs
      cloud_data <- read_csv(paste0(output_dir, "/", name, "/",
                                    site, "_", name, "_annual_summary.csv"),col_names = TRUE)
      # remove all years for which there are less than 180 data points
      cloud_data <- cloud_data %>% filter(total > 180)
      # Create jpeg file to contain plots
      jpeg(paste0(output_dir, "/", name, "/",
                  site, "_", name, "_cloud_data.jpeg"), width = 1400, height = 1400)
      # Set file to contain 2 plots, one above the other
      op <- par(mfcol = c(2,1))
      # Create first plot, Relative number of sunny days per year
      plot(cloud_data$Year,
           cloud_data$sunny/cloud_data$total,
           xlab = "year",
           ylab = "proportion of sunny days",
           main = "Relative number of sunny days per year",
           pch = 18,
           cex = 2.5,
           frame = FALSE)
      # Create second plot, relative number of very cloudy days per year
      plot(cloud_data$Year,
           cloud_data$v.cloudy/cloud_data$total,
           xlab = "year",
           ylab = "proportion of v.cloudy days",
           main = "Relative number of very cloudy days per year",
           pch = 18,
           cex = 2.5,
           frame = FALSE)
      par(op)
      # close jpeg file
      dev.off()
  }
  }
  }

# A function to take all the csv files found in the input directory and combine them
# in one dataframe.
gather_data <- function(input_dir){
  # Create list of all csv files in input directory
  file_paths <- list.files(input_dir, pattern = ".*\\.csv", full.names = TRUE)
  # load all files into a list of dataframes
  files <- lapply(file_paths, load_file)
  # bind all elements of the list together into one dataframe
  gathered_data <- do.call(plyr::rbind.fill, files)
  # convert to a tibble
  gathered_data <- dplyr::as_tibble(gathered_data)
  # find names of all columns containing the word solar
  solar_names <- grep("Solar", names(gathered_data), ignore.case = TRUE, value = TRUE)
  # convert all columns to the appropriate class
  gathered_data <- hablar::convert(gathered_data, hablar::chr(file),
                                   hablar::int(Year,
                                       Day),
                                   hablar::num(dplyr::all_of(solar_names)))
  if (sum(hour_identifier(gathered_data, return_fractional_day = FALSE)) > 0){
    gathered_data <- mutate(gathered_data, date = lubridate::ymd_hms(date))
  } else {
  gathered_data <- mutate(gathered_data, date = lubridate::ymd(date))
  }
}

# A function to produce model data for both max and mean solar data. Also adds a fractional_year
# and climate_ID column
produce_models <- function(compiled_solar, site_abreviation, W_or_KW){
  # Set conversion factor based on units of max/min solar data
  if (W_or_KW == "W"){
    con_factor <- 1
  } else {
    con_factor <- 0.001
  }
  processed_solar <- compiled_solar %>%
    ungroup() %>%
    # create fractional_year column for use with functions that do not accept posix data
    mutate(fractional_year = Year + Day/365,
           # Create unique climate ID for easy future binding with further variables
           climate_ID = paste0(site_abreviation, "_", date),
           # Calculate declination, needed to model solar insolation
           # 23.45 is the tilt angle of the earth
           # 172 is the day of year at the summer solstice
           # 360 is degrees in a circle
           # 365 is number of days in a year
           # further details can be found here:
           # https://www.sciencedirect.com/topics/engineering/solar-declination
           declination = (23.45*pi/180)*cos((360/365)*(Day-172)*pi/180),
           # calculate max solar insolation as the insolation at solar noon.
           # 1370 W/m^2 is the solar constant
           # the sunr function calculates the distance between earth and sun in astronomical units
           # 78 is the latitude for Alex Fjord
           # explanation of calculation can be found here:
           # https://files.eric.ed.gov/fulltext/EJ1164362.pdf
           calc_max_solar = (1370/sunr(Day)^2)*(sin(78*pi/180)*sin(declination) +
                                                  cos(78*pi/180)*cos(declination))*con_factor,
           # Any values calculated to be below zero should be set to 0
           calc_max_solar = ifelse(calc_max_solar > 0, calc_max_solar, 0))
  # Calculate the model data on an hourly basis from solar noon till midnight
  for (hour in -12:12){
    hour_model <- (1370/sunr(processed_solar$Day)^2)*(sin(78*pi/180)*sin(processed_solar$declination) +
                                        cos(78*pi/180)*cos(processed_solar$declination)*
                                        cos(15*pi*hour/180))*con_factor
    processed_solar <- processed_solar %>%
      mutate("hour_{hour + 12}" := hour_model)
  }
  # Calculate the model data on an hourly basis for the AM hours.
  # for (hour in -12:-1){
  #   processed_solar <- processed_solar %>%
  #     mutate("hour_{hour + 12}" := (1370/sunr(Day)^2)*(sin(78*pi/180)*sin(declination) +
  #                                                        cos(78*pi/180)*cos(declination)*
  #                                                        cos(15*pi*hour/180)))*con_factor
  # }
  # Convert all negative numbers in the calculated hourly data to 0
  hour_names <- grep("hour", names(processed_solar), value = TRUE)
  for (name in hour_names){
    processed_solar <- processed_solar %>%
      mutate("{name}" := ifelse(.data[[name]] > 0, .data[[name]], 0))
  }
  # Calculate a daily average from all the hourly model data
  processed_solar <- processed_solar %>% ungroup() %>%
    rowwise() %>%
    mutate(calc_mean_solar = mean(c_across(starts_with("hour"))))


  processed_solar
}

# A function which takes all columns with name starting "solar", analyses its data year by year,
# fitting it to model max and mean data. Each year of data for each solar column is then moved to
# either a solar_mean or solar_max column, based on which model fit it better. Also, some data
# has been recorded as 0 when it should be NA. All 0 values found outwith the winter are converted
# to NA.
split_mean_and_max <- function(processed_solar, W_or_KW, output_dir, fix_cols = FALSE){
  # Create empty lists to contain mean and max data
  solar_ave_list <- list()
  solar_max_list <- list()
  # Find all columns with names beginning "solar"
  solar_names <- grep("^solar", names(processed_solar), ignore.case = TRUE, value = TRUE)
  # For each column of solar data, for each year the data spans, check if it more closely mathces
  # the mean or max model data and then append to the appropriate list
  col <- 0
  for (name in solar_names){
    col <- col + 1
    max_years <- vector("integer")
    mean_years <- vector("integer")
    # Convert all non-winter 0 values to NA
    processed_solar <- processed_solar %>%
      mutate("{name}" := ifelse(Day > 60 & Day < 300 & .data[[name]] == 0, NA, .data[[name]]))
    # For each year, append data to the appropriate column
    for (current_year in unique(processed_solar$Year)){
      # Create new dataframe containing only the data from the current year, with the column
      # being analysed re-named current_solar
      year_solar <- processed_solar %>% filter(Year == current_year) %>%
        rename(current_solar = {name})
      # If no non-NA data is present, skip to next year
      if (sum(!is.na(year_solar$current_solar)) == 0){next}

      # Fit data to max and mean models and extract r squared values
      solar_ave_model <- lm(year_solar$current_solar ~ year_solar$calc_mean_solar)
      solar_max_model <- lm(year_solar$current_solar ~ year_solar$calc_max_solar)
      r2_max <- summary(solar_max_model)$r.squared
      r2_ave <- summary(solar_ave_model)$r.squared

      # Append data to either max or mean list based on which r squared value was higher
      if (r2_max >= r2_ave){
        year_solar <- year_solar %>% select(!starts_with("solar")) %>%
          rename("solar_max_{W_or_KW}" := current_solar)
        solar_max_list <- append(solar_max_list, list(year_solar))
        max_years <- append(current_year, max_years)
      } else {
        year_solar <- year_solar %>% select(!starts_with("solar")) %>%
          rename("solar_mean_{W_or_KW}" := current_solar)
        solar_ave_list <- append(solar_ave_list, list(year_solar))
        mean_years <- append(current_year, mean_years)
      }

    }
    if (length(max_years) > 0 & length(mean_years) > 0){
      write_lines(paste0("Solar column ", col, " was split"),
                  file = paste0(output_dir, "/split_columns.txt"), append = TRUE)
      write_lines(paste0("mean years: ", mean_years),
                  file = paste0(output_dir, "/split_columns.txt"), append = TRUE)
      write_lines(paste0("max_years: ", max_years),
                  file = paste0(output_dir, "/split_columns.txt"), append = TRUE)
    }
  }
  # Combine each of the years in both max and mean data together, if present
  average_data <- data.frame()
  max_data <- data.frame()
  if (length(solar_ave_list) > 0){
    average_data <- do.call(rbind, solar_ave_list)
  }
  if (length(solar_max_list) > 0){
    max_data <- do.call(rbind, solar_max_list)
  }
  # Create final dataframe with all of the segregated mean and/or max data
  if (length(average_data) > 0 & length(max_data) > 0){
    processed_solar <- plyr::rbind.fill(max_data, average_data)
  } else if (length(average_data) > 0){
    processed_solar <- average_data
  } else {
    processed_solar <- max_data
  }
  processed_solar
}

# A function which takes the model data produced by produce_models, and produces a separate
# scaled model, scaled to the models associated data such that 90% of the non-winter data points
# lie below the new scaled model data. Also, creates a categorical variable for each data type,
# utilising the scaled model, to describe whether each day was cloudy or sunny.
scale_model <- function(processed_solar, W_or_KW){
  # Find names of all columns containing max/mean solar data
  solar_names <- grep("^solar", names(processed_solar), value = TRUE)
  mean_list <- list()
  max_list <- list()
  # Create new scaled model data for each column of max and/or mean solar data
  for (name in solar_names){
    # Identify associated model data and determine name for new scaled data
    if (grepl("mean", name)){
      col <- "calc_mean_solar"
      col_adjusted <- "calc_mean_adjusted"
    } else {
      col <- "calc_max_solar"
      col_adjusted <- "calc_max_adjusted"
    }
    # Determine column index for model data
    model_index <- which(names(processed_solar) == col, arr.ind = TRUE)

    # Create new scaled model column, with the data set to one third of the un-scaled model data
    processed_solar <- processed_solar %>% ungroup() %>%
      mutate("{col_adjusted}" := .data[[col]]/3)

    # Determine column index of solar data and its associated scaled model data
    data_index <- which(names(processed_solar) == name, arr.ind = TRUE)
    model_index <- which(names(processed_solar) == col_adjusted, arr.ind = TRUE)

    # iteratively increase the scaled model data until 90% of the non-winter data points lie
    # below the model's prediction. Scale each year separately.
    for (year in unique(processed_solar$Year)){
      # Create new dataframe containing all data for the next year
      year_solar <- processed_solar %>% filter(Year == year)
      # Create new dataframe containing max or mean data for the given year, where the data is not NA,
      # and the day is between 60 and 300 (the time that isn't permanently night)
      non_dark_data <- year_solar[[data_index]][year_solar$Day > 60 & year_solar$Day < 300 &
                                                  !is.na(year_solar[[data_index]])]
      # Similarly, create an equivalent dataframe for the scaled model data
      non_dark_model <- year_solar[[model_index]][year_solar$Day > 60 & year_solar$Day < 300 &
                                                    !is.na(year_solar[[data_index]])]
      # If no non-NA data is present for given year, skip to next year
      if (sum(!is.na(non_dark_data)) == 0){next}

      # Calculate the percentage of max or mean solar data points found below the scaled model
      percent_below <- length(non_dark_data[non_dark_data < non_dark_model])/length(non_dark_data)

      # While the percentage of max or mean solar data points lying under the scaled model is less than
      # 90%, increase the scaled model values by 5%.
      while (percent_below <= 0.9){
        year_solar <- year_solar %>% mutate("{col_adjusted}" := 1.05*.data[[col_adjusted]])

        non_dark_model <- year_solar[[model_index]][year_solar$Day > 60 & year_solar$Day < 300 &
                                                      !is.na(year_solar[[data_index]])]

        percent_below <- length(non_dark_data[non_dark_data < non_dark_model])/length(non_dark_data)
      }
      # Create a factor variable which describes whether or not each given day was cloudy based
      # on how far the data point is from the scaled model prediction
      # Set the breaks for the splitting of the solar data based on its units
      if (W_or_KW == "W"){
        cloudy_breaks <- c(-100, 80, 120, 500)
      } else {
        cloudy_breaks <- c(-1, 0.08, 1.2, 500)
      }
      year_solar <- year_solar %>%
        mutate("cloudy_{name}" := cut(.data[[{col_adjusted}]] - .data[[{name}]],
                                      breaks = cloudy_breaks,
                                      labels = c("sunny", "cloudy", "v.cloudy")))
      # Append the year's data to the appropriate list
      if (grepl("mean", name)){
        mean_list <- append(mean_list, list(year_solar))
      } else {
        max_list <- append(max_list, year_solar)
      }
    }
  }
  # Combine all years and mean and/or max data to reconstruct full dataframe containing new columns
  average_data <- data.frame()
  max_data <- data.frame()
  # Combine all years for same data types into one dataframe
  if (length(mean_list) > 0){
    average_data <- do.call(rbind, mean_list)
  }
  if (length(max_list) > 0){
    max_data <- do.call(rbind, max_list)
  }

  #Combine different data types (max and mean) into same dataframe, if both are present
  if (length(average_data) > 0 & length(max_data) > 0){
    processed_solar <- cbind(max_data, average_data)
  } else if (length(average_data) > 0){
    processed_solar <- average_data
  } else {
    processed_solar <- max_data
  }
  processed_solar
}
# A function to determine if there was snow interfereing with the sensor in the spring. It does
# so by checking if at least 15% of the days 70-110 have been determined to be cloudy or v.cloudy
# (cloudy is an indicator that the solar reading is significantly lower than would be expected on a
# sunny day, and the early spring is not expected to have many cloudy days because the sea ice has not
# yet melted). If there are enough cloudy days to indicate snow on the sensor then a rolling average of
# the solar data is taken to average out the noise, and the first derivative is taken to allow the steep
# increase in solar output that is associated with the snow melting out on the sensor. The first sunny
# day (i.e day where the solar output roughly matches the scaled model) after the steep increase is
# determined, and all days previous to this are considered to have snow on the sensor. A new category
# "snow", is added to the cloudy descriptor.
detect_snow <- function(processed_solar){
  # Find all column names which contain max and/or mean solar data
  solar_names <- grep("^solar", names(processed_solar), value = TRUE)
  # For each max or mean column present, analyse each year of data to determine if snow is on the
  # sensor, and update the categorical variable if snow is found
  for (name in solar_names){
    # determine column index of the max or mean solar data
    solar_index <- which(names(processed_solar) == name, arr.ind = TRUE)
    # Take a rolling average of the solar data. Doing so recursively results in a smoother curve
    roll_ave <- rollmean(
      rollmean(
        rollmean(
          processed_solar[[solar_index]],5, fill = NA), 5, fill = NA), 5, fill = NA)
    # Add rolling average to dataframe
    processed_solar <- processed_solar %>% mutate("roll_ave_{name}" := roll_ave)
    # determine column indicies of the rolling average and the relevant categorical variable
    roll_index <- which(names(processed_solar) == paste0("roll_ave_", name), arr.ind = TRUE)
    cloud_index <- which(names(processed_solar) == paste0("cloudy_", name), arr.ind = TRUE)

    # Determine all years for which data is present
  years <- unique(processed_solar$Year)
  # Add the option "snow" to the categorical variable
  levels(processed_solar[[cloud_index]]) <- c(levels(processed_solar[[cloud_index]]), "snow")
  # For each year that is present, determine if snow was present on the sensor, and if it was change
  # the categorical variable to "snow"
  for (year in years){
    processed_spring <- processed_solar %>% filter(Year == year, Day > 70 & Day < 140)
    early_spring <- processed_solar %>% filter(Year == year, Day > 70 & Day < 110)

    # If there are no non-NA data present in the early spring, skip to the next year
    if (length(early_spring[[solar_index]][!is.na(early_spring[[solar_index]])]) == 0) {next}

    # If at least 15% of the days in the early spring are cloudy or v.cloudy, then determine the
    # the day when the steepest increase in the early spring takes place, find the first sunny day from
    # that day, and set the categorical variable to snow for all days that year before the sunny day.
      if ((length(early_spring[[cloud_index]][early_spring[[cloud_index]] == "v.cloudy"]) +
         length(early_spring[[cloud_index]][early_spring[[cloud_index]] == "cloudy"]))/
         length(early_spring[[cloud_index]]) > 0.15){
        # Create a function that predicts the rolling average of the solar data in the spring, given
        # the day. The function skips the final 11 days of the spring as this data is lost taking the
        # rolling average
      solar_func <- splinefun(processed_spring$Day[1:(length(processed_spring$Day) - 11)],
                              processed_spring[[roll_index]][1:(length(processed_spring$Day) - 11)])
      # Determine the greatest gradient on the rolling curve by examining the first derivative of the
      # solar_func
      max_gradient <- max(solar_func(processed_spring$Day[1:(length(processed_spring$Day) - 11)], 1))

      # Determine the day when the steepest gradient took place
      snow_melt_day <- processed_spring$Day[which.closest(
        solar_func(processed_spring$Day[1:(length(processed_spring$Day) - 11)], 1), max_gradient)]
      found_sunny_day <- FALSE
      # While no sunny day has been detected since the steepest gradient, set the snow_melt day to the
      # next day for which there is data and check if it is sunny.
      while (!found_sunny_day){
        if (max(processed_spring$Day) > snow_melt_day &
            processed_spring[[cloud_index]][which(processed_spring$Day > snow_melt_day)][1] != "sunny"){
          snow_melt_day <- processed_spring$Day[which(processed_spring$Day > snow_melt_day)][1]
        } else {
          found_sunny_day <- TRUE
        }
      }
      # Change the categorical vector to "snow" for all days between the start of the year and the
      # snow_melt_day
      processed_solar[[cloud_index]] <- if_else(processed_solar$Year == year &
                                          processed_solar[[solar_index]] > 0 &
                                          processed_solar$Day <= snow_melt_day,
                                        factor("snow", levels = c("sunny",
                                                                  "cloudy",
                                                                  "v.cloudy",
                                                                  "snow")),
                                        processed_solar[[cloud_index]])
    }
  }
  }
  processed_solar
}
# A function which produces model max and mean solar data, determines whether any solar data
# is max or mean, and segregates and labels it accordingly, produces a scaled model that most closely
# fits each year of data available, and uses this scaled model to detect if a day was sunny, cloudy
# or if the sensor had been obscured by snow.
process_data <- function(compiled_solar, site_abreviation, W_or_KW = "W", output_dir, fix_cols){
  # Create model max and/or mean solar data

  processed_solar <- produce_models(compiled_solar, site_abreviation, W_or_KW)

  # Split the solar data by type (mean or max) and label it as such
  processed_solar <- split_mean_and_max(processed_solar, W_or_KW, output_dir, fix_cols)
  # Produce model data scaled to more closely fit the observed data
  processed_solar <- scale_model(processed_solar, W_or_KW)
  # Detect any instances of snow obscuring the sensor in the spring
  processed_solar <- detect_snow(processed_solar)

  processed_solar
}

         #  cloudy = cut(calc_max_solar - Solar, breaks =
          #                c(-1, 0.15, 0.2, 1.5), labels = c("sunny", "cloudy", "v.cloudy"))




  # processed_solar <- cbind(processed_solar,
  #                          roll_mean_max_solar =
  #                            rollmean(rollmean(rollmean(processed_solar$Solar,5, fill = NA), 5, fill = NA), 5, fill = NA))
  # years <- unique(processed_solar$Year)
  # levels(processed_solar$cloudy) <- c(levels(processed_solar$cloudy), "snow")
  # for (year in years){
  #   processed_spring <- processed_solar %>% filter(Year == year, Day > 70 & Day < 140)
  #   early_spring <- processed_solar %>% filter(Year == year, Day > 70 & Day < 100)
  #   if (length(early_spring$cloudy[early_spring$cloudy == "v.cloudy"]) > 0 &
  #       (length(early_spring$cloudy[early_spring$cloudy == "v.cloudy"]) +
  #        length(early_spring$cloudy[early_spring$cloudy == "cloudy"]))/length(early_spring$cloudy) > 0.15){
  #     max_solar_func <- splinefun(processed_spring$Day, processed_spring$roll_mean_max_solar)
  #     max_gradient <- max(max_solar_func(processed_spring$Day, 1))
  #
  #     snow_melt_day <- processed_spring$Day[which.closest(max_solar_func(processed_spring$Day, 1), max_gradient)]
  #     found_sunny_day <- FALSE
  #     while (!found_sunny_day){
  #       if (max(processed_spring$Day) > snow_melt_day &
  #           processed_spring$cloudy[which(processed_spring$Day > snow_melt_day)][1] != "sunny"){
  #         snow_melt_day <- processed_spring$Day[which(processed_spring$Day > snow_melt_day)][1]
  #       } else {
  #         found_sunny_day <- TRUE
  #       }
  #     }
  #
  #     processed_solar$cloudy <- if_else(processed_solar$Year == year &
  #                                         processed_solar$Solar > 0 &
  #                                         processed_solar$Day <= snow_melt_day,
  #                                       factor("snow", levels = c("sunny",
  #                                                                 "cloudy",
  #                                                                 "v.cloudy",
  #                                                                 "snow")),
  #                                       processed_solar$cloudy)
  #   }
  # }


#   processed_solar
# }




