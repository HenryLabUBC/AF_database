library(tidyverse)
library(tundra)
library(parallel)

#' A function to compile all soil moisture data from the CSV Alex database. Before running this script
#' the bash script file_finder.sh should be run with the input directory pointing to the CSV Alex database,
#' the output directory wherever the output should be written to, and the pattern "moist" - this will produce
#' a text file listing all filepaths in the database that contain "moist", and this should include all soil
#' moisture files.
#' Compile_moisture() goes through each file in files.txt, standardises the names of the columns, adds a 
#' column detailing the filepath of the original file, converts the position into long form, extracts year
#' and day data from the filepath or file metadata if required, and standardises the the plot data while 
#' issuing a unique plot_id using the standardise_plots function. All of the data is then combined into one
#' dataframe, a few corrections and ommisions are performed based on files which have manually been determined
#' to be problematic, duplicate records are removed, a series of plots are produced to allow the completeness
#' of the data to be visualised, and the final dataframe is written to the output_dir.

compile_moisture <- function(){
  output_dir <- "/media/ross/external/Copying_files/archive/moisture/"
  file_paths <- read_lines("/media/ross/external/Copying_files/archive/moisture/files.txt")
  files <- lapply(file_paths, extract_moisture, output_dir = output_dir)
  dataframe <- do.call(plyr::rbind.fill, files)
  # The file "AF_field_data_by_year/2017/2017_Fieldwork_Alex/2017_217_Soilmoisture_LTSites_data_all_Willow.csv"
  # has the wrong day and year within it. change year and day data from this file to 2017 and 217
  dataframe <- dataframe %>% 
    mutate(year = ifelse(
      grepl(paste0("AF_field_data_by_year/2017/2017_Fieldwork_Alex/2017_217_Soilmoisture_LTSites_data_all_Willow.csv|",
                   "AF_field_data_by_year/2017/2017_Fieldwork_Alex/2017_217_Soilmoisture_LTSites_data_lo_Willow.csv|",
                   "Entered_data/Environmental_Data/Soil_Moisture/2017_217_Soilmoisture_LTSites_data_all_Willow.csv|",
                   "Entered_data/Environmental_Data/Soil_Moisture/2017_217_Soilmoisture_LTSites_data_lo_Willow.csv"),
            file), "2017", year),
      day = ifelse(
        grepl(paste0("AF_field_data_by_year/2017/2017_Fieldwork_Alex/2017_217_Soilmoisture_LTSites_data_all_Willow.csv|",
                     "AF_field_data_by_year/2017/2017_Fieldwork_Alex/2017_217_Soilmoisture_LTSites_data_lo_Willow.csv|",
                     "Entered_data/Environmental_Data/Soil_Moisture/2017_217_Soilmoisture_LTSites_data_all_Willow.csv|",
                     "Entered_data/Environmental_Data/Soil_Moisture/2017_217_Soilmoisture_LTSites_data_lo_Willow.csv"),
              file), "217", day))
  
  # the file Old_folders/Old_-_from_Datastick_-_removed_Aug_2010/Sam's_Folder/Soil_moisture_all_sites_93-09_in_progree_SAM_2004.csv
  # is duplicated and poorly annotated data and should be excluded
  problomatic_files <- c("Entered_data/Environmental_Data/Soil_Moisture/Soil_Moisture_ALLYEARS_(93-2011)_93__94__95__01__02.csv",
                         "Old_folders/Old_-_from_Datastick_-_removed_Aug_2010/Sam's_Folder/Soil_moisture_all_sites_93-09_in_progree_SAM_2004.csv",
                         "93-09")
  for (problomatic_file in problomatic_files){
    dataframe <- dataframe %>% filter(!grepl(pattern = {problomatic_file}, file, fixed = TRUE))
  }
  
  # remove duplicate records
  dataframe <- dataframe %>%
    ungroup() %>%
    group_by(plot_id, year, day, position, angle) %>%
    distinct(soil_moisture, .keep_all = TRUE)
  # remove records that contain contradictory values
  dataframe <- dataframe %>% ungroup() %>% 
    group_by(plot_id, year, day, position) %>%
    # calculate standard deviation of all soil moistures that share a plot, position and date
    # if the plot could not be identified then standard deviation is NA
    mutate(moisture_SD = ifelse(!is.na(plot_id), sd(soil_moisture), NA)) %>%
    # Exclude any values for which a standard deviation could be calculated i.e. multiple
    # different soil moistures are recorded for the same place and time
    filter(is.na(moisture_SD))
  
  # convert data to numeric
  dataframe <- dataframe %>% mutate(year = as.numeric(year),
                                    day = as.numeric(day),
                                    soil_moisture = as.numeric(soil_moisture))
  # create graphs
  create_plots(dataframe, paste0(output_dir, "/plots/"))
  # write pertinent data to file
  dataframe <- dataframe %>% select(plot_id,
                                    site,
                                    otc_treatment,
                                    co2_plot,
                                    plot,
                                    year,
                                    day,
                                    position,
                                    angle,
                                    soil_moisture)
  write_csv(dataframe, paste0(output_dir, "/compiled_moisture.csv"))
  dataframe
}

extract_moisture <- function(file_path, output_dir){
  # Load the next soil moisture file. 
  dataframe <- load_file(file_path)
  # If a list of dataframes was returned by load_file, combine into one dataframe
  if (!is.data.frame(dataframe)){
    dataframe <- do.call(plyr::rbind.fill, dataframe)
  }
  
  # Identify columns which had duplicated names in the original data file. load_file automatically
  # adds an iterator to duplicated column names (but not the first instance) separated by "_._"
  duplicate_cols <- grepl("_\\._[0-9]$", names(dataframe))
  # If duplicates were found then the data may be in a "wide" form with the same columns present twice.
  # convert to a "long" form.
  if (sum(duplicate_cols) > 0){
    # isolate left and right dataframes
    dataframe_1 <- dataframe %>% select(!matches("_\\._[0-9]$"))
    dataframe_2 <- dataframe %>% select(matches("_\\._[0-9]$"))
    # remove the iterator from column names in right hand dataframe
    names(dataframe_2) <- str_extract(names(dataframe_2), ".*(?=_\\._[0-9]$)")
    
    # If dataframes are truly of the same form then combine them
    if (identical(names(dataframe_1), names(dataframe_2))){
    # combine left and right dataframes in "long" form
    dataframe <- plyr::rbind.fill(dataframe_1, dataframe_2)
    }
  }
  # Standardise all column names
  dataframe <- standardise_names(dataframe)
  
  # add a column containing the file path
  dataframe <- mutate(dataframe, file = file_path)
  
  # The position the measurement was taken within the plot (north, south, centre), and sometimes
  # the angle the probe was inserted (15, 90), is contained within column names in wide form. Convert
  # to long form. If no position columns are found then "no position" is returned
  dataframe_with_position <- format_positions(dataframe, file_path, output_dir)
  # If no position is found but there is a column name containing the text "moisture", then rename this column
  # "soil_moisture" and create a "position" column populated by NA. Alternatively, if there are two columns
  # with names containing the text "moisture" then there are some files that have two near identical columns
  # of soil moisture data. If the two columns data are within 5% of each other then discard one and keep the
  # other, else discard both and skip to the next file.
  if (!is.data.frame(dataframe_with_position)){
    # If there is one moisture column
    if (sum(grepl("moisture", names(dataframe), ignore.case = TRUE)) == 1){
      # Create position column and fill with NA
      dataframe <- mutate(dataframe, position = NA)
      # Name the moisture column "soil_moisture"
      names(dataframe)[grepl("moisture", names(dataframe))] <- "soil_moisture"
      # If there are two moisture columns
    } else if (sum(grepl("moisture", names(dataframe), ignore.case = TRUE)) == 2){
      # Get names of both columns
      moisture_cols <- grep("moisture", names(dataframe), value = TRUE)
      # Calculate the ratio of the values in one column over the other
      moisture_ratio <- as.numeric(dataframe[[moisture_cols[1]]])/as.numeric(dataframe[[moisture_cols[2]]])
      # Determine the average of this ratio i.e. how similar is the data
      mean_ratio <- mean(moisture_ratio, na.rm = TRUE)
      # If the data is within 5% then discard the second column and rename the first column "soil_moisture"
      # Else skip this file
      if (mean_ratio < 1.05 & mean_ratio > 0.95){
        dataframe <- dataframe %>% select(!matches(moisture_cols[2])) %>% 
          rename(soil_moisture = .data[[moisture_cols[1]]])
      } else {return()}
      # If there are more than 2 or no moisture columns then skip this file
    } else {return()}
    # If a position column was already present in the file then use the long form dataframe produced by
    # the format_positions() function
  } else {
    dataframe <- dataframe_with_position
  }
  
  # If no site column is present try to extract site data from the file meta-data or file_path
  if (!"site" %in% names(dataframe)){
    dataframe <- fill_parameter(dataframe, site_extractor, output_dir, file_path, "site", breaks = FALSE)
  }
  # Extract the year from the file or file path and add to a column named "year"
  dataframe <- fill_parameter(dataframe, year_extractor, output_dir, file_path, "year")
  # Extract the day from the file or file path and add to a column named "day". Search for numbers
  # from 100-366
  DOY_names <- c(',"DOY', ',"D.O.Y', ',"day.of.year', ',"day', ',"date')
  dataframe <- fill_parameter(dataframe,
                              DOY_extractor,
                              output_dir,
                              file_path,
                              "day",
                              breaks = TRUE,
                              DOY_names,
                              three_digits = TRUE)
  # Determine if the day was successfully extracted
  DOY_present <- parameter_present(dataframe, DOY_extractor, col_name = "day",
                                   three_digits = TRUE, breaks = TRUE)
  
  # If the day of year was not found, try again, looking for a calendar date
  if (!DOY_present){
    dataframe <- fill_parameter(dataframe,
                                date_extractor,
                                output_dir,
                                file_path,
                                breaks = TRUE,
                                "day",
                                date_day_month = "date")
    # Check if a date was successfully extracted
    date_present <- parameter_present(dataframe, date_extractor, breaks = TRUE,
                                      col_name = "day", date_day_month = "date")
    # If a date was found, convert it to a DOY
    if (date_present){
      # Extract the dates found by fill_parameter
      date <- date_extractor(dataframe$day)
      # Extract the days from those dates
      day <- date_extractor(date, date_day_month = "day")
      # If one, and only one day is found, determine the DOY and add it to the column "day"
      if (length(day) == 1){
        # Extract month and year from date
        year <- year_extractor(date)
        month <- date_extractor(date, date_day_month = "month")
        # Combine into a posix date format
        date <- lubridate::ymd(paste0(year, month, day))
        # Determine day of year
        DOY <- lubridate::yday(date)
        # Add DOY to "day" column
        dataframe <- mutate(dataframe, day = DOY)
        # Remove the entry in missing_day.txt that was produced when searching for the DOY
        # and not the date
        missing_day_files <- read_lines(paste0(output_dir, "/missing_day.txt"))
        missing_day_files <- gsub(file_path, "", missing_day_files, fixed = TRUE)
        write_lines(missing_day_files, paste0(output_dir, "/missing_day.txt"))
      }
    }
  }
  
  
  # Standardise all plot information
  dataframe <- standardise_plots(dataframe, output_dir, file_path)
  
    dataframe
}

# A function to standardise the column names found in a dataframe relating to treatment, comments,
# day, and conditions as well as ensuring centre is always spelt centre.
standardise_names <- function(dataframe){
  for (name in names(dataframe)){
    otc_names <- c("otc_cntr", "otc", "otc_cntrl", "treatment", "treat")
    comment_names <- c("comment", "comments", "notes")
    day_names <- c("day", "day_of_year", "date")
    condition_names <- c("condition", "conditions")
    
    if (name %in% otc_names){
      names(dataframe)[names(dataframe) == name] <- "treatment"
    }
    if (name %in% comment_names){
      names(dataframe)[names(dataframe) == name] <- "comments"
    }
    if (name %in% day_names){
      names(dataframe)[names(dataframe) == name] <- "day"
    }
    if (name %in% condition_names){
      names(dataframe)[names(dataframe) == name] <- "conditions"
    }
    
  }
  names(dataframe) <- gsub("center(.*)", "centre\\1", names(dataframe))
  dataframe
}

# A function which searches for columns pertaining to positions within a plot that soil moisture
# was measured. If found then these data are converted to long form. Furthermore, sometimes the
# measurement was performed with the probe at different angles. In these cases the angle (15 or 90),
# is separated from the position header and an angle column is generated. The long form dataframe is
# then returned. If no columns named after the cardinal positions are detected then "no position" is
# returned
format_positions <- function(dataframe, file_path, output_dir){
  # The position column names that have been found in soil moisture data
  positions <- c("north", "south", "east", "west", "centre", "north_90", "north_15",
                 "south_90", "south_15", "centre_90", "centre_15", "n", "e", "s", "w", "c")
  # Consider position columns to be present if there are at least 2 matches to the known position names
  position_present <- sum(positions %in% names(dataframe)) > 1
  # If columns named after cardinal points are present then convert the data from wide to long, creating
  # a position, angle, and soil_moisture column.
  if (position_present){
    dataframe <- dataframe %>% pivot_longer(any_of(positions),
                                            values_to = "soil_moisture",
                                            names_to = c("position", "angle"),
                                            names_sep = "_")
    # If at least 90% of the data points are NA or NULL then consider the data missing and return
    # "data_missing"
    data_missing <- sum(is.na(dataframe$soil_moisture) |
                          is.null(dataframe$soil_moisture))/length(dataframe$soil_moisture) > 0.9
    if (data_missing){
      return("data missing")
    }
    # If no columns named after cardinal points is present then write the filepath to a warning file
    # before returning "no position"
  } else {
    write_lines(file_path, append = TRUE, file = paste0(output_dir, "/missing_position.txt"))
    return("no position")
  }
  dataframe
}

# A function which takes the final soil moisture dataframe and produces a series of plots to
# allow the completeness of the data to be evaluated. The data is seperated by site and treatment,
# with a graph for every plot_id and year of soil moisture against measurement day. The plots are
# stored in the svg format because they can get quite large and this permits zooming in on the data.
create_plots <- function(dataframe, output_dir){
  # If the output directory for the plots does not yet exist then create it
  if (!dir.exists(output_dir)){
    dir.create(output_dir)
  }
  # Find all site names present in the dataframe
  sites <- unique(dataframe$site)
  # Create a list of strings of two elements with the first the name of the treatment
  # and the second a regex that will provide a hit if that treatment is conclusively
  # identified by the plot_id. If the standardise_plots function did not have enough data
  # to identify a unique plot then a series of potential plots will have been incorporated
  # in the plot_id, seperated by "/". In this case the treatment is considered unknown.
  treatment_patterns <- list(c("OTC", "^[^/]*\\.o\\.[^/]*$"), 
                             c("Control", "^[^/]*\\.c\\.[^/]*$"),
                             c("Cover", "^[^/]*\\.cv\\.[^/]*$"),
                             c("Unknown", "/"))
  # For each site in the dataframe, cycle through each treatment type creating a plot of 
  # soil moisture against measurement day for all years and plots
  for (site_name in sites){
    # Isolate the data pertaining to only the current site
    site_data <- dataframe %>% filter(site == {site_name})
    # For each treatment type create a plot
    for (treatment_pattern in treatment_patterns){
      # Isolate all data pertaining to the current treatment by applying the regex stored
      # within treatment_patterns to the plot_ids
      treatment_data <- site_data %>% filter(grepl({treatment_pattern[2]}, plot_id))
      # If no data of the current treatment is present then skip to the next one
      if (nrow(treatment_data) == 0){next}
      # Title the plot with the site name and treatment type
      plot_title <- paste(site_name, treatment_pattern[1])
      # Create the plot
      gplot <- ggplot(treatment_data, aes(day, soil_moisture))
      plot <- gplot + geom_point() + facet_grid(year ~ plot_id)
      # Save the plot as a large svg file
      ggsave(paste0(output_dir, "/", plot_title, ".svg"), plot = plot, device = "svg", width = 30, height = 30)
     }
  }
}

find_no_positions <- function(file_path, output_dir){
  dataframe <- load_file(file_path, multi_header = TRUE)
  # If a list of dataframes was returned by load_file, combine into one dataframe
  if (!is.data.frame(dataframe)){
    dataframe <- do.call(plyr::rbind.fill, dataframe)
  }
  # Identify columns which had duplicated names in the original data file. load_file automatically
  # adds an iterator to duplicated column names (but not the first instance) separated by "_._"
  duplicate_cols <- grepl("_\\._[0-9]$", names(dataframe))
  # If duplicates were found then the data is in a "wide" form with the same columns present twice.
  # convert to a "long" form.
  if (sum(duplicate_cols) > 0){
    # isolate left and right dataframes
    dataframe_1 <- dataframe %>% select(!matches("_\\._[0-9]$"))
    dataframe_2 <- dataframe %>% select(matches("_\\._[0-9]$"))
    # remove the iterator from column names in right hand dataframe
    names(dataframe_2) <- str_extract(names(dataframe_2), ".*(?=_\\._[0-9]$)")
    
    # combine left and right dataframes in "long" form
    dataframe <- plyr::rbind.fill(dataframe_1, dataframe_2)
  }
  # Standardise all column names
  dataframe <- standardise_names(dataframe)
  
  # add a column containing the file path
  dataframe <- mutate(dataframe, file = file_path)
  
  # The position the measurement was taken within the plot (north, south, centre), and sometimes
  # the angle the probe was inserted (15, 90), is contained within column names in wide form. Convert
  # to long form. If no position columns are found then "no position" is returned
  dataframe_with_position <- format_positions(dataframe, file_path, output_dir)
  # If no position is found then skip to next file
  if (!is.data.frame(dataframe_with_position) & dataframe_with_position == "no position"){
    if (sum(grepl("weight|mass|bag", names(dataframe), ignore.case = TRUE)) > 0){
      return()
    } else {return(dataframe)}
  } else {
    return()
  }
  return()
}



compile_no_position <- function(){
  output_dir <- "/media/ross/external/Copying_files/archive/moisture/"
  file_paths <- read_lines("/media/ross/external/Copying_files/archive/moisture/files.txt")
  files <- lapply(file_paths, find_no_positions, output_dir = output_dir)
  dataframe <- do.call(plyr::rbind.fill, files)
  dataframe
}