library(tidyverse)
library(tundra)
library(parallel)

extract_moisture <- function(file_path, output_dir){
  dataframe <- load_file(file_path, multi_header = TRUE)
  if (!is.data.frame(dataframe)){
    dataframe <- do.call(plyr::rbind.fill, dataframe)
  }
  duplicate_cols <- grepl("_\\._[0-9]$", names(dataframe))
  if (sum(duplicate_cols) > 0){
    dataframe_1 <- dataframe %>% select(!matches("_\\._[0-9]$"))
    dataframe_2 <- dataframe %>% select(matches("_\\._[0-9]$"))
    names(dataframe_2) <- str_extract(names(dataframe_2), ".*(?=_\\._[0-9]$)")

    dataframe <- plyr::rbind.fill(dataframe_1, dataframe_2)
  }

  dataframe <- standardise_names(dataframe)

  dataframe <- mutate(dataframe, file = file_path)

  dataframe <- format_positions(dataframe, file_path, output_dir)
  if (!is.data.frame(dataframe)){return()}

  dataframe <- fill_parameter(dataframe, year_extractor, output_dir, file_path, "year")
  DOY_names <- c(',"DOY', ',"D.O.Y', ',"day.of.year', ',"day', ',"date')
  dataframe <- fill_parameter(dataframe,
                              DOY_extractor,
                              output_dir,
                              file_path,
                              "day",
                              DOY_names,
                              three_digits = TRUE)
  DOY_present <- parameter_present(dataframe, DOY_extractor, col_name = "day", three_digits = TRUE)

  if (!DOY_present){
    dataframe <- fill_parameter(dataframe,
                                date_extractor,
                                output_dir,
                                file_path,
                                "day",
                                date_day_month = "date")
    date_present <- parameter_present(dataframe, date_extractor,
                                      col_name = "day", date_day_month = "date")
    if (date_present){
      date <- date_extractor(dataframe$day)
      day <- date_extractor(date, date_day_month = "day")
      if (length(day) == 1){
        year <- year_extractor(date)
        month <- date_extractor(date, date_day_month = "month")
        date <- lubridate::ymd(paste0(year, month, day))
        DOY <- lubridate::yday(date)
        dataframe <- mutate(dataframe, day = DOY)
        missing_day_files <- read_lines(paste0(output_dir, "/missing_day.txt"))
        missing_day_files <- gsub(file_path, "", missing_day_files, fixed = TRUE)
        write_lines(missing_day_files, paste0(output_dir, "/missing_day.txt"))
      }
    }
  }
  # dataframe <- standardise_sites(dataframe)
  # dataframe <- find_treatment_cols(dataframe, output_dir, file_path)
  # dataframe <- standardise_plot_number(dataframe)
  # dataframe <- standardise_treatment(dataframe)

  dataframe <- standardise_plots(dataframe, output_dir, file_path)



  dataframe
}
compile_moisture <- function(){
  output_dir <- "/media/ross/external/Copying_files/archive/moisture/"
  file_paths <- read_lines("/media/ross/external/Copying_files/archive/moisture/files.txt")
  files <- mclapply(file_paths, extract_moisture, output_dir = output_dir)
  dataframe <- do.call(plyr::rbind.fill, files)
  dataframe
}




standardise_names <- function(dataframe){
  for (name in names(dataframe)){
    otc_names <- c("otc_cntr", "otc", "otc_cntrl", "treatment")
    comment_names <- c("comment", "comments", "notes")
    day_names <- c("day", "day_of_year", "date")
    condition_names <- c("condition", "conditions")

    # if (name %in% otc_names){
    #   names(dataframe)[names(dataframe) == name] <- "otc"
    #}
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

format_positions <- function(dataframe, file_path, output_dir){
  positions <- c("north", "south", "east", "west", "centre", "north_90", "north_15",
                 "south_90", "south_15", "centre_90", "centre_15")
  position_present <- sum(positions %in% names(dataframe)) > 0
  if (position_present){
    dataframe <- dataframe %>% pivot_longer(any_of(positions),
                                            values_to = "soil_moisture",
                                            names_to = c("position", "angle"),
                                            names_sep = "_")
    data_missing <- sum(is.na(dataframe$soil_moisture) |
                          is.null(dataframe$soil_moisture))/length(dataframe$soil_moisture) > 0.9
    if (data_missing){
      return("data missing")
    }
  } else {
    write_lines(file_path, append = TRUE, file = paste0(output_dir, "/missing_position.txt"))
    return("no position")
  }
  dataframe
}
