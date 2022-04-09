library(tundra)
library(tidyverse)

willow_file_path <- "/home/ross/Desktop/thermocouple_data/Temperature.Willow_(93-14).csv"
cassiope_file_path <- "/home/ross/Desktop/thermocouple_data/Cassiope_(92-14)_Temperature_AirSurfSoil.csv"

output_dir <- "/home/ross/Desktop/thermocouple_data/"

combine_thermocouple_data <- function(willow_file_path, cassiope_file_path, output_dir){
  # Get Willow and Cassiope data in long format, and look up treatment and site data using
  # the plot_ids
  willow_data <- format_thermocouple_site_data(willow_file_path, "Will")
  cassiope_data <- format_thermocouple_site_data(cassiope_file_path, "Cas")
  
  # Combine the willow and cassiope data
  thermocouple_data <- plyr::rbind.fill(willow_data, cassiope_data)
  
  # rearrange the order of the columns
  thermocouple_data <- thermocouple_data %>% relocate(plot_id,
                                                     site,
                                                     plot,
                                                     year,
                                                     day,
                                                     contains("treatment"),
                                                     position,
                                                     starts_with("temperature"))
  
  # write data to file
  write_csv(thermocouple_data, paste0(output_dir, "/formatted_thermocouple_data(92-14).csv"))
  
}

# A function which accepts a file_path to previously compiled thermocouple data, as well as a site
# abbreviation. The previous compilation has the data in wide format, with the plot_ids as column
# headers. This function converts to long format, and then uses the plot_ids to lookup the site,
# plot, and treatment data. The site abbreviation is the code at the start of the plot_id used to
# identify the site, "Cas" for cassiope and "Will" for willow.
format_thermocouple_site_data <- function(file_path, site_abreviation){
  thermocouple_data <- load_file(file_path)
  # Extract plot, position, and whether the data is air/soil/surface as well as max/min from the 
  # column names and put this info into columns "plot", "position", and "media_and_type". The Cassiope
  # data does not have position information so needs a different names pattern.
  if (site_abreviation == "Will"){
    name_pattern <- "(.*)_([ncs])_(.*)"
    names <- c("plot_id", "position", "media_and_type")
  } else {
    name_pattern <- "([^0-9]*[0-9]*)_(.*)"
    names <- c("plot_id", "media_and_type")
  }
  thermocouple_data <- thermocouple_data %>% pivot_longer(cols = starts_with(tolower(site_abreviation)),
                                                          names_to = names, 
                                                          names_pattern = name_pattern,
                                                          values_to = "temperature"
                                                          )
  # Remove any data that has no year identified. It is useless and confuses the pivot_wider function
  thermocouple_data <- thermocouple_data %>% filter(!is.empty(year))
  # Convert any error values, aka -6999, to NA
  thermocouple_data <- thermocouple_data %>% 
    mutate(temperature = ifelse(grepl("6999", temperature), NA, temperature))
  # Remove any observations that include no temperature data, these seem to have no meaning and are
  # likely the result of copy pasting columns strangely
  thermocouple_data <- thermocouple_data %>% filter(!is.empty(temperature))
  # Rename any ref columns with no identifier number ref_1, so that they are identified the same
  # from datasets with one or more than one reference
  ref_col_names <- c("ref", "ref_max", "ref_min")
  for (i in seq_along(ref_col_names)){
    if (ref_col_names[i] %in% names(thermocouple_data)){
      new_name <- sub("ref", "ref_1", ref_col_names[i])
      thermocouple_data <- rename(thermocouple_data, "{new_name}" := ref_col_names[i])
    }
  }
  
  # Ensure the day column is not called jday
  if ("jday" %in% names(thermocouple_data)){
    thermocouple_data <- rename(thermocouple_data, day = jday)
  }
  # Expand the number of temperature columns to have one for every type identified in the
  # "media_and_type" column
  thermocouple_data <- thermocouple_data %>% pivot_wider(names_from = "media_and_type", 
                                                         names_prefix = "temperature_",
                                                         values_from = "temperature"
                                                         )
  # Edit plot_id to match those found in tundra::plot_names by replacing "_" with "." and capitalising
  # the site abbreviation
  thermocouple_data <- thermocouple_data %>% mutate(plot_id = gsub("_", ".", plot_id),
                                                    plot_id = sub(tolower(site_abreviation),
                                                                  site_abreviation,
                                                                  plot_id
                                                                  )
                                                    )
  # Add columns for treatment, plot, and site populated by NA, then use treatments_from_plot_id
  # to update this with the real data by looking up the plot_ids in tundra::plot_names
  thermocouple_data <- thermocouple_data %>% mutate(site = NA,
                                                    plot = NA,
                                                    otc_treatment = NA,
                                                    snow_treatment = NA,
                                                    fert_treatment = NA)
  thermocouple_data <- treatments_from_plot_id(thermocouple_data)
  thermocouple_data
}