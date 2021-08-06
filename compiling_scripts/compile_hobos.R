library(data.table)
library(stringr)
library(lubridate)
library(tundra)

output_dir <- "/home/ross/Desktop/compiled_hobo/"
input_dir <- "/media/ross/BackupPlus/Hoba_data/"

compile_hobo_data <- function(output_dir, input_dir, site){
    # create named list of all site specific extractor functions. The appropriate function will
    # be selected by the site_setup function
    site_extractor_functions <- list(
        Cassiope = extract_plot_and_type_Cas,
        Dryas = extract_plot_and_type_Dry,
        Meadow = extract_plot_and_type_Mead,
        Willow = extract_plot_and_type_Wil,
        Beach = extract_plot_and_type_Beach
    )
    # get site specific data/functions
    site_specific <- site_setup(input_dir, site, site_extractor_functions)
    # The filepath to the lump file containing the site specific data
    file_path <- site_specific[[1]]
    # The standardised name for the site
    site_name <- site_specific[[2]]
    # add the site name to the output directory path
    output_dir <- paste0(output_dir, "/", site_name)
    # The site specific function which extracts the plot name and sensor type from the 
    # file names of the original data (stored in column two of the lump files)
    extract_plot_and_type <- site_specific[[3]]
    # Read in data from the lump file's produced by Cassandra. The lump files contain
    # all hobo data for a given site across all years. The file names of the non-aggregated
    # data are stored in column 2 of the lump file.
    site_data <- fread(file = file_path)
    
    # Rename the columns of the data.table
    col_names <- names(site_data)
    setnames(site_data, col_names, c("Year",
                                     "file_name",
                                     "number",
                                     "Date_Time",
                                     "all_data"
    ))
    
    # Remove a needless counting number that was somehow appended to the file names
    # in the making of the lump file.
    site_data[, file_name := sub("\\. \"?[0-9]+\"?", "", file_name)]
    
    # Extract the plot name and type of sensor from the file names, and store in the data.table
    # in columns "Plot" and "data_type"
    extract_plot_and_type(site_data)
    
    # Convert all numeric data to numeric from character data
    site_data[, all_data := as.numeric(all_data)]
    
    # Combine the file_name with the year to produce a unique identifier for each file (some file_names
    # were used multiple times across years)
    site_data[, file_with_year := paste0(file_name, "_", Year)]
    
    # Remove all column headers within the data (not including the top header)
    site_data <- site_data[is.na(number) | number != "#"]
    
    # filter out all data not relevant to the desired sensor type
    site_data <- data_type_selector(site_data)
    
    # The formatting of the dates in the lump (and original) data has been selectively
    # mangled by Excel performing undesirable auto-formatting. The fix_date function
    # tries to read the date in year/month/day format, before attempting to read anything that
    # fails in month/day/year format. The date is verified by comparing the year of the read
    # date with the year in the Year column of the data.table. The data in the Year column was
    # extracted from the file path of the original data and represents the year the data was
    # saved. The date is considered correct if it has a year equal to the year of saving, or no
    # more than two years less.
    site_data[ # Create a new date column and fill it with the output of reading the
        # original dates in year/month/day format
        , fixed_date := ymd_hms(Date_Time)
    ][ # If ymd_hms returned a date with a year not equal to the year of saving, or no
        # more than two less, then change these dates to NA
        !is.na(fixed_date) &
            (Year < year(fixed_date) |
                 Year - 2 > year(fixed_date)),
        fixed_date := NA
    ][ # For any row that still has NA in fixed_date, attempt to read the original date in
        # month/day/year format
        is.na(fixed_date), fixed_date := mdy_hms(Date_Time)
    ][# Again, any dates with a year not equal or no more than two less than the year of
        # saving is changed to NA
        !is.na(fixed_date) &
            (Year < year(fixed_date) |
                 Year - 2 > year(fixed_date)),
        fixed_date := NA
    ]
    
    # The data is in a mixture of Celsius and Fahrenheit. Convert all Fahrenheit to Celsius 
    convert_to_celsius(site_data)
    
    # If multiple entries have somehow been recorded for the same plot and time then they are averaged if the
    # Standard deviation is less than one. Otherwise, or if the values are outwith -60 - 35 oC, then they are
    # replaced with NA
    site_data <- clean_data(site_data)
    # Two different types of hobo have been used. When the "big" hobos were replaced they were ran
    # concurrently with the "small" hobos for a time. The data collected by the "big" hobos was identified
    # in the file name with "big". This function looks, per plot, at the first time a data point comes from
    # a file called big. Everything before this time is assumed to be a big hobo, and everything after is 
    # assumed to be small if it isn't labeled big
    identify_big_hobos(site_data)
    
    site_data[, hobo_id := hobo_id_extractor(file_name, unique = FALSE)]
    # Get list of data columns
    data_cols <- grep("data", names(site_data), value = TRUE)
    # For every data column create a csv file containing that data and plot information, and also produce plots
    # for each file_year
    for (data_col in data_cols){
        # create output_dir if doesn't already exist
        if (!dir.exists(paste0(output_dir, "/", data_col))){
            dir.create(paste0(output_dir, "/", data_col), recursive = TRUE)
        }
        # create a column in site_data detailing which file_years are associated with non-NA data in data_col
        site_data[, contains_col_data := sum(is.na(.SD)) < nrow(.SD), by=file_with_year, .SDcol = data_col]

        # Create a new datatable containing only the data relevant to data_col
        specific_data.table <- copy(site_data[contains_col_data == TRUE & is.na(test),
                                              .(plot, 
                                                fixed_date,
                                                get(data_col),
                                                file_name,
                                                file_with_year,
                                                hobo_id)
                                              ])
        
        # For some reason the name of the data_col is lost and refuses to be re-appointed using dt[, (data_col) := V3]
        names(specific_data.table)[3] <- data_col
        # A plot is made for every plot in every year data is available and written to the output directory
        make_plots(paste0(output_dir, "/", data_col), specific_data.table)
        
        # Write compiled data to file
        year_range <- paste0(specific_data.table[,min(year(fixed_date))],
                             "-",
                             specific_data.table[, max(year(fixed_date))]
        )
        data_col_name <- sub("_data", "", data_col)
        # Add site column, remove file with year, and rename "fixed_date" to "date'
        specific_data.table[, c("site", "date", "file_with_year") := .(get("site_name"),
                                                                       fixed_date,
                                                                       NULL
        )]
        # re-order the columns
        specific_data.table <- specific_data.table[, .(site, date, plot, get(data_col), file_name, hobo_id)]
        # Give the data column its appropriate name
        names(specific_data.table)[4] <- data_col_name
        
        # convert to a dataframe so that the standardise_plots function can be applied, providing a
        # standardised format for plot information and a unique plot ID for each entry
        setDF(specific_data.table)
        specific_data.table <- tundra::standardise_plots(specific_data.table, output_dir, data_col)
        # Rearange column order for final writing of file
        specific_data.table <- dplyr::select(specific_data.table, site,
                                             plot_id,
                                             date,
                                             plot,
                                             otc_treatment,
                                             snow_treatment,
                                             co2_plot,
                                             .data[[data_col_name]],
                                             file_name,
                                             hobo_id)
        specific_data.table <- dplyr::arrange(specific_data.table, date, plot)
        # Create CSV file containing specific data type
        write.csv(specific_data.table, paste0(output_dir,
                                              "/",
                                              data_col,
                                              "/compiled_hobo_",
                                              site,
                                              "_",
                                              data_col_name,
                                              "_",
                                              year_range,
                                              ".csv"
        )
        )
    }
    
    site_data
}

# Two different types of hobo have been used. When the "big" hobos were replaced they were ran
# concurrently with the "small" hobos for a time. The data collected by the "big" hobos was identified
# in the file name with "big". This function looks, per plot, at the first time a data point comes from
# a file called big. Everything before this time is assumed to be a big hobo, and everything after is 
# assumed to be small if it isn't labeled big
identify_big_hobos <- function(datatable){
    # Identify the first time every plot had a file called big
    datatable[grepl("big", file_name, ignore.case = TRUE), start_small := min(fixed_date), by=plot]
    # Copy the start_new data to every datapoint in the datatable (by plot), as it currently only
    # exists for the subset of datapoints belonging to file_names containing "big"
    datatable[, start_new := unique(.SD$start_small[!is.na(.SD$start_small)]), by=plot]
    # For all data points where the file is not labeled as being a "big" hobo, assume big hobo if the
    # date is before the installation date for small hobos. Else, or if there is no installation date,
    # assume big_hobo == FALSE
    datatable[!grepl("big", file_name, ignore.case = TRUE), 
              big_hobo := ifelse(is.na(start_small), FALSE, ifelse(fixed_date < start_small, TRUE, FALSE))]
    # Set big_hobo to TRUE for all data labeled with big
    datatable[grepl("big", file_name, ignore.case = TRUE), old_hobo := TRUE]
    datatable
}

extract_plot_and_type_Cas <- function(datatable){
    
    datatable[# Remove needless counting number at end of file_name
        , file_name := sub("\\. [0-9]+", "", file_name)
    ][ # Extract the data immediately following the site name, until the first "."
        , plot := sub("cass[_-]?([^_.]*).*",
                      "\\1",
                      file_name,
                      ignore.case = TRUE
        )
    ][ # Remove any hyphens from the extracted plot names
        , plot := sub("-", "", plot)
    ][ # Change the order of the plot name to Letter-Number if not already
        , plot := sub("([0-9]+)([A-z]+)", "\\2\\1", plot)
    ][ # Remove all "b"s from the plot names
        , plot := sub("b", "", plot)
    ][ # Remove all starting 0's from plot names eg T03 becomes T3
        , plot := sub("([A-z])0([0-9])", "\\1\\2", plot)
    ][ # Convert all letters to upper case
        , plot := toupper(plot)
    ][ # Extract the words "soil", "lux", or "_F_" from file names and store in column data_type
        , data_type := str_extract(file_name, "([Ss]oil|[Ll]ux)")
    ][ # Extract the word "test" from file names and store in column test
        , test := str_extract(file_name, "[Tt]est")
    ]
    datatable
}

extract_plot_and_type_Dry <- function(datatable){
    datatable[ # Remove needless counting number at end of file_name
        , file_name := sub("\\. [0-9]+", "", file_name)
    ][ # Extract the data immediately following the site name until first ".", "_", or "-". 
        # Several different site names are accounted for.
        # If 1-letter site name: there must be an underscore, period, or hyphen after
        , plot := sub("^d[^A-z]*[._-]([^_.]*)[._-]?([0-9]*).*",
                      "\\1\\2",
                      file_name,
                      ignore.case = TRUE
        )
    ][ # If whole site name is spelled out
        , plot := sub(".*dryas[._-]?([^_.]*).*", "\\1", plot, ignore.case = TRUE)
    ][ # If "Dryas" is abbreviated to "dry"
        , plot := sub(".*dry(?!a)[._-]?([^_.]*).*",
                      "\\1",
                      plot,
                      ignore.case = TRUE,
                      perl = TRUE
        )
    ][ # If "Dryas" is misspelled "drya"
        , plot := sub(".*drya(?!s)[._-]?([^_.]*).*",
                      "\\1",
                      plot,
                      ignore.case = TRUE,
                      perl = TRUE
        )
    ][  # The extracted data in the plot column must be cleaned up to be coherent.
        # Remove any hyphens from the extracted plot names
        , plot := sub("-", "", plot)
    ][ # Change the order of the plot name to Letter-Number, if not already. 
        # The look ahead (?>!) serves to prevent this line from running if it starts with a letter.
        , plot := sub("(?<![A-z])([0-9]+)([A-z]+)", "\\2\\1", plot, perl = TRUE)
    ][ # Convert all letters to upper case
        , plot := toupper(plot)
    ][ # Standardize treatment codes to C for control and T for treatment (i.e. OTC, W).
        , plot := gsub("OTC", "T", plot)
    ][
        , plot := gsub("W", "T", plot)
    ][
        , plot := gsub("CTR", "C", plot)
    ][
        , plot := gsub("CTL", "C", plot)
        
    ][ # Remove all characters after the plot code (i.e. in "dryasT9a", "T9A" turns into "T9")
        , plot := sub("([A-Z][0-9]{1,2}).*", "\\1", plot)
        
    ][ # Extract the words "soil", "lux", from file names and store in column data_type
        , data_type := str_extract(file_name, "([Ss]oil|[Ll]ux)")
    ][ # Extract the word "test" from file names and store in column test
        , test := str_extract(file_name, "[Tt]est")
    ]
    datatable
}

extract_plot_and_type_Wil <- function(datatable){
    datatable[ # Remove needless counting number at end of file_name
        , file_name := sub("\\. [0-9]+", "", file_name)
    ][ # Extract the data immediately following the site name until first ".", "_", or "-".
        # Several different site names are accounted for.
        # If 1-letter site name: there must be an underscore, period, or hyphen after
        , plot := sub("^w[._-]([^_.]*)[._-]?([0-9]*).*",
                      "\\1\\2",
                      file_name,
                      ignore.case = TRUE
        )
    ][ # If whole site name is spelled out
        , plot := sub(".*Willow[._-]?([^_.]*).*",
                      "\\1",
                      plot,
                      ignore.case = TRUE
        )
    ][ # If "Willow" is abbreviated to "will", with an underscore after
        , plot := sub(".*Will(?!o)[._-]?([^_.]*).*",
                      "\\1",
                      plot,
                      ignore.case = TRUE,
                      perl = TRUE
        )
    ][ # The extracted data in the plot column must be cleaned up to be coherent.
        # Remove any hyphens from the extracted plot names
        , plot := sub("-", "", plot)
    ][ # Change the order of the plot name to Letter-Number, if not already. 
        # The look behind (?<!) serves to prevent this line from running if it starts with a letter.
        , plot := sub("(?<![A-z])([0-9]+)([A-z]+)", "\\2\\1", plot, perl = TRUE)
    ][ # Convert all letters to upper case
        , plot := toupper(plot)
    ][ # Standardize treatment codes to C for control and T for treatment (i.e. OTC, W).
        , plot := gsub("OTC", "T", plot)
    ][
        , plot := gsub("W", "T", plot)
    ][
        , plot := gsub("CTR", "C", plot)
    ][
        , plot := gsub("CTL", "C", plot)
    ][ # Remove all characters after the plot code (i.e. in "dryasT9a", "T9A" turns into "T9")
        , plot := sub("([A-Z][0-9]{1,2}).*", "\\1", plot)
        
    ][ # Extract the words "soil", "lux", "DewPt", or "RH__%" from file names and store in column data_type
        , data_type := str_extract(file_name, "([Ss]oil|[Ll]ux|RH_[_]?%|DewP[Tt])")
    ][ # Extract the word "test" from file names and store in column test
        , test := str_extract(file_name, "[Tt]est")
    ]
}

extract_plot_and_type_Mead <- function(datatable){
    datatable[ # Remove needless counting number at end of file_name
        , file_name := sub("\\. [0-9]+", "", file_name)
    ][ # Extract the data immediately following the site name until first ".", "_", or "-".
        # Several different site names are accounted for. For 1-letter site name: "m" must be at
        # the beginning of the string and there must be an underscore, period, or hyphen after.
        , plot := sub("^m[._-]([^_.]*)[._-]?([0-9]*).*", 
                      "\\1\\2", 
                      file_name,
                      ignore.case = TRUE
        )
    ][ # For 1-letter site name: if "m" is not at the beginning it must be bookended by underscores
        # this is particularly for the super long strings of RH%, DewPt, and T data for
        # T7 (a very messy string that required an exception)
        , plot := sub(".*[._-]m[._-](7)(T)[._-]?.*", 
                      "\\1\\2", 
                      plot,
                      ignore.case = TRUE
        )
    ][ # If whole sitename is spelled out
        , plot := sub(".*Meadow[._-]?([^_.]*).*", 
                      "\\1", 
                      plot, 
                      ignore.case = TRUE
        )
    ][ # If "Meadow" is abbreviated to "MEAD", with an underscore after
        , plot := sub(".*mead(?!o)[._-]?([^_.]*).*", 
                      "\\1", 
                      plot, 
                      ignore.case = TRUE,
                      perl = TRUE
        )
    ][ # The extracted data in the plot column must be cleaned up to be coherent.
        # Remove any hyphens from the extracted plot names
        , plot := sub("-", "", plot)
    ][ # Change the order of the plot name to Letter-Number if not already
        , plot := sub("(?<![A-z])([0-9]{1,2})([A-z]{1,3})", 
                      "\\2\\1", 
                      plot, 
                      perl = TRUE
        )
    ][ # Convert all letters to upper case
        , plot := toupper(plot)
    ][ # Standardize treatment codes to C for control and T for treatment (i.e. OTC, W).
        , plot := gsub("OTC", "T", plot)
    ][
        , plot := gsub("W", "T", plot)
    ][
        , plot := gsub("CTR", "C", plot)
    ][
        , plot := gsub("CTL", "C", plot)
    ][ # Remove all characters after the plot code (i.e. in "dryasT9a", "T9A" turns into "T9")
        , plot := sub("([A-Z][0-9]{1,2}).*", "\\1", plot)
        
    ][ # Extract the words "soil", "lux", "DewPt", or "RH__%" from file names and store in column data_type
        , data_type := str_extract(file_name, "([Ss]oil|[Ll]ux|RH_[_]?%|DewP[Tt])")
    ][ # Extract the word "test" from file names and store in column test
        , test := str_extract(file_name, "[Tt]est")
    ]
}

extract_plot_and_type_Beach <- function(datatable){
    datatable[ # Remove needless counting number at end of file_name
        , file_name := sub("\\. [0-9]+", "", file_name)
    ][ # 2-letter site name: "BR" must be at the beginning of the string and there must be 
        # an underscore, period, or hyphen after.
        , plot := sub("^BR[._-]([^_.]*)[._-]?([0-9]*).*", 
                      "\\1\\2", 
                      file_name,
                      ignore.case = TRUE
        )
    ][ # 2-letter site name: "BS" must be at the beginning of the string and there must
        # be an underscore, period, or hyphen after.
        # The lookahead functions to prevent this function from grabbing "BS_ridge" strings.
        , plot := sub("^BS[._-](?!r)([^_.]*)[._-]?([0-9]*).*", 
                      "\\1\\2", 
                      plot,
                      perl = TRUE,
                      ignore.case = TRUE)
    ][ # Whole sitename is spelled out
        , plot := sub(".*ridge[._-]?([^_.]*).*", 
                      "\\1", 
                      plot, 
                      ignore.case = TRUE)
    ][ # The extracted data in the plot column must be cleaned up to be coherent.
        # Remove any hyphens from the extracted plot names
        , plot := sub("-", "", plot)
    ][ # Change the order of the plot name to Letter-Number if not already
        , plot := sub("(?<![A-z])([0-9]{1,2})([A-z]{1,3})", 
                      "\\2\\1", 
                      plot, 
                      perl = TRUE
        )
    ][ # Convert all letters to upper case
        , plot := toupper(plot)
    ][ # Standardize treatment codes to C for control and T for treatment (i.e. OTC, W).
        , plot := gsub("OTC", "T", plot)
    ][
        , plot := gsub("W", "T", plot)
    ][
        , plot := gsub("CTR", "C", plot)
    ][
        , plot := gsub("CTL", "C", plot)
    ][ # Remove all characters after the plot code (i.e. in "dryasT9a", "T9A" turns into "T9")
        , plot := sub("([A-Z][0-9]{1,2}).*", "\\1", plot)
        
    ][ # Extract the words "soil", "lux", from file names and store in column data_type
        , data_type := str_extract(file_name, "([Ss]oil|[Ll]ux)")
    ][ # Extract the word "test" from file names and store in column test
        , test := str_extract(file_name, "[Tt]est")
    ]
}

convert_to_celsius <- function(datatable){
    
    # Create list of all columns containing temperature data
    temp_col_names <- grep("temp", names(datatable), ignore.case = TRUE, value = TRUE)
    # For each temperature column, convert any Fahrenheit data to Celsius
    for (name in temp_col_names){
        # Create a list of all the files that contain data in the summer months (this should be all of them)
        summer_files <- unique(datatable[month(fixed_date) %in% c(6,7,8) & !is.na(get(name)), file_with_year])
        # Create a list of any files that do not contain any summer data
        no_summer <- unique(datatable[!file_with_year %in% summer_files & !is.na(get(name)), file_with_year])
        # If any such files exist throw an error; the strategy for identifying C Vs F will need amending
        if (length(no_summer) > 0){
            errorCondition(paste("The following files contained no summer data and so the temperature unit",
                                 "could not be determined:", no_summer, sep = "\n"))
        }
        # Name for column to contain summer_median of the data in the named temp_col_name
        summer_median_col <- paste0(name, "_summer_median")
        # Name for column that will store the determined units for the data in the named temp_col_name
        units_col <- paste0(name, "_units")
        # Calculate the median temperature in the summer (June, July, Aug), for every file
        datatable[!is.na(get(name)), (summer_median_col) :=
                      median(.SD[, get(name)][month(fixed_date) %in% c(6,7,8)], na.rm = TRUE),
                  by=file_with_year
        ][# if the summer median is between 0 and 25, record the temperature as C
            # If between 25 and 70 record the temperature as F, else NA
            !is.na(get(name)) , (units_col) := ifelse(get(summer_median_col) > 0 & get(summer_median_col) < 25,
                                                      "C",
                                                      ifelse(get(summer_median_col) > 25 & get(summer_median_col) < 70,
                                                             "F", NA)),
            by= get(summer_median_col)
        ][ # for all Fahrenheit values, convert the temperature to C
            get(units_col) == "F" & !is.na(get(name)), (name) := round((get(name) - 32)/1.8, 3)
        ]
    }
    # Remove all columns added to the datatable by this function
    cols_to_remove <- grep("summer|units", names(datatable), value = TRUE)
    datatable[, (cols_to_remove) := NULL]
    datatable
}

clean_data <- function(datatable){
    # Create list of columns containing data to be compiled
    data_cols <- grep("data", names(datatable), value = TRUE)
    # For every data column, set data to NA if out of bounds and remove duplicates
    for (data_col in data_cols){
        # Set reasonable bounds based on data type
        if (grepl("temp", data_col, ignore.case = TRUE)){
            min <- -60
            max <- 35
        } else {
            min <- 0
            max <- 1000000
        }
        # Make name for column to contain standard deviation for this data_col
        data_col_SD <- paste0(data_col, "_SD")
        # For all values with a shared plot and date-time, calculate the standard deviation
        datatable[!is.na(get(data_col)), (data_col_SD) := sd(get(data_col), na.rm = TRUE), by=.(plot, fixed_date)]
        # For all standard deviations less than one, amend the temperature to the average 
        # of the shared results
        datatable[data.table::between(get(data_col_SD), 0, 1, FALSE), (data_col) := mean(get(data_col)),
                  by=.(plot, fixed_date)]
        # If the SD is greater than one, or the temperature is not between -60 and 35 C then
        # amend the temperature to NA
        datatable[get(data_col_SD) >= 1 | !data.table::between(get(data_col), get("min"), get("max")) , (data_col) := NA]
    }
    # Remove all duplicate entries
    datatable <- unique(datatable, by = c("fixed_date", "plot", data_cols))
    
    # Remove SD columns
    SD_cols <- grep("SD", names(datatable), value = TRUE)
    datatable[, (SD_cols) := NULL]
    datatable
    
}

make_plots <- function(output_dir, datatable){
    # Create dir for plots if it doesn't already exist
    if (!dir.exists(output_dir)){
        dir.create(output_dir)
    }
    # Get name of data column
    data_col <- grep("data", names(datatable), value = TRUE)
    # find all unique plots and files_with_year
    plots <- unique(datatable[, plot])
    files <- unique(datatable[, file_with_year])
    # Make a plot for each file and plot and store in output_dir
    for (current_plot in plots){
        for (current_file in files){
            plot_file_data <- datatable[plot == current_plot &
                                            file_with_year == current_file &
                                            !is.na(get(data_col))]
            if (nrow(plot_file_data) > 0){
                year_range <- paste0(plot_file_data[,min(year(fixed_date))],
                                     "-",
                                     plot_file_data[, max(year(fixed_date))]
                )
                jpeg(paste0(output_dir,
                            "/",
                            current_plot,
                            "_",
                            year_range,
                            ".jpeg"
                ),
                width = 1400, height = 1400)
                plot(plot_file_data$fixed_date,
                     plot_file_data[[data_col]],
                     xlab = "Date",
                     ylab = data_col)
                dev.off()
            }
        }
    }
}

# A function which accepts an input directory, site name and list of site specific functions
# which extract the plot and data_type from file_names stored within the lump files. It returns
# a list containing the file_path to the appropriate site lump file, a standardised name for the 
# site, and the function, specific to the site requested, which will extract the plot and 
# sensor_type data
site_setup <- function(input_dir, site, extractor_functions){
    if (grepl("Cas", site, ignore.case = TRUE)){
        file_path <- paste0(input_dir, "/Cassiope_lump.txt")
        site_name <- "Cassiope"
        extractor_function <- extractor_functions$Cassiope
    } else if (grepl("Dry", site, ignore.case = TRUE)){
        file_path <- paste0(input_dir, "/Dryas_lump.txt")
        site_name <- "Dryas"
        extractor_function <- extractor_functions$Dryas
    } else if (grepl("Wil", site, ignore.case = TRUE)){
        file_path <- paste0(input_dir, "/Willow_lump.txt")
        site_name <- "Willow"
        extractor_function <- extractor_functions$Willow
    } else if (grepl("mead", site, ignore.case = TRUE)){
        file_path <- paste0(input_dir, "/Meadow_lump.txt")
        site_name <- "Meadow"
        extractor_function <- extractor_functions$Meadow
    } else if (grepl("beach|br|ridge", site, ignore.case = TRUE)){
        file_path <- paste0(input_dir, "/BR_lump.txt")
        site_name <- "Beach_Ridge"
        extractor_function <- extractor_functions$Beach
    } else {
        errorCondition("Did not recognise site name")
    }
    list(file_path, site_name, extractor_function)
}

# A function to subset the site specific hobo data to contain only the data pertaining to
# the sensor type selected. It returns a list containing the subset datatable and also standardises
# the name of the data_type
data_type_selector <- function(datatable){
    # Synonyms for all the sensor types. First synonym will be name of column for this data_type and
    # should include the word "data".
    soil_syn <- c("soil_temp_data", "soil", "Soil")
    light_syn <- c("lux_data", "lux", "Lux")
    dew_syn <- c("dew_point", "DewPt")
    humidity_syn <- c("humidity", "RH__%")
    
    # Create list of synonym lists
    syn_list <- list(soil_syn, light_syn, dew_syn, humidity_syn)
    
    # For each sensor type, create a new column with the first element of its synonym list as its
    # name containing all data marked as having that data_type. Also, standardise the synonyms to
    # all match the first entry in the synonym list
    for (sensor_type in syn_list){
        # Create new data column if data type is present
        if (sum(sensor_type %in% datatable$data_type) > 0){
            datatable[data_type %in% sensor_type, data_type := ..sensor_type[[1]]]
            datatable[data_type %in% sensor_type, (sensor_type[[1]]) := all_data]
        }
    }
    # Create a column containing the file_name appended with the year of collection to allow
    # differentiation between files with the same name between years
    datatable[, year_file := paste0(Year, file_name)]
    # get list of filenames associated with light sensors
    light_files <- datatable[data_type == light_syn[[1]], unique(year_file)]
    
    # If light files are present, give any air temperature data from the same file the column name
    # air_temp_sun_data
    if (length(light_files) > 0){
        # remove everything past the first dot from the file names. Everything past the first dot
        # should be the column name that was appended to the original file name. That original file name
        # should be shared with temperature data that was recorded by the same device. This temp data
        # needs segregating, as it was recorded while exposed to direct sunlight, which is not normally
        # the case.
        
        light_files <- gsub("\\..*", "", light_files)
        
        # Remove the column name from the file_name within year_file so that they can be easily compared
        # with the contents of  light_files
        datatable[, year_file := gsub("\\..*", "", year_file)]
        
        # mark out all air temperature data from the same file as light data as sun exposed
        datatable[is.na(data_type) & year_file %in% light_files, data_type := "Air_temp_sun_data"]
        
        # Create column with sun exposed data in it
        datatable[data_type == "Air_temp_sun_data", air_temp_sun_data := all_data]
    }
    
    # Create column for the rest of the air temp data
    datatable[is.na(data_type), air_temp_data := all_data]
    
    # remove columns all_data, data_type, and year_file from datatable
    datatable[, c("all_data", "data_type", "year_file") := NULL]
    
    datatable
}