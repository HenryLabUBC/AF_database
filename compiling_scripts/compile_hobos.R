library(data.table)
library(stringr)
library(lubridate)

output_dir <- "<directory for output graphs and CSVs>"
input_dir <- "<directory for source lump files>"

compile_hobo_data <- function(output_dir, input_dir, site, data_selection){
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
                                     "Temp"
    ))
    
    # Remove a needless counting number that was somehow appended to the file names
    # in the making of the lump file.
    site_data[, file_name := sub("\\. \"?[0-9]+\"?", "", file_name)]
    
    # Extract the plot name and type of sensor from the file names, and store in the data.table
    # in columns "Plot" and "data_type"
    extract_plot_and_type(site_data)
    
    # Combine the file_name with the year to produce a unique identifier for each file (some file_names
    # were used multiple times across years)
    site_data[, file_with_year := paste0(file_name, "_", Year)]
    
    # Remove all column headers within the data (not including the top header)
    site_data <- site_data[is.na(number) | number != "#"]
    
    # filter out all data not relevant to the desired sensor type
    site_data <- data_type_selector(site_data, data_selection)
    
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
    
    # to provide a method of checking that the dates were read correctly, the time difference in
    # seconds was calculated between a given entry and the following row. This is stored in the
    # column date_diff. To calculate this, the date of the next row is stored in next_date. To allow
    # easy comparison, the time difference from the previous row is stored in column prev_diff
    site_data[# Offset the dates in fixed_date to create the next_date column. Group by file and plot
        # to prevent transitioning between plot or file resulting in large time differences
        , next_date := c(.SD$fixed_date[2:length(.SD$fixed_date)], NA), by = .(file_name, plot)
    ][ # convert the date back into date format. Data.table likes to return the time in seconds
        # from 1970
        , next_date := as_datetime(next_date)
    ][ # Calculate time difference between next and current date-time
        , date_diff := next_date - fixed_date
    ][ # Offset the differences to produce a column of time differences from one row previous
        , prev_diff := c(NA, .SD$date_diff[1:(length(.SD$date_diff) - 1)]),
        by = .(file_name, plot)
    ]
    
    # The data is in a mixture of Celsius and Fahrenheit. Convert all Fahrenheit to Celsius 
    if (grepl("soil|air", data_selection, ignore.case = TRUE)){
        
        convert_to_celsius(site_data)
    }
    # If multiple entries have somehow been recorded for the same plot and time then they are averaged if the
    # Standard deviation is less than one. Otherwise, or if the values are outwith -60 - 35 oC, then they are
    # replaced with NA
    clean_data(site_data)
    
    # create output_dir if doesn't already exist
    if (!dir.exists(output_dir)){
        dir.create(output_dir, recursive = TRUE)
    }
    # A plot is made for every plot in every year data is available and written to the output directory
    # make_plots(output_dir, site_data, data_selection)
    
    # Write compiled data to file
    year_range <- paste0(site_data[,min(year(fixed_date))],
                         "-",
                         site_data[, max(year(fixed_date))]
    )
    data_col_name <- site_data[1, data_type]
    site_data[, c("site", "date", (data_col_name)) := .(get("site_name"),
                                                        fixed_date,
                                                        Temp
    )]
    selected_cols <- c("site", "plot", "date", data_col_name, "file_name")
    site_data <- site_data[, ..selected_cols]
    browser()
    write.csv(site_data, paste0(output_dir,
                                "/compiled_hobo_",
                                site,
                                "_",
                                data_col_name,
                                "_",
                                year_range,
                                ".csv"
    )
    )
    
    
    site_data
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
        , plot := sub("(\\d+)(\\w+)", "\\2\\1", plot)
    ][ # Remove all 0s and "b"s from the plot names
        , plot := sub("0|b", "", plot)
    ][ # Convert all letters to upper case
        , plot := toupper(plot)
    ][ # Extract the words "soil", "lux", or "_F_" from file names and store in column data_type
        , data_type := str_extract(file_name, "([Ss]oil|[Ll]ux)")
    ][ # Extract the word "test" from file names and store in column test_data
        , test_data := str_extract(file_name, "[Tt]est")
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
        , plot := sub("(?<![A-z])(\\d+)(\\w+)", "\\2\\1", plot, perl = TRUE)
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
    ][ # Extract the word "test" from file names and store in column test_data
        , test_data := str_extract(file_name, "[Tt]est")
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
        , plot := sub("(?<![A-z])(\\d+)(\\w+)", "\\2\\1", plot, perl = TRUE)
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
    ][ # Extract the word "test" from file names and store in column test_data
        , test_data := str_extract(file_name, "[Tt]est")
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
    ][ # Extract the word "test" from file names and store in column test_data
        , test_data := str_extract(file_name, "[Tt]est")
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
        , data_type := str_extract(file_name, "([Ss]oil|[Ll]ux")
    ][ # Extract the word "test" from file names and store in column test_data
        , test_data := str_extract(file_name, "[Tt]est")
    ]
}

convert_to_celsius <- function(datatable){
    
    # Create a list of all the files that contain data in the summer months (this should be all of them)
    summer_files <- unique(datatable[month(fixed_date) %in% c(6,7,8), file_with_year])
    # Create a list of any files that do not contain any summer data
    no_summer <- unique(datatable[!file_with_year %in% summer_files, file_with_year])
    # If any such files exist throw an error; the strategy for identifying C Vs F will need amending
    if (length(no_summer) > 0){
        errorCondition(paste("The following files contained no summer data and so the temperature unit",
                             "could not be determined:", no_summer, sep = "\n"))
    }
    # ensure that the temperature is stored as numeric
    datatable[, Temp := as.numeric(Temp)]
    # Calculate the median temperature in the summer (June, July, Aug), for every file
    datatable[, summer_median :=
                  median(.SD$Temp[month(fixed_date) %in% c(6,7,8)], na.rm = TRUE),
              by=file_with_year
    ][# if the summer median is between 0 and 25, record the temperature as C
        # If between 25 and 70 record the temperature as F, else NA
        , units := ifelse(summer_median > 0 & summer_median < 25,
                          "C",
                          ifelse(summer_median > 25 & summer_median < 70,
                                 "F", NA)),
        by=summer_median
    ][ # for all Fahrenheit values, convert the temperature to C
        units == "F", Temp := round((Temp - 32)/1.8, 3)
    ]
    
}

clean_data <- function(datatable){
    data_type <- datatable[1, data_type]
    if (grepl("temp", data_type, ignore.case = TRUE)){
        min <- -60
        max <- 35
    } else {
        min <- 0
        max <- 1000000
    }
    # For all values with a shared plot and date-time, calculate the standard deviation
    datatable[, Temp_SD := sd(Temp, na.rm = TRUE), by=.(plot, fixed_date)]
    # For all standard deviations less than one, amend the temperature to the average 
    # of the shared results
    datatable[data.table::between(Temp_SD, 0, 1, FALSE), Temp := mean(Temp),
              by=.(plot, fixed_date)]
    # If the SD is greater than one, or the temperature is not between -60 and 35 C then
    # amend the temperature to NA
    datatable[Temp_SD >= 1 | !data.table::between(Temp, get("min"), get("max")) , Temp := NA]
    # Remove all duplicate entries
    unique(datatable, by=c("plot", "fixed_date"))
    datatable
    
}

make_plots <- function(output_dir, datatable, data_type){
    if (!dir.exists(output_dir)){
        dir.create(output_dir)
    }
    plots <- unique(datatable[, plot])
    files <- unique(datatable[, file_with_year])
    for (current_plot in plots){
        for (current_file in files){
            plot_file_data <- datatable[plot == current_plot &
                                            file_with_year == current_file &
                                            !is.na(Temp)]
            if (nrow(plot_file_data) > 0){
                jpeg(paste0(output_dir,
                            "/",
                            current_plot,
                            "_",
                            unique(plot_file_data$Year)[1],
                            ".jpeg"
                ),
                width = 1400, height = 1400)
                plot(plot_file_data$fixed_date,
                     plot_file_data$Temp,
                     xlab = "Date",
                     ylab = data_type)
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
data_type_selector <- function(datatable, data_selection){
    # Synonyms for all the sensor types
    soil_syn <- c("soil", "Soil")
    light_syn <- c("lux", "Lux")
    RH_syn <- c("RH%", "RH_%", "RH__%")
    DewPt_syn <- c("DewPt", "DewPT")
    test_syn <- c("test", "Test")
    
    # If data_selection matches any known sensor type except air then subset the datatable
    # by the data_type column containing all the associated synonyms. If "air" is requested
    # return all data with no data_type recorded (data_type == NA). If data selection is not
    # recognised return an error message.
    # Soil temperature data
    if (grepl("soil", data_selection, ignore.case = TRUE)){
        datatable <- datatable[data_type %in% soil_syn]
        datatable[, data_type := "soil_temp"]
        # Light intensity   
    } else if (grepl("lux|light", data_selection, ignore.case = TRUE)){
        datatable <- datatable[data_type %in% light_syn]
        datatable[, data_type := "light_lux"]
        # Relative Humidity    
    } else if (grepl("humidity|relative|RH", data_selection, ignore.case = TRUE)){
        datatable <- datatable[data_type %in% RH_syn]
        datatable[, data_type := "relative_humidity"]
        # Dew Point    
    } else if (grepl("dew_point|dewpt|dew_pt", data_selection, ignore.case = TRUE)){
        datatable <- datatable[data_type %in% DewPt_syn]
        datatable[, data_type := "dew_point"]
        # Test data
    } else if (grepl("test", data_selection, ignore.case = TRUE)){
        datatable <- datatable[data_type %in% test_syn]
        datatable[, data_type := "dew_point"]
        # Air temperature data    
    } else if (grepl("Air", data_selection, ignore.case = TRUE)){
        datatable <- datatable[is.na(data_type)]
        datatable[, data_type := "air_temp"]
        # Error conditioning
    } else {
        errorCondition("Did not recognise data_selection")
    }
    datatable
}
