library(data.table)
library(stringr)
library(lubridate)
file_path <- "/media/ross/BackupPlus/Hoba_data/"
output_dir <- "/home/ross/Desktop/plots/uncleaned"

compile_hobo_data <- function(file_path, output_dir){
    # Read in data from the lump file's produced by Cassandra. The lump files contain
    # all hobo data for a given site across all years. The file names of the non-aggregated
    # data are stored in column 2 of the lump file.
    Cas_data <- fread(file = paste0(file_path, "/Cassiope_lump.txt"))

    # Rename the columns of the data.table
    col_names <- names(Cas_data)
    setnames(Cas_data, col_names, c("Year",
                                     "file_name",
                                     "number",
                                     "Date_Time",
                                     "Soil_Temp"
                                     ))

    # Remove a needless counting number that was somehow appended to the file names
    # in the making of the lump file.
    Cas_data[, file_name := sub("\\. \"?[0-9]+\"?", "", file_name)]

    # Extract the plot name and type of sensor from the file names, and store in the data.table
    # in columns "Plot" and "data_type"
    extract_plot_and_type(Cas_data)

    # Remove all column headers within the data (not including the top header)
    Cas_data <- Cas_data[is.na(number) | number != "#"]

    # filter out all data not relevant to the desired sensor type
    Cas_data <- Cas_data[data_type == "soil" | data_type == "Soil"]

    # The formatting of the dates in the lump (and original) data has been selectively
    # mangled by Excel performing undesirable auto-formatting. The fix_date function
    # tries to read the date in year/month/day format, before attempting to read anything that
    # fails in month/day/year format. The date is verified by comparing the year of the read
    # date with the year in the Year column of the data.table. The data in the Year column was
    # extracted from the file path of the original data and represents the year the data was
    # saved. The date is considered correct if it has a year equal to the year of saving, or no
    # more than two years less.
    Cas_data[ # Create a new date column and fill it with the output of reading the
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

    Cas_data[
        , next_date := c(.SD$fixed_date[2:length(.SD$fixed_date)], NA), by = .(file_name, plot)
             ][
                 , next_date := as_datetime(next_date)
                 ][
                     , date_diff := next_date - fixed_date
                 ][
                     , prev_diff := c(NA, .SD$date_diff[1:(length(.SD$date_diff) - 1)]),
                     by = .(file_name, plot)
                     ]

    convert_to_celsius(Cas_data)
    clean_data(Cas_data)
    # make_plots(output_dir, Cas_data)


    Cas_data
}

extract_plot_and_type <- function(datatable){

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
        , data_type := str_extract(file_name, "([Ss]oil|[Ll]ux|_F_)")
    ][ # Remove the underscores from _F_
        , data_type := sub("_F_", "F", data_type)
    ]
    datatable
}

convert_to_celsius <- function(datatable){
    datatable[, file_with_year := paste0(file_name, "_", Year)]
    summer_files <- unique(datatable[month(fixed_date) %in% c(6,7,8), file_with_year])
    no_summer <- unique(datatable[!file_with_year %in% summer_files, file_with_year])
    if (length(no_summer) > 0){
        errorCondition(paste("The following files contained no summer data and so the temperature unit",
                             "could not be determined:", no_summer, sep = "\n"))
    }
    datatable[, Soil_Temp := as.numeric(Soil_Temp)]
    datatable[, summer_mean :=
                  median(.SD$Soil_Temp[month(fixed_date) %in% c(6,7,8)], na.rm = TRUE),
        by=file_with_year
        ][
            , units := ifelse(summer_mean > 0 & summer_mean < 25,
                              "C",
                              ifelse(summer_mean > 25 & summer_mean < 70,
                                     "F", NA)),
            by=summer_mean
        ][
            units == "F", Soil_Temp := round((Soil_Temp - 32)/1.8, 3)
        ]

}

clean_data <- function(datatable){
    datatable[, Soil_Temp_SD := sd(Soil_Temp, na.rm = TRUE), by=.(plot, fixed_date)]
    datatable[data.table::between(Soil_Temp_SD, 0, 1, FALSE), Soil_Temp := mean(Soil_Temp),
              by=.(plot, fixed_date)]
    #datatable[Soil_Temp_SD >= 1 | !data.table::between(Soil_Temp, -60, 35) , Soil_Temp := NA]
    #unique(datatable, by=c("plot", "fixed_date"))
    datatable

}

make_plots <- function(output_dir, datatable){
    plots <- unique(datatable[, plot])
    files <- unique(datatable[, file_with_year])
    for (current_plot in plots){
        for (current_file in files){
            plot_file_data <- datatable[plot == current_plot &
                                            file_with_year == current_file &
                                            !is.na(Soil_Temp)]
            if (nrow(plot_file_data) > 0){
                jpeg(paste0(output_dir,
                            "/",
                            current_plot,
                            "_",
                            unique(plot_file_data$Year)[1],
                            ".jpeg"
                            ),
                     width = 1400, height = 1400)
                plot(plot_file_data$fixed_date, plot_file_data$Soil_Temp)
                dev.off()
            }
        }
    }
}
