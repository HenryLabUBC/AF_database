#' A limited function to avoid duplicated column names. It takes a dataframe and
#' the name which should not be duplicated as inputs, and returns the dataframe with
#' names edited as required. If the name is not present, or is not duplicated then the
#' dataframe is returned unedited, If there is more than one instance of the name then
#' the second instance onwards will be iterated as <name>_<iterator>, with the iterator
#' starting at 2. Unlike vctrs::vec_as_names, this ensures a greater degree of continuity
#' of final result, as the name does not depend on the column number in which it was found.
#' @param dataframe A dataframe which will have its column names examined and potentially
#' fixed
#' @param name A character string of the name to search for duplicates of
#' @export

itterate_duplicated_col_names <- function(dataframe, name){

count <- 2
# While there is more than one instance iterate the second one
while (length(which(names(dataframe) == name)) > 1){
  indicies <- which(names(dataframe) == name, arr.ind = TRUE)
  names(dataframe)[indicies[2]] <- paste0(name, "_", count)
  count <- count + 1
}
dataframe
}