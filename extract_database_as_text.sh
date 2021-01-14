#!/bin/bash

echo running extract_database_as_text.sh

RmSpaces3.sh
no_crash.sh Excel_to_CSV.R ./files.txt
move_csv.sh 
remove_non_ASCII.sh