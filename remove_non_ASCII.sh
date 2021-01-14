#!/bin/bash

echo running remove_non_ASCII.sh
find -path './duplicateCSV*' -type f > files.txt

while (( ! $(wc -l files.txt |sed 's/ .*//') == 0 )); do
	file=$(sed -n 1p files.txt)

	echo $file

	cat $file|iconv -c -t ASCII --output temp.txt 
	mv temp.txt $file
	sed 1d files.txt > temp2.txt
	mv temp2.txt files.txt 
done