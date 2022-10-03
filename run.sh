#!/bin/bash

set -e

code_folder=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )


data_folder=$1
csv_folder=$data_folder/csv
cd $csv_folder
ls $csv_folder
read -p "Is this the right folder containing CSV files?\nAnd do you have correct activated python VENV?\nAnd are you sure the csv's contain data up to at most one month from today's date?" -n 1 -r

if [[ $REPLY =~ ^[Yy]$ ]]
then

    echo "Okay, running script..."
    # dedup all files in directory by shasum
    echo "Deduplicating files..."
    declare -A arr
    shopt -s globstar

    for file in **; do
      [[ -f "$file" ]] || continue
       
      read cksm _ < <(md5sum "$file")
      if ((arr[$cksm]++)); then 
        echo "removing $file"
        rm $file
      fi
    done


    echo "Processing raw files..."
    cd $code_folder
    python3 rename_files.py $csv_folder

    echo "Updating Master CSV..."


fi
