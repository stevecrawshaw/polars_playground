#!/bin/bash

# Check if an argument is provided
if [ $# -eq 0 ]; then
    echo "Please provide the path to the zip files as an argument."
    exit 1
fi

# Use the provided path
zip_path="$1"

# Change to the specified directory
cd "$zip_path" || exit 1

for zipfile in *.zip; do
    # Check if zip files exist
    if [ -e "$zipfile" ]; then
        # Extract the CSV file from the zip
        unzip -j "$zipfile" "*.csv" -d . && \
        # If extraction was successful, delete the zip file
        rm "$zipfile"
    fi
done
