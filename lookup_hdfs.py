import os
import time

folder_path = "/path/to/folder"  # Replace with the actual folder path

# Check if the folder exists
if dbutils.fs.is_dir(folder_path):
    # Get the current modification timestamp of the folder
    current_timestamp = dbutils.fs.ls(folder_path)[0].modificationTime

    # Check if the previous timestamp file exists
    previous_timestamp_file_path = "/path/to/previous/timestamp/file"  # Replace with the actual previous timestamp file path

    if dbutils.fs.exists(previous_timestamp_file_path):
        # Read the previous timestamp from the file
        with open(previous_timestamp_file_path, "r") as file:
            previous_timestamp = float(file.read())

        # Compare the timestamps
        if current_timestamp > previous_timestamp:
            print("The folder or files have been changed since the last check.")
        else:
            print("The folder or files have not been changed since the last check.")
    else:
        # Create the previous timestamp file and save the current timestamp
        with open(previous_timestamp_file_path, "w") as file:
            file.write(str(current_timestamp))
        print("No previous timestamp file found. Created a new one.")
else:
    print("The specified folder does not exist.")
