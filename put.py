import os
import json
import shutil
from datetime import datetime
import re

def read_json(path):
    """
    Reads JSON data from the given file path.
    """
    with open(path, "r") as file:
        data = json.load(file)
    return data

def compine_files(list_of_jsons):
    """
    Combines multiple lists of JSON objects into a single list.
    """
    result = []
    for json_list in list_of_jsons:
        for item in json_list:
            result.append(item)
    return result

def write_json(path, data):
    """
    Writes JSON data to the specified file path with indentation.
    """
    with open(path, "w") as file:
        json.dump(data, file, indent=4)

def archieve(archive_dir: str, files: list):
    """
    Moves all given files to an archive directory. Creates the directory if it doesn't exist.
    """
    if not os.path.exists(archive_dir):
        os.mkdir(archive_dir)
        print("Archive folder created.")
    
    for file in files:
        shutil.move(file, archive_dir)

def compaction(directory):
    """
    Compacts all .json files in the directory (excluding 'table.json'):
    - Reads and combines their contents.
    - Moves original files to an archive folder.
    - Writes a new compacted file with current timestamp.
    """
    final_json = []

    # Generate a timestamp and get base table name
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    sample_file = os.listdir(directory)[0]
    table_name = re.match(r"^(.*?)_", sample_file).group(1)
    output_path = os.path.join(directory, f"{table_name}_{current_date}.json")

    files = []

    # Collect and read data from valid JSON files
    for filename in os.listdir(directory):
        if filename.endswith(".json") and filename != "table.json":
            file_path = os.path.join(directory, filename)
            files.append(file_path)
            data = read_json(file_path)
            final_json.append(data)

    # Archive the original JSON files
    archive_dir = os.path.join(os.getcwd(), "archive")
    archieve(archive_dir, files)

    # Combine all lists into one and write the final output
    result = compine_files(final_json)
    print(result)
    write_json(output_path, result)


compaction("./test_table")