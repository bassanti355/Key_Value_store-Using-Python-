import os
import json
import shutil
from datetime import datetime
import re
def read_json(path):
    with open(path, "r") as file:
        data = json.load(file)
    return data



def compine_files(list_of_jsons):
    result = []
    for json_list in list_of_jsons:
        for item in json_list:
            result.append(item)
    return result




def write_json(path, data):
    with open(path, "w") as file:
        json.dump(data, file, indent=4)



def compaction(directory):
    final_json = []
    current_Date=str(datetime.now())
    table_name = re.match(r"^(.*?)_", os.listdir(directory)[0])
    table_name=table_name.group(1)
    output_path = os.path.join(directory, f"{table_name}_{current_Date}.json")

    files=[]
    for filename in os.listdir(directory):
        if filename.endswith(".json") and filename != "table.json":
            file_path = os.path.join(directory, filename)
            files.append(file_path)
            data = read_json(file_path)
            final_json.append(data)

    archive_dir = os.path.join(os.getcwd(), "archive")
    if not os.path.exists(archive_dir):
        os.mkdir(archive_dir)
        print("archive folder created.")
    
    for i in files:
        shutil.move(i,archive_dir)
    result = compine_files(final_json)
    print(result)
    write_json(output_path, result)