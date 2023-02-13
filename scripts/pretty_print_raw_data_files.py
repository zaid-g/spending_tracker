import json
import sys
import os

data_fol_path = sys.argv[1] + "/"

def extract_date_from_file_name(file_name):
    return int(file_name[0:4] + file_name[5:7] + file_name[8:10])
    
with open(data_fol_path + "config.json", "rb") as f:
    config = json.load(f)

print("\nPrinting files...\n")

file_names = []

for fname in os.listdir(data_fol_path + "raw/"):
    file_names.append(fname)
    if not any(account in fname for account in config["accounts"]):
        raise Exception(
            f"File {fname} is missing account name"
        )


for account in config["accounts"]:
    account_files = [file_name for file_name in file_names if account in file_name]
    account_files.sort(key=extract_date_from_file_name)
    for account_file in account_files:
        print(account_file)
    print()
