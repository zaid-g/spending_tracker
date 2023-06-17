import json
import datetime
from dateutil import parser
import sys
import os

data_fol_path = sys.argv[1] + "/"


def ensure_proper_date_range(file_names):
    # first assert that to date > from date
    for i in range(len(file_names)):
        from_date = parser.parse(
            file_names[i][0:4] + file_names[i][5:7] + file_names[i][8:10]
        )
        to_date = parser.parse(
            file_names[i][14:18] + file_names[i][19:21] + file_names[i][22:24]
        )
        assert (
            to_date > from_date
        ), f"Error: Found file with from date greater than to date {file_names[i]}"
    # next assert no gaps/overlaps in date ranges
    for i in range(len(file_names) - 1):
        from_date_next = parser.parse(
            file_names[i + 1][0:4] + file_names[i + 1][5:7] + file_names[i + 1][8:10]
        )
        to_date = parser.parse(
            file_names[i][14:18] + file_names[i][19:21] + file_names[i][22:24]
        )
        assert from_date_next - to_date == datetime.timedelta(
            days=1
        ), f"Error: Found gaps/overlaps in date range for  {file_names[i], file_names[i+1]}"


def extract_date_from_file_name(file_name):
    return int(file_name[0:4] + file_name[5:7] + file_name[8:10])


with open(data_fol_path + "config.json", "rb") as f:
    config = json.load(f)

print("\nPrinting files...\n")

file_names = []

for fname in os.listdir(data_fol_path + "raw/"):
    file_names.append(fname)
    if not any(account in fname for account in config["accounts"]):
        raise Exception(f"File {fname} is missing account name")


for account in config["accounts"]:
    account_files = [file_name for file_name in file_names if account in file_name]
    account_files.sort(key=extract_date_from_file_name)
    ensure_proper_date_range(account_files)
    print(f"***   {account}   ***\n")
    for account_file in account_files:
        print(account_file)
    print()
