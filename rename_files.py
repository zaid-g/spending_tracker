import re
import sys
import numpy as np
import pandas as pd
import os
from os import listdir
from os.path import isfile, join

data_fol_path = sys.argv[1]
files = [f for f in listdir(data_fol_path) if isfile(join(data_fol_path, f))]

files = [file for file in files if file[0] != "."]  # remove hidden files


def is_not_formatted(file):
    match_ = re.search(r"\d{4}-\d{2}_formatted", file)
    if match_ == None:
        return True
    return False


unformatted_files = [file for file in files if is_not_formatted(file)]
print(f"Unformatted files are {unformatted_files}")

sources = ["venmo", "amazon", "chase", "citi", "paypal"]

for file in files:
    file_path = data_fol_path + "/" + file
    print(file_path)
    file_source_identified = False
    # check if venmo
    try:
        df = pd.read_csv(file_path, skiprows=[0, 1])
        if list(df.columns) == [
                "Unnamed: 0",
                "ID",
                "Datetime",
                "Type",
                "Status",
                "Note",
                "From",
                "To",
                "Amount (total)",
                "Amount (tip)",
                "Amount (fee)",
                "Funding Source",
                "Destination",
                "Beginning Balance",
                "Ending Balance",
                "Statement Period Venmo Fees",
                "Terminal Location",
                "Year to Date Venmo Fees",
                "Disclaimer",
            ] :
            file_source_identified = True
            os.rename(file_path, data_fol_path + "/venmo_" + file)
    except:
        pass
