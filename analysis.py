import os
import sys
import pandas as pd


data_fol_path = sys.argv[1] + "/"
historical_categorized_csv_path = data_fol_path + "history.csv"

if os.path.isfile(historical_categorized_csv_path) is False:
    exit("No history file found")
df = pd.read_csv(historical_categorized_csv_path, parse_dates=["datetime"])
amount_by_category = df.groupby(by=['category'])['amount'].sum()
print(amount_by_category.sort_values(ascending=False))
import ipdb; ipdb.set_trace()

