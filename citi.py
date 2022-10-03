import sys
import numpy as np
import pandas as pd

file_path = sys.argv[1]

df = pd.read_csv(
    file_path,
)

if (sum(np.isnan(df.Credit.values)) + sum(np.isnan(df.Debit.values))) != len(df):
    raise Exception("Failed to parse debit/credit columns")


def merge_debit_credit_columns(row):
    if np.isnan(row["Debit"]):
        return row["Credit"]
    else:
        return row["Debit"]


df["amount"] = df.apply(lambda row: merge_debit_credit_columns(row), axis=1)
df["uid"] = None
df = df[["uid", "Date", "amount", "Description"]]
df.columns = ["uid", "datetime", "amount", "note"]
df["source"] = "citi"
df["preselected_category"] = None
df = df[["uid", "datetime", "amount", "source", "preselected_category", "note"]]
print(df)
