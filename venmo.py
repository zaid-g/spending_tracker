import sys
import numpy as np
import pandas as pd

file_path = sys.argv[1]

df = pd.read_csv(
    file_path,
    skiprows=[0, 1],
)
df = df[df.columns[1:]]
df = df.drop(0).reset_index()

print(df.columns)

for i in range(len(df)):
    if pd.isna(df.ID[i]):
        break

df = df.iloc[0:i]

df["Note"] = df.apply(
    lambda row: row["Note"] + "__From: " + row["From"] + ". To: " + row["To"], axis=1
)
df = df[["ID", "Datetime", "Amount (total)", "Note"]]
df.columns = ["uid", "datetime", "amount", "note"]

chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]


def rm_chars(s):
    l = list(s)
    l = [c for c in l if c in chars]
    return float("".join(l))


df["amount"] = df["amount"].apply(lambda x: rm_chars(x))
df["source"] = "venmo"
df["preselected_category"] = None
df = df[["uid", "datetime", "amount", "source", "preselected_category", "note"]]
assert len(df["uid"].value_counts) == len(df)
print(df)
