import numpy as np
import pandas as pd

df = pd.read_csv("/home/debian11/doc/projects/spending_tracker/my_data/venmo_statement.csv", skiprows=[0, 1])
df = df[df.columns[1:]]
df = df.drop(0).reset_index()

for i in range(len(df)):
    if pd.isna(df.ID[i]):
        break

df = df.iloc[0:i]

df = df[["Datetime",  "Amount (total)", "Note"]]
df.columns = ["datetime", "amount", "note"]

chars = ['-', '.', '0','1','2','3','4','5','6','7','8','9']
def rm_chars(s):
    l = list(s)
    l = [c for c in l if c in chars]
    return float(''.join(l))

df["amount"] = df["amount"].apply(lambda x: rm_chars(x))
print(df)
