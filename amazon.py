import sys
import numpy as np
import pandas as pd

file_path = sys.argv[1]

df = pd.read_csv(
    file_path,
)

df = df[["Order ID", "Order Date", "Item Total", "Category", "Title"]]
df.columns = ["uid", "datetime", "amount", "preselected_category", "note"]


chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]


def rm_chars(s):
    l = list(s)
    l = [c for c in l if c in chars]
    return float("".join(l))


df["amount"] = df["amount"].apply(lambda x: rm_chars(x))
assert len(df["uid"].value_counts) == len(df)
print(df)
