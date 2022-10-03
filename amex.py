import sys
import numpy as np
import pandas as pd

file_path = sys.argv[1]

df = pd.read_csv(
    file_path,
)

df["uid"] = None
df = df[["uid", "Date", "Amount", "Description"]]
df.columns = ["uid", "datetime", "amount", "note"]
df["source"] = "amex"
df["preselected_category"] = None
df = df[["uid", "datetime", "amount", "source", "preselected_category", "note"]]
print(df)
