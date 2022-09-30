import sys
import numpy as np
import pandas as pd

file_path = sys.argv[1]

df = pd.read_csv(
    file_path,
)

import ipdb; ipdb.set_trace()
df = df[["Order ID", "Order Date", "Item Total"]]
df.columns = ["id", "datetime", "amount"]
df["note"] = None


chars = ["-", ".", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]


def rm_chars(s):
    l = list(s)
    l = [c for c in l if c in chars]
    return float("".join(l))


df["amount"] = df["amount"].apply(lambda x: rm_chars(x))
print(df)
