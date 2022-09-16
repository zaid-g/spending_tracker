import numpy as np
import pandas as pd

df = pd.read_csv("my_data/venmo_statement.csv", skiprows=[0, 1])
df = df[df.columns[1:]]
df = df.drop(0).reset_index()

for i in range(len(df)):
    if pd.isna(df.ID[i]):
        break

df = df.iloc[0:i]
