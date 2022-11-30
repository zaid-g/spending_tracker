import os
import sys
import pandas as pd


data_fol_path = sys.argv[1] + "/"
historical_categorized_csv_path = data_fol_path + "history.csv"

if os.path.isfile(historical_categorized_csv_path) is False:
    exit("No history file found")
df = pd.read_csv(historical_categorized_csv_path, parse_dates=["datetime"])
amount_by_category = df.groupby(by=["category"])["amount"].sum()

print(f"\n\nTotal by category: {amount_by_category.sort_values(ascending=False)}")


# ---------- [uncategorized transactions warning] ----------:

if df["category"].isna().any():
    print(f"\n\nWarning: uncategorized transactions: {df[df['category'].isna()]}")

# ---------- [mapped sanity checks] ----------:

# amazon
mapped_amazon_total = amount_by_category["mapped/amazon"]
amazon_total = sum(
    df[(df["source"] == "amazon_items") | (df["source"] == "amazon_refunds")].amount
)
print(
    f"\n\nCategory 'mapped/amazon' total = {mapped_amazon_total}, total amazon payments = {amazon_total}"
)

# venmo
mapped_venmo_total = amount_by_category["mapped/venmo"]
venmo_total = sum(df[df["source"] == "venmo"].amount)
print(
    f"\n\nCategory 'mapped/venmo' total = {mapped_venmo_total}, total venmo payments = {venmo_total}"
)
