# Beta (basic functionality works but needs documentation on how to set up & support more account types).

# Python Spending Tracker for US Accounts

Fast command line tool for tracking expenses and providing basic analytics with an emphasis on extensibility, usability, future proof-ness, and low-level user control.

I wrote this script because I was tired of buggy expense tracker applications like Mint and their limitations, such as:
- Unable to map and categorize individual expenses sourced in outside accounts (e.g. Amazon, Venmo). For example, amazon purchases just appear as "AMZN MKTP XXXX-XXXXX-XXXX" on your Credit Card. Was that purchase Electronics/Clothing/Groceries? Who knows...
- Unable to use user-created Regular Expressions to auto-categorize transactions.
- Unable to create arbitrary categorization layers e.g. categories, sub-categories, sub-sub-categories.
- Sharing all financial data with a cloud provider who may sell your data.


# Supported Account Types

- Chase
- CitiBank
- American Express
- Venmo
- Amazon (Items & Refunds)

# How to run

1) Clone repo
2) `pip3 install -r requirements.txt`
3) `mkdir -p <path>/data/raw`
4) Manually download every account's CSV's into the `raw` folder. CSV's should be named with this format:
    YYYY-MM-DD_to_YYYY-MM-DD_<account_name>_.*.csv
5) Run `bash run.sh <path>/data`

Once done, a new file, `<path>/data/history.csv` will store a cleaned up, aggregated, and categorized CSV file of every transaction. The next time you run the app with new CSV files in the `raw` folder, it will remember and apply all the Regular Expression patterns created during your last run on the new transactions (of course, you can override these auto-categorizations).

6) Run `python3 analysis.py <path>/data` for insights.
