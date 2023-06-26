# Python Spending Tracker

Python application for tracking expenses and providing analytics with an emphasis on extensibility, usability, future proof-ness, simplicity, and user control. PR's encouraged to support more accounts, see below.

![Alt text](./screenshots/screenshot.png?raw=true "Title")

# Why Python Spending Tracker?

Commercial expense trackers have their limitations, such as:

- Unable to map and categorize expenses sourced in outside accounts. For example, Amazon purchases are often grouped and just appear as "AMZN MKTP XXXX-XXXXX-XXXX" on your Credit Card. Was that purchase Electronics/Clothing/Groceries? Was it a single or multiple items? Finding out involves much manual work...
- Unable to use powerful Regular Expressions to auto-categorize transactions using user defined patterns.
- Sharing finances with 3rd party who may not have strong privacy protections.
- Getting locked to one particular vendor. Hard to switch and keep your data.
- Paid

This application offers an intuitive terminal user interface that allows for fast categorization and analytics of all your expenses, without any of these limitations.

# Supported Accounts

- Amazon (Purchases & Refunds)
- Citi Double Cash Card
- Citi Custom Cash Card
- Chase Freedom Unlimited Card
- Chase Debit Card
- American Express Blue Cash Preferred Card

To support more accounts, please submit a PR with these changes:
1) Add the account name and columns of the raw downloaded csv to `spending_tracker/config.yml`
2) Add method with the same name in `spending_tracker/engines/raw_data_processing_engine.py` for that account.

# How to run

Python 3.11 required.

1. Clone repo.
2. Create and activate virtual environment `python3 -m venv .spending_tracker`, `. ./.spending_tracker/bin/activate`.
3. `pip3 install -r requirements.txt`
4. Create directory for your raw data files:
    `mkdir -p <path>/spending_tracker_data/raw`
5. Run `export SPENDING_TRACKER_DATA_PATH=<path>/spending_tracker_data` or add to profile.
5. Download CSV's for your accounts into the `raw` folder. CSV's should be named with this format:
   `YYYY-MM-DD_to_YYYY-MM-DD*<account_name>*.csv`. For example `2022-09-01_to_2022-12-31_citi_double_cash_2022_1_lastyear2022.csv`
6. Run `python3 -m spending_tracker.main`

Once done, a new file, `<path>/data/categorized_transactions.csv` will store your categorized transactions. The next time you run the app with new files in the `raw` folder, it will remember and apply all the Regular Expression patterns created in previous runs on the new transactions (of course, you can override these auto-categorizations).
