import os
import json
import sys
import pandas as pd


class AnalyticsEngine:
    def __init__(
        self, root_data_folder_path: str, data_validation_engine: DataValidationEngine
    ):
        self.root_data_folder_path = root_data_folder_path
        self.categorized_transactions_file_path = (
            self.root_data_folder_path + "categorized_transactions.csv"
        )
        self.data_validation_engine = data_validation_engine
        self.categorized_transactions = self.load_categorized_transactions()

    def load_categorized_transactions(self) -> pd.DataFrame:
        categorized_transactions = pd.read_csv(
            self.categorized_transactions_file_path, parse_dates=["datetime"]
        )
        if categorized_transactions["category"].isna().any():
            print(
                f"\n\nWarning: uncategorized transactions: {categorized_transactions[categorized_transactions['category'].isna()]}"
            )
        self.data_validation_engine.verify_spend_amount_for_mapped_categories(
            categorized_transactions
        )

    def analyze_categorized_transactions(self, categorized_transactions) -> tuple:
        self.data_validation_engine.verify_spend_amount_for_mapped_categories(
            categorized_transactions
        )
        spend_amount_by_category = (
            categorized_transactions.groupby(by=["category"])["amount"]
            .sum()
            .sort_values(ascending=False)
        )
        return {"spend_amount_by_category": spend_amount_by_category}

    def print_results(self, **kwargs) -> None:
        print(f"\n\nTotal by category: {spend_amount_by_category}")
