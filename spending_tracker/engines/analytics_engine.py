import os
import json
import sys
import pandas as pd
from spending_tracker.engines.data_validation_engine import DataValidationEngine


class AnalyticsEngine:
    def __init__(
        self, root_data_folder_path: str, data_validation_engine: DataValidationEngine
    ):
        self.root_data_folder_path = root_data_folder_path
        self.categorized_transactions_file_path = (
            self.root_data_folder_path + "categorized_transactions.csv"
        )
        self.data_validation_engine = data_validation_engine

    def load_categorized_transactions(self) -> None:
        """Loads categorized transactions and sets as a class attribute"""
        self.categorized_transactions = pd.read_csv(
            self.categorized_transactions_file_path, parse_dates=["datetime"]
        )
        if self.categorized_transactions["category"].isna().any():
            print(
                f"\n\nWarning: uncategorized transactions: {self.categorized_transactions[self.categorized_transactions['category'].isna()]}"
            )
        self.data_validation_engine.verify_spend_amount_for_mapped_categories(
            self.categorized_transactions
        )

    def analyze_categorized_transactions(self) -> tuple:
        self.data_validation_engine.verify_spend_amount_for_mapped_categories(
            self.categorized_transactions
        )
        spend_amount_by_category = (
            self.categorized_transactions.groupby(by=["category"])["amount"]
            .sum()
            .sort_values(ascending=False)
        )
        return {"spend_amount_by_category": spend_amount_by_category}

    def print_results(self, **kwargs) -> None:
        print(f"\n\nTotal by category: {spend_amount_by_category}")
