import pandas as pd

from spending_tracker.engines.data_validation_engine import DataValidationEngine


class AnalyticsEngine:
    def __init__(
        self, root_data_folder_path: str, data_validation_engine: DataValidationEngine
    ):
        self.root_data_folder_path = root_data_folder_path.rstrip("/") + "/"
        self.categorized_transactions_file_path = (
            self.root_data_folder_path + "categorized_transactions.csv"
        )
        self.data_validation_engine = data_validation_engine

    def load_categorized_transactions(self) -> None:
        """Loads categorized transactions and sets as a class attribute"""
        self.categorized_transactions = pd.read_csv(
            self.categorized_transactions_file_path, parse_dates=["datetime"]
        )

    def analyze_categorized_transactions(self) -> tuple:
        """Run analyses on finalized user categorized expenses"""
        self.data_validation_engine.verify_spend_amount_for_mapped_categories(
            self.categorized_transactions
        )
        spend_amount_by_category = (
            self.categorized_transactions.groupby(by=["category"], dropna=False)[
                "amount"
            ]
            .sum()
            .sort_values(ascending=False)
        )
        print(spend_amount_by_category)
        return {"spend_amount_by_category": spend_amount_by_category}
