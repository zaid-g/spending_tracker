import os

import pandas as pd
from dependency_injector.wiring import Provide, inject

from spending_tracker.containers.container import Container
from spending_tracker.engines.analytics_engine import AnalyticsEngine
from spending_tracker.engines.categorization_engine import CategorizationEngine
from spending_tracker.engines.raw_data_processing_engine import RawDataProcessingEngine

pd.set_option("display.max_rows", 10000)


@inject
def main(
    raw_data_processing_engine: RawDataProcessingEngine = Provide[
        Container.raw_data_processing_engine
    ],
    categorization_engine: CategorizationEngine = Provide[
        Container.categorization_engine
    ],
    analytics_engine: AnalyticsEngine = Provide[Container.analytics_engine],
) -> None:
    raw_data_processing_engine.process_raw_data_files()

    categorization_engine.load_data_to_categorize()
    categorization_engine.run_categorization_TUI()

    analytics_engine.load_categorized_transactions()
    analytics_engine.analyze_categorized_transactions()


if __name__ == "__main__":
    container = Container()
    container.config.root_data_folder_path.from_env(
        "SPENDING_TRACKER_DATA_PATH",
        f"{os.path.expanduser('~') + '/spending_tracker_data'}",
        required=True,
    )
    container.config.from_yaml("./spending_tracker/config.yml")
    container.wire(modules=[__name__])

    main()
