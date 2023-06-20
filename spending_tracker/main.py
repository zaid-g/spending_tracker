from dependency_injector.wiring import Provide, inject
from spending_tracker.engines.raw_data_processing_engine import RawDataProcessingEngine
from spending_tracker.engines.categorization_engine import CategorizationEngine
from spending_tracker.containers.categorization_container import CategorizationContainer
from copy import deepcopy
import datetime
import ipdb
import glob
import sys
import pandas as pd
import os
import json

pd.set_option("display.max_rows", 10000)


@inject
def main(
    raw_data_processing_engine: RawDataProcessingEngine = Provide[
        CategorizationContainer.raw_data_processing_engine
    ],
    categorization_engine: CategorizationEngine = Provide[
        CategorizationContainer.categorization_engine
    ],
) -> None:
    raw_data_processing_engine.process_raw_data_files()


if __name__ == "__main__":
    categorization_container = CategorizationContainer()
    categorization_container.config.root_data_folder_path.from_env(
        "SPENDING_TRACKER_DATA_PATH", required=True
    )
    categorization_container.config.from_yaml("./config.yml")
    categorization_container.wire(modules=[__name__])

    main()
