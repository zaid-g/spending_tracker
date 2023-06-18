from dependency_injector import containers, providers
from dependency_injector.wiring import Provide, inject
from spending_tracker.engines.raw_data_processing_engine import RawDataProcessingEngine
from spending_tracker.engines.categorization_engine import CategorizationEngine
from spending_tracker.models.paths import Paths


class CategorizationContainer(containers.DeclarativeContainer):
    config = providers.Configuration()

    import ipdb; ipdb.set_trace()
    paths = providers.Singleton(
        Paths,
        root_data_folder_path=config.root_data_folder_path,
        raw_data_folder_path=config.root_data_folder_path + "raw/",
        cleaned_data_folder_path=config.root_data_folder_path + "cleaned/",
        historical_categorized_data_file_path=config.root_data_folder_path + "history.csv",
    )

    raw_data_processing_engine = providers.Singleton(
        RawDataProcessingEngine,
        paths=paths,
        supported_accounts=config.supported_accounts,
    )
    categorization_engine = providers.Singleton(
        CategorizationEngine, paths=paths
    )
