from dependency_injector import containers, providers

from spending_tracker.engines.analytics_engine import AnalyticsEngine
from spending_tracker.engines.categorization_engine import CategorizationEngine
from spending_tracker.engines.data_validation_engine import DataValidationEngine
from spending_tracker.engines.raw_data_processing_engine import RawDataProcessingEngine


class Container(containers.DeclarativeContainer):
    """Main container class for app"""

    config = providers.Configuration()

    data_validation_engine = providers.Singleton(
        DataValidationEngine,
        supported_accounts=config.supported_accounts,
    )
    raw_data_processing_engine = providers.Singleton(
        RawDataProcessingEngine,
        root_data_folder_path=config.root_data_folder_path,
        supported_accounts=config.supported_accounts,
        data_validation_engine=data_validation_engine,
    )
    categorization_engine = providers.Singleton(
        CategorizationEngine,
        root_data_folder_path=config.root_data_folder_path,
        data_validation_engine=data_validation_engine,
    )
    analytics_engine = providers.Singleton(
        AnalyticsEngine,
        root_data_folder_path=config.root_data_folder_path,
        data_validation_engine=data_validation_engine,
    )
