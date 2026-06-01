import logging
import os

from masschange.dataproducts.datasetfactory import DatasetFactory
from masschange.dataproducts.utils import get_dataproducts
from masschange.db.ensure import regenerate_dataset_metadata
from masschange.utils.logging import configure_root_logger

if __name__ == '__main__':
    configure_root_logger(log_filepath=None)

    database_name = os.environ['TSDB_DATABASE']
    logging.info(f'Regenerating all dataset metadata for db "{database_name}"')

    for product in get_dataproducts():
        product.ensure()
        for version in product.get_available_versions():
            for instrument_id in product.instrument_ids:
                dataset = DatasetFactory.create(product, version, instrument_id)
                logging.info(f'Updating metadata for dataset "{dataset.get_table_name()}"')
                regenerate_dataset_metadata(dataset)