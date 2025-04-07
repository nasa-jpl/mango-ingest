"""
Implements a filesystem crawler which, for each file which matches any data-product's input file regex:
- inserts a JobManagerEntry row into the database
- moves the file to the staging directory
"""
import logging
from pathlib import Path
from typing import Union, Iterable

from masschange.dataproducts.timeseriesdataproduct import TimeSeriesDataProduct
from masschange.dataproducts.utils import get_dataproducts
from masschange.ingest.utils.enumeration import enumerate_files_in_dir_tree

log = logging.getLogger()
class DataProductFileCrawler:
    src_root_path: Path
    staging_root_path: Path

    def __init__(self, src_root_path: Union[Path, str], staging_root_path: Union[Path, str]):
        self.src_root_path = Path(src_root_path)
        self.staging_root_path = Path(staging_root_path)

    def run(self):
        log.info(f'File-system crawl started for root path {self.src_root_path}')
        for product in get_dataproducts():
            log.debug(f'Crawling for files matching {product.get_full_id()} inputs')
            self.crawl_filepaths_for_product(product)

    def crawl_filepaths_for_product(self, product: TimeSeriesDataProduct) -> Iterable[Path]:
        filepaths = map(Path, enumerate_files_in_dir_tree(str(self.src_root_path)))
        for filepath in filepaths:
            self.catalog_file_in_db(filepath)
            try:
                self.stage_file(filepath)
                self.set_file_sta