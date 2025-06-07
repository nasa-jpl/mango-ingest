"""
Implements a filesystem crawler which, for each file which matches any data-product's input file regex:
- inserts a JobManagerEntry row into the database
- moves the file to the staging directory
"""
import argparse
import logging
import os
import shutil
from pathlib import Path
from typing import Union, Collection

from masschange.dataproducts.dataproduct import DataProduct
from masschange.dataproducts.utils import get_dataproducts
from masschange.ingest.manager.filestatus import FileStatus
from masschange.ingest.manager.ingestmanager import IngestManager
from masschange.ingest.utils.enumeration import enumerate_files_in_dir_tree
from masschange.utils.logging import configure_root_logger

log = logging.getLogger()


class DataProductFileCrawler:
    ingest_manager: IngestManager
    products_cache: Collection[DataProduct] = get_dataproducts()  # cached to avoid redundant resolution from filesystem

    src_root_path: Path
    staging_root_path: Path

    _remove_src_files_on_stage: bool

    def __init__(self, src_root_path: Union[Path, str], staging_root_path: Union[Path, str], remove_src_files_on_stage: bool):
        self.src_root_path = Path(src_root_path)
        self.staging_root_path = Path(staging_root_path)
        self.ingest_manager = IngestManager()
        self._remove_src_files_on_stage = remove_src_files_on_stage

        if not self._remove_src_files_on_stage:
            log.warning('Files are not being removed upon staging - this should only happen during development and '
                        'should be addressed if crawler is being run as a service rather than a single job.')

    def run(self):
        log.info(f'File-system crawl started for src root path {self.src_root_path}, staging files at '
                 f'{self.staging_root_path}, {"removing" if self._remove_src_files_on_stage else "not removing"} source '
                 f'files when staged')

        filepaths = map(Path, enumerate_files_in_dir_tree(str(self.src_root_path)))
        for filepath in filepaths:
            self.process(filepath)

    def process(self, src_filepath: Union[Path, str]):
        product = self.get_product_matching(src_filepath)
        if product is None:
            return

        log.info(f'Processing file: {src_filepath} as {product.get_full_id()}')

        try:
            log.debug(f'Registering file for ingestion: {src_filepath}')
            file_ingest_record = self.ingest_manager.register(src_filepath, product)
        except Exception as e:
            log.error(f'Registration of {src_filepath} with ingest manager failed with {e.__class__}: {e}')
            return

        filename = os.path.basename(src_filepath)
        file_subdir_name = str(file_ingest_record.id)
        staging_dest_dirpath = os.path.join(self.staging_root_path, file_subdir_name)
        staging_dest_filepath = os.path.join(staging_dest_dirpath, filename)

        try:
            # copy file to staging location
            log.debug(f'Staging {src_filepath} into {staging_dest_filepath}')
            os.makedirs(os.path.dirname(staging_dest_filepath), exist_ok=True)
            shutil.copyfile(src_filepath, staging_dest_filepath)

            # set file staged
            self.ingest_manager.set_staged(file_ingest_record, staging_dest_filepath)

            if self._remove_src_files_on_stage:
                log.debug(f'Removing source file: {src_filepath}')
                # remove source file, having successfully executed the copy/update
                os.remove(src_filepath)

        except Exception as e:
            log.error(f'Staging of file {src_filepath} failed with {e.__class__}: "{e}"')
            shutil.rmtree(staging_dest_dirpath, ignore_errors=True)

    @classmethod
    def get_product_matching(cls, filepath: Path) -> Union[DataProduct, None]:
        matching_products = [product for product in cls.products_cache if product.get_reader().accepts(filepath)]
        matching_product_count = len(matching_products)

        if matching_product_count == 0:
            log.warning(f'Unrecognised file in staging area: {filepath}')
            return None

        if matching_product_count != 1:
            log.error(f'File at {filepath} matches multiple products: {[p.get_full_id() for p in matching_products]}')
            return None

        return matching_products[0]

if __name__ == '__main__':
    ap = argparse.ArgumentParser(
        prog='MassChange Data Crawler',
        description='Given an input root directory and a staging root directory, crawl the input directory and '
                    'register/stage files for ingestion',
    )

    ap.add_argument('--src', required=True, dest='src', help='the root directory containing input data files')
    ap.add_argument('--dest', required=True, dest='dest', help='the root directory under which to stage registered files')
    ap.add_argument('--remove-src-files', action='store_true', default=False, dest='remove_src_files',
                    help='remove files from src upon stage - for safety, must be invoked manually and should be set in '
                         'production')

    configure_root_logger()
    args = ap.parse_args()

    crawler = DataProductFileCrawler(args.src, args.dest, args.remove_src_files)
    crawler.run()
