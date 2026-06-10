"""
Implements a filesystem crawler which, for each file which matches any data-product's input file regex:
- inserts a JobManagerEntry row into the database
- moves the file to the staging directory
"""
import argparse
import logging
import os
import shutil
import time
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
        self.skipped_files_root_path = self.staging_root_path / "skipped"
        os.makedirs(self.skipped_files_root_path, exist_ok=True)
        self.ingest_manager = IngestManager()
        self._remove_src_files_on_stage = remove_src_files_on_stage

        if not self._remove_src_files_on_stage:
            log.warning('Files are not being removed upon staging - this should only happen during development and '
                        'should be addressed if crawler is being run as a service rather than a single job.')

    def run(self, silence_start_log: bool = False):
        if not silence_start_log:
            log.info(f'File-system crawl started for src root path {self.src_root_path}, staging files at '
                     f'{self.staging_root_path}, {"removing" if self._remove_src_files_on_stage else "not removing"} source '
                     f'files when staged')

        filepaths = map(Path, enumerate_files_in_dir_tree(str(self.src_root_path)))
        for filepath in filepaths:
            self.process(filepath)

    def process(self, src_filepath: Union[Path, str]):
        # TODO: confirm whether or not zipped-file support is actually part of the production requirements, or if it should be excised
        matching_products = [product for product in self.products_cache if product.get_reader().accepts(src_filepath, exclude_zips=False)]
        matching_product_count = len(matching_products)

        if matching_product_count == 0:
            src_filepath = Path(src_filepath)
            archived_filepath =  self.skipped_files_root_path / Path(src_filepath).name
            log.warning(f'Unrecognised file in staging area: {src_filepath} - moving to {archived_filepath}')
            if src_filepath.is_symlink():
                src_filepath.unlink()
            else:
                shutil.move(src_filepath, archived_filepath)
            return

        disambiguation_required = matching_product_count > 1
        log.info(f'Processing file: {src_filepath}{" (with disambiguation)" if disambiguation_required else ""}')

        for product in matching_products:

            if disambiguation_required:
                log.debug(f'Processing {src_filepath}, disambiguated as {product.get_full_id()}')

            try:
                log.debug(f'Registering file for ingestion: {src_filepath}')
                file_ingest_record = self.ingest_manager.register(src_filepath, product)
            except Exception as e:
                log.error(f'Registration of {src_filepath} with ingest manager failed with {e.__class__}: {e}')
                return

            # Guard for empty (zero-byte) files, which should be cleaned up and not staged or ingested
            file_is_empty = os.stat(src_filepath).st_size == 0
            if file_is_empty:
                err_msg = f'File {src_filepath} has zero bytes and will not be staged or ingested - deleting {src_filepath}'
                log.warning(err_msg)
                self.ingest_manager.set_status(file_ingest_record, FileStatus.REJECTED, err_msg=err_msg)
                os.remove(src_filepath)
                return

            src_filename = os.path.basename(src_filepath)
            staging_filename = src_filename if not disambiguation_required else encode_product_into_filename(product, src_filename)
            file_subdir_name = str(file_ingest_record.id)
            staging_dest_dirpath = os.path.join(self.staging_root_path, file_subdir_name)
            staging_dest_filepath = os.path.join(staging_dest_dirpath, staging_filename)

            try:
                # copy file to staging location
                log.debug(f'Staging {src_filepath} into {staging_dest_filepath}')
                os.makedirs(os.path.dirname(staging_dest_filepath), exist_ok=True)
                shutil.copyfile(src_filepath, staging_dest_filepath)

                # set file staged
                self.ingest_manager.set_staged(file_ingest_record, staging_dest_filepath)

            except Exception as e:
                log.error(f'Staging of file {src_filepath} failed with {e.__class__}: "{e}"')
                shutil.rmtree(staging_dest_dirpath, ignore_errors=True)

        if self._remove_src_files_on_stage:
            log.debug(f'Removing source file: {src_filepath}')
            # remove source file, having successfully executed the copy/update
            os.remove(src_filepath)

def encode_product_into_filename(product: DataProduct, filename: str) -> str:
    disambiguation_suffix = product.get_reader().get_disambiguation_suffix()
    return f'{filename}{disambiguation_suffix}'

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
    ap.add_argument('--loop', action='store_true', default=False, dest='loop_execution',)

    configure_root_logger(log_filepath=None)
    args = ap.parse_args()

    crawler = DataProductFileCrawler(args.src, args.dest, args.remove_src_files)
    crawler.run()
    while args.loop_execution:
        time.sleep(1)
        crawler.run(silence_start_log=True)
