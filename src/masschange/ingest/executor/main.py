import argparse
import logging
import os
import shutil
import time
from datetime import timedelta

from masschange.ingest.executor import ingest
from masschange.ingest.executor.errors import EmptyProductException
from masschange.ingest.manager.fileingestrecord import FileIngestRecord
from masschange.ingest.manager.ingestmanager import IngestManager
from masschange.utils.logging import configure_root_logger


class IngestExecutor:
    poll_sleep_delay = timedelta(seconds=1)  # todo: parametrise
    exclude_offred = os.environ.get('MAY_INGEST_OFFRED', '').lower() not in ['true', '1', 'yes']

    def run(self, loop_forever: bool = False):
        ingest_manager = IngestManager()

        while True:
            available_job: FileIngestRecord = ingest_manager.fetch_next_valid_job(exclude_offred=self.exclude_offred)
            if available_job is None and loop_forever:
                time.sleep(self.poll_sleep_delay.total_seconds())
                # logging.debug(f'No jobs available - sleeping {self.poll_sleep_delay.total_seconds()}sec')
                continue
            elif available_job is None and not loop_forever:
                # If no jobs are available, return
                logging.debug(f'No jobs available - terminating ingestion process')
                return

            try:
                logging.debug(f'Ingesting job {available_job.id}: {available_job.staged_filepath}')
                ingest.ingest_file_to_db(available_job.product, available_job.staged_filepath)
            except EmptyProductException:
                logging.debug(f'Valid file had zero records {available_job.id}: {available_job.staged_filepath}')
                pass
            except Exception as e:
                logging.error(f'Failed to ingest file {available_job.staged_filepath}: {e}')
                ingest_manager.set_terminated(available_job, success=False, err_msg=str(e))
                continue

            try:
                ingest_manager.set_terminated(available_job, success=True)
                logging.debug(f'cleaning up {available_job.staged_filepath.parent}')
                shutil.rmtree(available_job.staged_filepath.parent)
            except Exception as e:
                logging.error(f'Failed to register ingest success and clean up staging for file {available_job.staged_filepath}: {e}')

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        prog='MassChange Ingest Executor',
        description='Pull jobs from the database and ingest them',
    )

    ap.add_argument('--loop', action='store_true', default=False, dest='loop_forever',
                    help='Keep polling forever, rather than terminating when no more valid jobs are available')

    configure_root_logger(log_filepath=None)
    args = ap.parse_args()

    executor = IngestExecutor()
    executor.run(loop_forever=args.loop_forever)