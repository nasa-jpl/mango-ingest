import argparse
import logging
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.dataproducts.implementations.gracefo.offred.offred import GraceFOOffredDataProduct
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.db.data.caggs import refresh_continuous_aggregates
from masschange.db.data.ensure import ensure_dataset_caggs_exist
from masschange.utils.logging import configure_root_logger

from masschange.utils.misc import get_human_readable_elapsed_since
from masschange.utils.timespan import TimeSpan

configure_root_logger(log_filepath=None, log_level=logging.DEBUG)

log = logging.getLogger()


def parse_utc_datetime(date_string: str) -> datetime:
    """Parses an ISO 8601 string or a YYYY-MM-DD date into a UTC datetime object."""
    try:
        # If the user provided just a date (YYYY-MM-DD), append midnight UTC
        if len(date_string) == 10 and date_string.count('-') == 2:
            clean_str = f"{date_string}T00:00:00+00:00"
        else:
            # Replace 'Z' with '+00:00' to support older Python versions (< 3.11)
            clean_str = date_string.replace('Z', '+00:00')

        dt = datetime.fromisoformat(clean_str)

        # If the user omitted a timezone, default to UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt
    except ValueError:
        raise argparse.ArgumentTypeError(
            f"Invalid format: '{date_string}'. Expected full ISO format (e.g., '2026-07-16T12:00:00Z') "
            f"or date only (e.g., '2026-07-16')."
        )


def get_args():
    parser = argparse.ArgumentParser(
        prog='Script to aggregate OFFRED data',
        description='Given an input root directory and a staging root directory, crawl the input directory and '
                    'register/stage files for ingestion',
    )

    parser.add_argument(
        'instrument',
        choices=['GF1', 'GF2'],
        help="The target instrument (GF1 or GF2)."
    )

    parser.add_argument(
        'start',
        type=parse_utc_datetime,
        help="Start date and time in ISO format (e.g., 2026-07-16T12:00:00Z), or just a date YYYY-MM-DD."
    )

    parser.add_argument(
        'end',
        type=parse_utc_datetime,
        help="End date and time in ISO format (e.g., 2026-07-16T12:00:00Z), or just a date YYYY-MM-DD."
    )

    parser.add_argument(
        '-n', '--num-threads',
        type=int,
        default=1,
        help='Number of threads to use for concurrent processing (default: 1)'
    )

    parser.add_argument(
        '-m', '--max-days-per-chunk',
        type=float,
        default=1.0,
        help='Maximum number of days a thread should process in a single aggregation step (default: 1)'
    )

    return parser.parse_args()


# 4. Define the worker function that each thread will execute
def worker(thread_span, dataset, max_days_per_chunk):
    """
    Takes the large time span allocated to this thread and processes it
    sequentially in smaller chunks.
    """
    chunk_duration = timedelta(days=max_days_per_chunk)
    current_start = thread_span.begin

    processed_chunks = []

    while current_start < thread_span.end:
        current_end = min(current_start + chunk_duration, thread_span.end)
        chunk_span = TimeSpan(begin=current_start, end=current_end)

        # Log the internal chunking for visibility
        log.debug(f"Thread aggregating chunk: {chunk_span.begin} to {chunk_span.end}")

        refresh_continuous_aggregates(
            dataset,
            chunk_span,
            enable_chunking=False,
        )

        processed_chunks.append(chunk_span)
        current_start = current_end

    return thread_span, processed_chunks


def run(args):
    dataset = TimeSeriesDataset(GraceFOOffredDataProduct(), DatasetVersion("00"), args.instrument)

    start = datetime.now()
    # create view tables without doing aggregation
    ensure_dataset_caggs_exist(dataset, do_refresh_caggs=False)

    log.info(
        f'"ensure_dataset_caggs_exist()" completed in {get_human_readable_elapsed_since(start)}')

    start = datetime.now()

    # 1. Standardize start and end times to UTC
    start_utc = args.start.replace(tzinfo=timezone.utc)
    end_utc = args.end.replace(tzinfo=timezone.utc)

    # 2. Divide the total time equally among the requested threads
    total_duration = end_utc - start_utc
    num_threads = args.num_threads
    thread_window = total_duration / num_threads

    thread_spans = []
    current_start = start_utc
    for i in range(num_threads):
        # For the last thread, ensure we hit the exact end time to avoid floating point drift
        current_end = current_start + thread_window if i < num_threads - 1 else end_utc
        thread_spans.append(TimeSpan(begin=current_start, end=current_end))
        current_start = current_end

    log.info(
        f'Allocated {num_threads} thread blocks. Threads will process chunks of up to {args.max_days_per_chunk} days internally.')

    # 5. Execute tasks in parallel using a ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit the large span and the chunking parameter to each thread
        futures = [executor.submit(worker, span, dataset, args.max_days_per_chunk) for span in thread_spans]

        # Wait for completion and handle potential exceptions
        for future in as_completed(futures):
            try:
                completed_span, chunks = future.result()
                print(f"Successfully processed thread span: {completed_span.begin} to {completed_span.end} "
                      f"({len(chunks)} chunks)")
            except Exception as e:
                print(f"Thread execution failed with error: {e}")


if __name__ == '__main__':
    args = get_args()

    print(f"Instrument: {args.instrument}")
    print(f"Start Time (UTC): {args.start.isoformat()}")
    print(f"End Time (UTC): {args.end.isoformat()}")
    print(f"Threads: {args.num_threads}")
    print(f"Max Days per Chunk: {args.max_days_per_chunk}")

    start = datetime.now()
    log.info(f'starting aggregation of OFFRED data for {args.instrument} instrument,  '
             f'time range [{args.start.isoformat()} : {args.end.isoformat()}]')
    run(args)

    log.info(
        f'aggregation for time range [{args.start.isoformat()} : {args.end.isoformat()}] '
        f'completed in {get_human_readable_elapsed_since(start)}')