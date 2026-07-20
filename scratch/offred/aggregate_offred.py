import argparse
import logging
from datetime import datetime, timezone


from masschange.dataproducts.datasetversion import DatasetVersion
from masschange.dataproducts.implementations.gracefo.offred.offred import GraceFOOffredDataProduct
from masschange.dataproducts.timeseriesdataset import TimeSeriesDataset
from masschange.db.data.caggs import refresh_continuous_aggregates
from masschange.db.data.ensure import ensure_dataset_caggs_exist
from masschange.utils.logging import configure_root_logger

from masschange.utils.misc import get_human_readable_elapsed_since
from masschange.utils.timespan import TimeSpan

configure_root_logger(log_filepath=None, log_level = logging.DEBUG)

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
        # Update the error message to reflect the new accepted formats
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

    # 2. Add the instrument argument
    parser.add_argument(
        'instrument',
        choices=['GF1', 'GF2'],
        help="The target instrument (GF1 or GF2)."
    )

    # 3. Add the required start argument
    parser.add_argument(
        'start',
        type=parse_utc_datetime,
        help="Start date and time in ISO format (e.g., 2026-07-16T12:00:00Z), or just a date YYYY-MM-DD."
    )

    # 3. Add the required start argument
    parser.add_argument(
        'end',
        type=parse_utc_datetime,
        help="End date and time in ISO format (e.g., 2026-07-16T12:00:00Z), or just a date YYYY-MM-DD."
    )
    parser.add_argument(
        'chunk_duration_sec',
        type = int,
        help="Chunk duration in seconds"
    )
    return  parser.parse_args()

def run(args):
    dataset = TimeSeriesDataset(GraceFOOffredDataProduct(), DatasetVersion("00"), args.instrument)

    start = datetime.now()
    # create view tables without doing aggregation
    ensure_dataset_caggs_exist(dataset, do_refresh_caggs=False)

    log.info(
        f'"ensure_dataset_caggs_exist()" completed in {get_human_readable_elapsed_since(start)}')

    start = datetime.now()
   # aggregate for the requested temporal span
    data_temporal_span = TimeSpan(begin=args.start.replace(tzinfo=timezone.utc),
                                  end=args.end.replace(tzinfo=timezone.utc))
    refresh_continuous_aggregates(dataset, data_temporal_span, enable_chunking=True, chunk_duration_seconds=args.chunk_duration_sec)

    log.info(
        f'"refresh_continuous_aggregates()" completed in {get_human_readable_elapsed_since(start)}')


if __name__ == '__main__':

    args = get_args()

    print(f"Instrument: {args.instrument}")
    print(f"Start Time (UTC): {args.start.isoformat()}")
    print(f"End Time (UTC): {args.end.isoformat()}")

    start = datetime.now()
    log.info(f'starting aggregation of OFFRED data for {args.instrument} instrument,  '
             f'time range [{args.start.isoformat()} : {args.end.isoformat()}]')
    run(args)

    log.info(
        f'aggregation for time range [{args.start.isoformat()} : {args.end.isoformat()}] '
        f'completed in {get_human_readable_elapsed_since(start)}')







