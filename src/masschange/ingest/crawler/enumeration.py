import os
import re
from typing import Iterable


def enumerate_files_in_dir_tree(root_dir: str, match_regex: str | None = None, match_filename_only: bool = False,
                                followlinks: bool = True) -> Iterable[str]:
    """
    Given a directory root and a regex match pattern, return all matching filepaths under it.

    Parameters
    ----------
    root_dir - the root directory path under which to search
    match_regex - the match pattern applied to each filepath/filename
    match_filename_only - apply the match pattern to the filename only, rather than the full filepath

    Returns
    -------
    an iterable collection of matching filepaths within the directory tree rooted at root_dir

    """

    for path, subdirs, filenames in os.walk(root_dir, followlinks=followlinks):
        for filename in filenames:
            filepath = os.path.join(path, filename)
            match_target = filename if match_filename_only else filepath

            if match_regex is None or re.match(match_regex, match_target):
                yield filepath


def order_filepaths_by_filename(filepaths: Iterable[str]) -> Iterable[str]:
    """
    Provide crude temporal ordering of input files by sorting them on filename. Timescaledb is optimised for insertion
    of most-recent data to a given table, so it is desirable to sort them during ingestion.
    """
    return sorted(filepaths, key=lambda fp: os.path.split(fp)[-1])
