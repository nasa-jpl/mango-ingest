import os
import re
from typing import Iterable


def enumerate_files_in_dir_tree(root_dir: str, match_regex: str | None = None, match_filename_only: bool = False) -> Iterable[str]:
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

    for path, subdirs, filenames in os.walk(root_dir):
        for filename in filenames:
            filepath = os.path.join(path, filename)
            match_target = filename if match_filename_only else filepath

            if match_regex is None or re.match(match_regex, match_target):
                yield filepath
