import os
import re
import shutil
import argparse

TYPE_TO_PATTERN_MAP = {
    'DDIC'   : r'^DDIC_seg_(?P<subset_version>\d{3})\.txt',
    'ICSNR'  : r'^ICSNR_(?P<instrument_id>[CD])_seg_(?P<subset_version>\d{3})\.txt',
    'CLK_DD' : r'^clk_dd\.txt',
    'RESIDS': r'^range_resids\.txt',
    'ACC1B_ANG_X': r'^ACC1B_(?P<instrument_id>[CD])_00.angx-mean',
    'ACC1B_ANG_Y': r'^ACC1B_(?P<instrument_id>[CD])_00.angy-mean',
    'ACC1B_ANG_Z': r'^ACC1B_(?P<instrument_id>[CD])_00.angz-mean',
    'ACC1B_LIN_X': r'^ACC1B_(?P<instrument_id>[CD])_00.linx-mean',
    'ACC1B_LIN_Y': r'^ACC1B_(?P<instrument_id>[CD])_00.liny-mean',
    'ACC1B_LIN_Z': r'^ACC1B_(?P<instrument_id>[CD])_00.linz-mean',
}
SUPPORTED_TYPES = TYPE_TO_PATTERN_MAP.keys()

def rename_and_copy(root_input_dir, dropbox_path, id_suffix):
    pattern = TYPE_TO_PATTERN_MAP[id_suffix]
    os.makedirs(dropbox_path, exist_ok=True)
    regex = re.compile(pattern)  # Compile the regex for efficiency

    for root, _, files in os.walk(root_input_dir):
        for filename in files:

            if regex.search(filename):  # Use search() to find pattern anywhere in filename

                fpath = os.path.join(root, filename)
                fname = os.path.basename(fpath)
                if 'subset_version' in regex.groupindex:
                    version = re.search(pattern, fname).group('subset_version')
                else:
                    version = get_version_from_fpath(fpath)

                if 'instrument_id' in regex.groupindex:
                    instrument_id = re.search(pattern, fname).group('instrument_id')
                else:
                    instrument_id = 'Y'

                date_str = get_date_from_fpath(fpath)
                new_name = get_new_name(id_suffix, date_str, instrument_id, version)
                destination = os.path.join(dropbox_path, new_name)

                    # Copy with a new name
                shutil.copy(fpath, destination)

def get_date_from_fpath(path):
    folders = []
    while True:
        path, folder = os.path.split(path)
        if folder:
            folders.append(folder)
        else:
            if path:
                folders.append(path)
            break
    for f in folders:
        if re.fullmatch(r"^\d{4}-\d{2}-\d{2}$", f):
            return f
    else:
        raise RuntimeError(f"Can not find date in {path}...")


    subdir = os.path.basename(os.path.dirname(fpath))
    return subdir

def get_version_from_fpath(path):
    # only 'range_resids\.txt' and 'clk_dd.txt'  have a version.
    # The location is similar to 'work/L1B/rl00/YYYY/YYYY-MM-DD/KBR1B'
    # Try to get the version from the path
    if os.path.basename(path) == 'range_resids.txt' or os.path.basename(path) == 'clk_dd.txt':
        folders = []
        while True:
            path, folder = os.path.split(path)
            if folder:
                folders.append(folder)
            else:
                if path:
                    folders.append(path)
                break

        version =  folders[4]

        if re.fullmatch(r"rl0\d", version):
            return version
        else:
            raise RuntimeError(f"Can not find version, got: {version}...")
    return None

def get_new_name(product_prefix, date_str, instrument_id, version):
    if not instrument_id:
        instrument_id = 'Y'
    if not version:
        return f'{product_prefix}_{date_str}_{instrument_id}.txt'
    return f'{product_prefix}_{date_str}_{instrument_id}_{version[-2:]}.txt'

def main():
    parser = argparse.ArgumentParser(description=f"Recursively search directory for product files, "
                                                 f"re-name them, and copy to the ingest dropbox. "
                                                 f"Supported types: {SUPPORTED_TYPES}")
    parser.add_argument("root_input_dir", help="The root input directory")
    # Defining an argument with a set of valid strings
    parser.add_argument(
        'product_type',
        help="Prefix for supported types, for example, DDIC, ICSNR, CLK_DD",
        choices=SUPPORTED_TYPES,
    )
    parser.add_argument("-d", "--dropbox_path", default="/soft/mango/input-data-dropbox/",
                        help="Path to ingest dropbox, default: /soft/mango/input-data-dropbox/")

    args = parser.parse_args()

    rename_and_copy(args.root_input_dir, args.dropbox_path, args.product_type)
    
if __name__ == "__main__":
    main()