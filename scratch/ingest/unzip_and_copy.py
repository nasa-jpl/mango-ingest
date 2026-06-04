import argparse
import zipfile
from pathlib import Path
import sys


def extract_zip_files(input_dir: str, output_dir: str):
    in_path = Path(input_dir)
    out_path = Path(output_dir)

    # Check if input directory exists
    if not in_path.is_dir():
        print(f"Error: Input directory '{input_dir}' does not exist.")
        sys.exit(1)

    # Ensure output directory exists (creates it if it doesn't)
    out_path.mkdir(parents=True, exist_ok=True)

    # Recursively find all .zip files
    zip_files = list(in_path.rglob("*.zip"))

    if not zip_files:
        print(f"No .zip files found in '{input_dir}' or its subdirectories.")
        return

    print(f"Found {len(zip_files)} .zip file(s). Starting extraction...\n")

    for zip_file in zip_files:
        print(f"Extracting: {zip_file.name} ...")
        try:
            with zipfile.ZipFile(zip_file, 'r') as zf:
                # Extract all contents to the output directory
                zf.extractall(out_path)
        except zipfile.BadZipFile:
            print(f"  -> [Error]: '{zip_file.name}' is a bad or corrupted zip file. Skipping.")
        except Exception as e:
            print(f"  -> [Error]: Failed to extract '{zip_file.name}'. Reason: {e}")

    print(f"\nDone! Extracted contents have been saved to: {out_path.resolve()}")


if __name__ == "__main__":
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(
        description="Recursively find and unzip all .zip files from an input directory to an output directory."
    )
    parser.add_argument("input_dir", help="The root directory to search for .zip files.")
    parser.add_argument("output_dir", help="The destination directory for extracted files.")

    args = parser.parse_args()

    # Run the extraction function
    extract_zip_files(args.input_dir, args.output_dir)