import os
import argparse
import subprocess
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


def process_file(external_script, file_path, dropbox_path):
    """
    Executes the external script on the given file path.
    """
    destination = os.path.join(dropbox_path, Path(file_path).stem + '.txt')
    try:
        # Construct the command: [script_name, argument]
        # Ensure external_script is executable or prefixed with the interpreter (e.g., 'python3')
        cmd = [external_script, '-binfile', file_path, '-ascfile', destination]
        # specific capture_output=True to handle stdout/stderr if needed
        # Run the command and capture output
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False  # We handle errors manually via return code
        )
        return (file_path, result.returncode, result.stdout, result.stderr)

    except Exception as e:
        return (file_path, -1, "", str(e))


def find_matching_files(root_dir, prefix):
    """
    Recursively yields file paths that match the .dat extension and prefix.
    """
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith(".dat") and filename.startswith(prefix):
                yield os.path.join(dirpath, filename)


def main():
    parser = argparse.ArgumentParser(description="Recursively process .dat files with a specific prefix.")
    parser.add_argument("directory", help="The root directory to search.")
    parser.add_argument("prefix", help="The filename prefix to search for.")
    parser.add_argument("script", help="The path to the external script to execute.")
    parser.add_argument("--dropbox_path", default="/soft/mango/input-data-dropbox/",
                        help="Path to ingest dropbox, default: /soft/mango/input-data-dropbox/")
    parser.add_argument("--threads", type=int, default=4, help="Number of threads to use (default: 4).")

    args = parser.parse_args()
    max_workers = args.threads
    external_script = args.script

    # Validate inputs
    if not os.path.isdir(args.directory):
        print(f"Error: Directory '{args.directory}' does not exist.")
        return
    # create output directory if does not exists
    os.makedirs(args.dropbox_path, exist_ok=True)

    # Check if the external script exists (optional check, dependent on use case)
    if not os.path.isfile(args.script) and not shutil.which(args.script):
        print(f"Warning: Script '{args.script}' not found locally. Ensure it is in your PATH.")

    # Collect all matching files first
    files_to_process = list(find_matching_files(args.directory, args.prefix))

    if not files_to_process:
        print("No matching files found.")
        return

    print(f"Found {len(files_to_process)} files. Starting processing with {args.threads} threads...")

    # Use ThreadPoolExecutor for multi-threading
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Map futures to file paths for tracking

        future_to_file = {
            executor.submit(process_file, external_script, f, args.dropbox_path): f
            for f in files_to_process
        }

        for future in as_completed(future_to_file):
            file_path, return_code, stdout, stderr = future.result()

            if return_code == 0:
                print(f"[SUCCESS] {file_path}")
            else:
                print(f"[ERROR] {file_path} (Exit Code: {return_code})")
                if stderr:
                    print(f"  Details: {stderr.strip()}")


if __name__ == "__main__":
    main()