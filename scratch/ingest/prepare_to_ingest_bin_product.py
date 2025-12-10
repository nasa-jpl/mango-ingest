import os
import argparse
import subprocess
import shutil
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

def process_file( external_script, file_path, ingest_dropbox_path,):
    """
    Executes the external script on the given file path.
    """
    destination = os.path.join(ingest_dropbox_path, Path(file_path).stem + '.txt')
    try:
        # Construct the command: [script_name, argument]
        # Ensure external_script is executable or prefixed with the interpreter (e.g., 'python3')
        cmd = [external_script, '-binfile', file_path, '-ascfile ', destination]
        print('QQQQ ', cmd)
        # specific capture_output=True to handle stdout/stderr if needed
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)

        print(f"[SUCCESS] Processed: {file_path}")
        # Optional: Print output from the external script
        # print(result.stdout)

    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Failed to process {file_path}. Exit code: {e.returncode}")
        print(f"Stderr: {e.stderr}")
    except Exception as e:
        print(f"[ERROR] An unexpected error occurred with {file_path}: {e}")

def find_matching_files(root_dir, prefix):
    """
    Recursively yields file paths that match the .dat extension and prefix.
    """
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            #if filename.endswith(".dat") and filename.startswith(prefix):
            if  filename.startswith(prefix):
                yield os.path.join(dirpath, filename)


def main():
    parser = argparse.ArgumentParser(description="Recursively process .dat files with a specific prefix.")
    parser.add_argument("directory", help="The root directory to search.")
    parser.add_argument("prefix", help="The filename prefix to search for.")
    parser.add_argument("script", help="The path to the external script to execute.")
    parser.add_argument("--dropbox_path", default="/soft/mango/input-data-dropbox/", \
                        help="Path to ingest dropbox, default: /soft/mango/input-data-dropbox/")
    parser.add_argument("--threads", type=int, default=4, help="Number of threads to use (default: 4).")

    args = parser.parse_args()

    # Validate inputs
    if not os.path.isdir(args.directory):
        print(f"Error: Directory '{args.directory}' does not exist.")
        return

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
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        # Submit tasks to the executor
        # We use a lambda or list comprehension to pass arguments effectively
        futures = [executor.submit(process_file, args.script, f, args.dropbox_path) for f in files_to_process]

        # Wait for all tasks to complete (context manager handles this, but explicit wait is possible)
        for future in futures:
            future.result()  # This ensures we catch any exceptions raised within the thread if needed

    print("All tasks completed.")


if __name__ == "__main__":
    main()