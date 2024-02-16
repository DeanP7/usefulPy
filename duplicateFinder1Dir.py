#Made this script to find duplicates in a directory.
#Script utilizes multithreading to find duplicate files within a directory and writes the results to a CSV file, with progress tracking functionality.
#Progress report is currently not working as intended, although it still functions in showing the script is still progressing.

import os
import hashlib
import csv
from concurrent.futures import ThreadPoolExecutor
from threading import Lock

def print_progress(traversed, total):
    print(f"Progress: {traversed}/{total} files traversed.")

def find_duplicates(directory, max_threads=None, progress_callback=None):
    duplicates = []
    files_hash = {}
    lock = Lock()
    total_files = 0

    def hash_file(file_path):
        try:
            hasher = hashlib.sha256()
            with open(file_path, "rb") as f:
                while True:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    hasher.update(chunk)
            return hasher.digest()
        except Exception as e:
            print(f"Error hashing file {file_path}: {e}")
            return None

    def process_file(file_path):
        nonlocal duplicates, total_files
        file_counter = 0
        total_files += 1
        file_hash = hash_file(file_path)
        if file_hash:
            with lock:
                if file_hash in files_hash:
                    duplicates.append((files_hash[file_hash], file_path))
                else:
                    files_hash[file_hash] = file_path
                    file_counter = len(files_hash)  # Count the number of lines written
            progress_callback(total_files, progress_callback.total, file_counter)  # Pass total as well

    progress_callback.total = 0  # Initialize total within the callback function

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        for root, _, files in os.walk(directory):
            total_files += len(files)
            progress_callback.total += len(files)  # Increment total within the loop
            for file_name in files:
                file_path = os.path.join(root, file_name)
                executor.submit(process_file, file_path)

    return duplicates

def write_duplicates_to_file(duplicate_files, output_file):
    with open(output_file, 'w', newline='') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(['File', 'Duplicate', 'Size (KB)'])
        
        total_kb = 0

        for duplicate_pair in duplicate_files:
            file1 = duplicate_pair[0]
            file2 = duplicate_pair[1]
            size1 = os.path.getsize(file1) / 1024  # Convert bytes to KB
            size2 = os.path.getsize(file2) / 1024  # Convert bytes to KB
            total_kb += size1 + size2
            csv_writer.writerow([file1, file2, size1])
            csv_writer.writerow([file2, file1, size2])
        
        csv_writer.writerow(['Total KB', '', total_kb])

if __name__ == "__main__":
    #Directory to be searched through
    directory = r""
    #Output path for .csv or .txt
    output_file = r""
    max_threads = 1

    if os.path.exists(directory):
        duplicate_files = find_duplicates(directory, max_threads=max_threads, progress_callback=print_progress)
        if duplicate_files:
            write_duplicates_to_file(duplicate_files, output_file)
            print("Duplicate files have been written to 'fileServerDuplicateFiles.txt'")
        else:
            print("No duplicates found.")
    else:
        print("The directory does not exist.")
