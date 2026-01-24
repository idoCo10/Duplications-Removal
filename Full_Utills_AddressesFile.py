#!/usr/bin/env python3

# Version 3  |  24/01/2026 16:20
# Changelog:
# Added Deduplication, Sorting, Search, Statistics and Bitcoin Address Splitter
# Added automatic sorting verification and correction

# Dependencies:  pip install colorama

import os
import sys
import shutil
import re
from datetime import datetime
from tempfile import TemporaryDirectory, gettempdir
import time
from pathlib import Path
import hashlib
import mmap

# Try to import colorama for better visibility, but don't require it
try:
    from colorama import init, Fore, Back, Style

    init(autoreset=True)
    HAS_COLORAMA = True
except ImportError:
    HAS_COLORAMA = False


    # Create dummy color classes
    class Fore:
        RED = YELLOW = GREEN = CYAN = MAGENTA = BLUE = WHITE = BLACK = ""


    class Back:
        RED = YELLOW = GREEN = CYAN = MAGENTA = BLUE = WHITE = BLACK = ""


    class Style:
        BRIGHT = DIM = NORMAL = RESET_ALL = ""

# ===== CONFIGURATION VARIABLES =====
INPUT_FILE = r"C:\General\My Scripts and Programs\CryptoPare\Test\test_address.txt"  # Your main input file
OUTPUT_FILE = r"C:\General\My Scripts and Programs\CryptoPare\Test\After_test_address.txt"  # Output file
SECOND_FILE = ""  # Optional: Second file to merge (set to "" if not needed)
TEMP_DIR = gettempdir()  # Windows temp directory
MEMORY_LIMIT_GB = 16  # Memory limit in GB for sorting
BUFFER_SIZE = 8 * 1024 * 1024  # 8MB buffer for file operations
REMOVE_CHARACTERS = "#"  # Characters that will cause entire lines to be removed
AUTO_VERIFY_SORTING = True  # Automatically check if output is sorted


# ====================================

class FileStats:
    """Utility class for file statistics"""

    @staticmethod
    def get_file_size(filepath):
        """Get human-readable file size"""
        if os.path.exists(filepath):
            size = os.path.getsize(filepath)
            return FileStats._humanize_size(size)
        return "0 bytes"

    @staticmethod
    def _humanize_size(size, decimal_places=2):
        """Convert bytes to human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size < 1024.0:
                break
            size /= 1024.0
        return f"{size:.{decimal_places}f} {unit}"

    @staticmethod
    def count_lines(filepath, show_progress=True):
        """Count lines in a file efficiently - Windows compatible"""
        if not os.path.exists(filepath):
            return 0

        if show_progress:
            print(f"{Fore.CYAN}Counting lines in {os.path.basename(filepath)}...")

        count = 0
        # Use memory mapping for large files
        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
                # Use mmap for large files
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    while True:
                        chunk = mm.read(1024 * 1024)  # 1MB chunks
                        if not chunk:
                            break
                        count += chunk.count(b'\n')
                        if show_progress and count % 10000000 == 0:
                            print(f"  Counted {Fore.YELLOW}{count:,}{Style.RESET_ALL} lines...")
        except:
            # Fallback for very large files or permission issues
            count = 0
            with open(filepath, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as f:
                for count, _ in enumerate(f, 1):
                    if show_progress and count % 10000000 == 0:
                        print(f"  Counted {Fore.YELLOW}{count:,}{Style.RESET_ALL} lines...")

        return count


class SortingChecker:
    """Simple checker to see if a file is sorted"""

    @staticmethod
    def is_file_sorted(filepath, max_check_lines=1000000000):
        """Quick check if file is sorted - returns (is_sorted, line_count_checked)"""
        if not os.path.exists(filepath):
            return False, 0

        print(f"{Fore.CYAN}Checking if file is sorted...{Style.RESET_ALL}")

        previous_line = None
        lines_checked = 0

        with open(filepath, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as f:
            for line_num, line in enumerate(f, 1):
                if line_num > max_check_lines:
                    break

                current_line = line.strip()

                if previous_line is not None and current_line < previous_line:
                    print(f"{Fore.RED}File is NOT sorted (unsorted at line {line_num}){Style.RESET_ALL}")
                    return False, line_num

                previous_line = current_line
                lines_checked = line_num

                if line_num % 50000 == 0:
                    print(f"  Checked {Fore.YELLOW}{line_num:,}{Style.RESET_ALL} lines...")

        if lines_checked > 0:
            print(f"{Fore.GREEN}✓ File appears to be sorted (checked {lines_checked:,} lines){Style.RESET_ALL}")
            return True, lines_checked
        else:
            print(f"{Fore.YELLOW}File is empty{Style.RESET_ALL}")
            return True, 0


class ExternalSorter:
    """External merge sort implementation for Windows"""

    def __init__(self, temp_dir, memory_limit_gb=4):
        self.temp_dir = temp_dir
        self.memory_limit_bytes = memory_limit_gb * 1024 * 1024 * 1024
        self.chunk_size = self.memory_limit_bytes // 2  # Use half of memory for chunks
        self.checker = SortingChecker()

        # Create temp directory if it doesn't exist
        os.makedirs(self.temp_dir, exist_ok=True)

    def external_sort(self, input_file, output_file, deduplicate=True, auto_verify=True):
        """External merge sort implementation with optional verification"""
        print(f"{Fore.CYAN}Starting external sort...")

        # First check if output file already exists and is sorted
        if os.path.exists(output_file) and auto_verify:
            is_sorted, _ = self.checker.is_file_sorted(output_file)
            if is_sorted:
                print(f"{Fore.GREEN}Output file already exists and is sorted. Skipping sort.{Style.RESET_ALL}")
                return True

        # Step 1: Split into sorted chunks
        chunks = self._create_sorted_chunks(input_file)

        # Step 2: Merge chunks
        self._merge_chunks(chunks, output_file, deduplicate)

        # Step 3: Auto-verify if requested
        if auto_verify:
            self._auto_verify_sorting(output_file)

        # Cleanup temporary chunks
        for chunk_file in chunks:
            try:
                os.remove(chunk_file)
            except:
                pass

        return True

    def _create_sorted_chunks(self, input_file):
        """Split large file into sorted chunks that fit in memory"""
        chunks = []
        chunk_num = 0

        print(f"{Fore.CYAN}Step 1: Creating sorted chunks...")

        with open(input_file, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as f:
            while True:
                # Read a chunk that fits in memory
                lines = []
                current_size = 0

                while current_size < self.chunk_size:
                    line = f.readline()
                    if not line:
                        break

                    line = line.strip()
                    if line:  # Skip empty lines
                        lines.append(line)
                        current_size += len(line.encode('utf-8'))

                if not lines:
                    break

                # Sort the chunk
                lines.sort()

                # Write chunk to temp file
                chunk_file = os.path.join(self.temp_dir, f"chunk_{chunk_num:06d}.txt")
                with open(chunk_file, 'w', encoding='utf-8') as chunk_f:
                    for line in lines:
                        chunk_f.write(line + '\n')

                chunks.append(chunk_file)
                chunk_num += 1

                print(
                    f"  Created chunk {Fore.YELLOW}{chunk_num}{Style.RESET_ALL} with {Fore.GREEN}{len(lines):,}{Style.RESET_ALL} lines")

        print(f"Created {Fore.GREEN}{len(chunks)}{Style.RESET_ALL} chunks")
        return chunks

    def _merge_chunks(self, chunks, output_file, deduplicate=True):
        """Merge sorted chunks into final file"""
        print(f"{Fore.CYAN}Step 2: Merging chunks...")

        if not chunks:
            return

        # Open all chunk files
        chunk_files = []
        current_lines = []

        for chunk_file in chunks:
            try:
                f = open(chunk_file, 'r', encoding='utf-8', buffering=BUFFER_SIZE)
                chunk_files.append(f)
                line = f.readline().strip()
                if line:
                    current_lines.append(line)
                else:
                    current_lines.append(None)
            except:
                current_lines.append(None)

        # Merge using heap algorithm
        with open(output_file, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as out_f:
            last_line = None
            lines_written = 0

            while True:
                # Find the smallest line
                min_line = None
                min_index = -1

                for i, line in enumerate(current_lines):
                    if line is not None:
                        if min_line is None or line < min_line:
                            min_line = line
                            min_index = i

                if min_line is None:
                    break

                # Write line if it's not a duplicate
                if not deduplicate or min_line != last_line:
                    out_f.write(min_line + '\n')
                    last_line = min_line
                    lines_written += 1

                    if lines_written % 1000000 == 0:
                        print(f"  Merged {Fore.YELLOW}{lines_written:,}{Style.RESET_ALL} lines...")

                # Read next line from the chunk we just used
                next_line = chunk_files[min_index].readline()
                if next_line:
                    current_lines[min_index] = next_line.strip()
                else:
                    current_lines[min_index] = None

        # Close all chunk files
        for f in chunk_files:
            f.close()

        print(f"Merge complete. Total lines written: {Fore.GREEN}{lines_written:,}{Style.RESET_ALL}")

    def _auto_verify_sorting(self, output_file):
        """Automatically verify sorting and re-sort if needed"""
        print(f"\n{Fore.CYAN}Auto-verifying output file...{Style.RESET_ALL}")

        is_sorted, lines_checked = self.checker.is_file_sorted(output_file)

        if not is_sorted:
            print(f"{Fore.YELLOW}Output file is not sorted properly. Re-sorting...{Style.RESET_ALL}")

            # Create a temporary file for re-sorting
            temp_output = output_file + ".temp"

            # Re-sort the file
            self.external_sort(output_file, temp_output, deduplicate=False, auto_verify=False)

            # Replace the original file with the sorted one
            try:
                os.remove(output_file)
                os.rename(temp_output, output_file)
                print(f"{Fore.GREEN}File has been re-sorted successfully.{Style.RESET_ALL}")

                # Final verification
                is_sorted, _ = self.checker.is_file_sorted(output_file)
                if is_sorted:
                    print(f"{Fore.GREEN}✓ File is now properly sorted.{Style.RESET_ALL}")
                else:
                    print(f"{Fore.RED}ERROR: File still not sorted after re-sorting!{Style.RESET_ALL}")
            except Exception as e:
                print(f"{Fore.RED}Error re-sorting file: {e}{Style.RESET_ALL}")
        else:
            print(f"{Fore.GREEN}✓ File is properly sorted.{Style.RESET_ALL}")


class BitcoinAddressProcessor:
    """Processor for Bitcoin address operations"""

    @staticmethod
    def is_valid_bitcoin_address(address):
        """Check if a string looks like a Bitcoin address"""
        address = address.strip()

        # Basic Bitcoin address patterns
        patterns = [
            r'^[13][a-km-zA-HJ-NP-Z1-9]{25,34}$',  # Legacy addresses (P2PKH and P2SH)
            r'^bc1[ac-hj-np-z02-9]{11,71}$',  # Native SegWit (Bech32)
            r'^bc1p[ac-hj-np-z02-9]{11,71}$',  # Taproot (Bech32m)
            r'^2[0-9A-Za-z]{25,34}$',  # P2SH addresses starting with 2 or 3
            r'^3[0-9A-Za-z]{25,34}$',
        ]

        for pattern in patterns:
            if re.match(pattern, address):
                return True
        return False

    @staticmethod
    def get_address_type(address):
        """Determine Bitcoin address type by prefix"""
        address = address.strip()

        if address.startswith('1'):
            return "P2PKH (Legacy)"
        elif address.startswith('3'):
            return "P2SH (Legacy)"
        elif address.startswith('bc1'):
            if address.startswith('bc1p'):
                return "P2TR (Taproot)"
            else:
                return "P2WPKH (Native SegWit)"
        elif address.startswith('2'):
            return "P2SH (Testnet)"
        else:
            return "Unknown"

    @staticmethod
    def split_bitcoin_addresses(input_file, output_dir=None):
        """Split Bitcoin addresses by type (1, 3, bc1) - Windows compatible"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}BITCOIN ADDRESS SPLITTER{Style.RESET_ALL}")
        print("=" * 60)

        if not os.path.exists(input_file):
            print(f"{Fore.RED}ERROR: Input file '{input_file}' not found!{Style.RESET_ALL}")
            return {}

        if output_dir is None:
            output_dir = os.path.dirname(input_file) or "."

        os.makedirs(output_dir, exist_ok=True)

        # Output files
        output_files = {
            '1': os.path.join(output_dir, "bitcoin_addresses_1.txt"),  # P2PKH
            '3': os.path.join(output_dir, "bitcoin_addresses_3.txt"),  # P2SH
            'bc1': os.path.join(output_dir, "bitcoin_addresses_bc1.txt"),  # Bech32/SegWit
            'other': os.path.join(output_dir, "bitcoin_addresses_other.txt")  # Other/Invalid
        }

        # Initialize counters
        counters = {key: 0 for key in output_files}
        total_lines = 0

        print(f"Input file: {Fore.YELLOW}{input_file}{Style.RESET_ALL}")
        print(f"Output directory: {Fore.YELLOW}{output_dir}{Style.RESET_ALL}")
        print(f"\nOutput files:")
        for prefix, filepath in output_files.items():
            print(f"  {Fore.GREEN}{prefix}{Style.RESET_ALL}: {filepath}")

        print("\nStarting to split addresses...")
        start_time = datetime.now()

        # Open all output files
        file_handles = {}
        for prefix, filepath in output_files.items():
            file_handles[prefix] = open(filepath, 'w', encoding='utf-8', buffering=BUFFER_SIZE)

        try:
            with open(input_file, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as f:
                for line_num, line in enumerate(f, 1):
                    address = line.strip()

                    # Determine where to write the address
                    if address.startswith('1'):
                        file_handles['1'].write(line)
                        counters['1'] += 1
                    elif address.startswith('3'):
                        file_handles['3'].write(line)
                        counters['3'] += 1
                    elif address.startswith('bc1'):
                        file_handles['bc1'].write(line)
                        counters['bc1'] += 1
                    else:
                        file_handles['other'].write(line)
                        counters['other'] += 1

                    total_lines += 1

                    # Progress reporting
                    if line_num % 1000000 == 0:
                        elapsed = datetime.now() - start_time
                        rate = line_num / elapsed.total_seconds()
                        print(f"  Processed {Fore.YELLOW}{line_num:,}{Style.RESET_ALL} lines ({rate:,.0f} lines/sec)")
                        print(f"    Addresses starting with '1': {Fore.GREEN}{counters['1']:,}{Style.RESET_ALL}")
                        print(f"    Addresses starting with '3': {Fore.GREEN}{counters['3']:,}{Style.RESET_ALL}")
                        print(f"    Addresses starting with 'bc1': {Fore.GREEN}{counters['bc1']:,}{Style.RESET_ALL}")
                        print(f"    Other addresses: {Fore.GREEN}{counters['other']:,}{Style.RESET_ALL}")

        except Exception as e:
            print(f"{Fore.RED}Error during processing: {e}{Style.RESET_ALL}")
        finally:
            # Close all file handles
            for fh in file_handles.values():
                try:
                    fh.close()
                except:
                    pass

        end_time = datetime.now()
        duration = end_time - start_time

        # Print summary
        print("\n" + "=" * 60)
        print(f"{Fore.GREEN}{Style.BRIGHT}SPLITTING COMPLETE!{Style.RESET_ALL}")
        print("=" * 60)
        print(f"Total lines processed: {Fore.YELLOW}{total_lines:,}{Style.RESET_ALL}")
        print(f"Time taken: {duration}")

        print(f"\nAddress Distribution:")
        print("-" * 40)

        for prefix in ['1', '3', 'bc1', 'other']:
            count = counters[prefix]
            percentage = (count / total_lines * 100) if total_lines > 0 else 0
            color = Fore.GREEN if percentage > 10 else Fore.YELLOW if percentage > 0 else Fore.RED
            print(f"  Starts with '{prefix}': {color}{count:,} ({percentage:.2f}%){Style.RESET_ALL}")

        print(f"\nOutput files created in: {Fore.YELLOW}{output_dir}{Style.RESET_ALL}")
        for prefix, filepath in output_files.items():
            if os.path.exists(filepath):
                size = FileStats.get_file_size(filepath)
                lines = FileStats.count_lines(filepath, show_progress=False)
                print(f"  {prefix}: {Fore.GREEN}{lines:,}{Style.RESET_ALL} lines, {size}")

        return counters


class LargeFileProcessor:
    """Main processor for large file operations - Windows compatible"""

    def __init__(self, temp_dir=TEMP_DIR, memory_limit_gb=MEMORY_LIMIT_GB):
        self.temp_dir = temp_dir
        self.memory_limit_gb = memory_limit_gb
        self.sorter = ExternalSorter(temp_dir, memory_limit_gb)
        self.checker = SortingChecker()
        self.remove_characters = REMOVE_CHARACTERS
        self.auto_verify = AUTO_VERIFY_SORTING

        # Create temp directory if it doesn't exist
        os.makedirs(self.temp_dir, exist_ok=True)

    def remove_lines_with_characters(self, input_file, output_file=None, characters=None):
        """Remove entire lines that contain specific characters"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}REMOVE LINES CONTAINING SPECIFIC CHARACTERS{Style.RESET_ALL}")
        print("=" * 60)

        if not os.path.exists(input_file):
            print(f"{Fore.RED}ERROR: Input file '{input_file}' not found!{Style.RESET_ALL}")
            return None

        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{os.path.splitext(input_file)[0]}_filtered_{timestamp}.txt"

        if characters is None:
            characters = self.remove_characters

        if not characters:
            print(f"{Fore.YELLOW}Warning: No characters specified for removal. Using default: '#'{Style.RESET_ALL}")
            characters = '#'

        # Get initial statistics
        initial_size = FileStats.get_file_size(input_file)
        initial_lines = FileStats.count_lines(input_file)

        print(f"\nInput File: {Fore.YELLOW}{input_file}{Style.RESET_ALL}")
        print(f"Initial size: {Fore.GREEN}{initial_size}{Style.RESET_ALL}")
        print(f"Initial lines: {Fore.GREEN}{initial_lines:,}{Style.RESET_ALL}")
        print(f"Removing lines containing any of these characters: {Fore.RED}{characters}{Style.RESET_ALL}")

        if initial_lines == 0:
            print(f"{Fore.YELLOW}File is empty!{Style.RESET_ALL}")
            return None

        print(f"\nProcessing {initial_lines:,} lines...")
        start_time = datetime.now()

        lines_removed = 0
        lines_kept = 0

        with open(input_file, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as infile, \
                open(output_file, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as outfile:

            for line_num, line in enumerate(infile, 1):
                # Check if line contains any of the characters to remove
                if any(char in line for char in characters):
                    lines_removed += 1
                else:
                    outfile.write(line)
                    lines_kept += 1

                # Progress reporting
                if line_num % 1000000 == 0:
                    elapsed = datetime.now() - start_time
                    rate = line_num / elapsed.total_seconds()
                    print(f"  Processed {Fore.YELLOW}{line_num:,}{Style.RESET_ALL} lines "
                          f"({rate:,.0f} lines/sec) - "
                          f"Kept: {Fore.GREEN}{lines_kept:,}{Style.RESET_ALL}, "
                          f"Removed: {Fore.RED}{lines_removed:,}{Style.RESET_ALL}")

        end_time = datetime.now()
        duration = end_time - start_time
        final_size = FileStats.get_file_size(output_file)

        # Print summary
        print("\n" + "=" * 60)
        print(f"{Fore.GREEN}{Style.BRIGHT}LINES REMOVAL COMPLETE!{Style.RESET_ALL}")
        print("=" * 60)
        print(f"Input file: {Fore.YELLOW}{os.path.basename(input_file)}{Style.RESET_ALL}")
        print(f"Output file: {Fore.YELLOW}{os.path.basename(output_file)}{Style.RESET_ALL}")
        print(f"\nInitial lines: {Fore.YELLOW}{initial_lines:,}{Style.RESET_ALL}")
        print(f"Lines kept: {Fore.GREEN}{lines_kept:,}{Style.RESET_ALL}")
        print(f"Lines removed: {Fore.RED}{lines_removed:,}{Style.RESET_ALL}")
        print(f"Removal rate: {Fore.CYAN}{(lines_removed / initial_lines * 100):.2f}%{Style.RESET_ALL}")
        print(f"\nInitial size: {initial_size}")
        print(f"Final size: {final_size}")
        print(
            f"Size reduction: {Fore.GREEN}{(1 - os.path.getsize(output_file) / os.path.getsize(input_file)) * 100:.2f}%{Style.RESET_ALL}")
        print(f"\nTime taken: {duration}")
        print(
            f"Processing speed: {Fore.CYAN}{initial_lines / duration.total_seconds():,.0f}{Style.RESET_ALL} lines/sec")

        return output_file

    def merge_multiple_files(self, file_list, output_file=None):
        """Merge multiple files, remove duplicates, and sort"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}MERGE MULTIPLE FILES, DEDUPLICATE AND SORT{Style.RESET_ALL}")
        print("=" * 60)

        # Validate files
        valid_files = []
        for i, file_path in enumerate(file_list, 1):
            if os.path.exists(file_path):
                valid_files.append(file_path)
                print(f"{Fore.GREEN}✓{Style.RESET_ALL} File {i}: {file_path}")
            else:
                print(f"{Fore.RED}✗{Style.RESET_ALL} File {i} not found: {file_path}")

        if not valid_files:
            print(f"{Fore.RED}ERROR: No valid files to merge!{Style.RESET_ALL}")
            return None

        if len(valid_files) < 2:
            print(
                f"{Fore.YELLOW}Warning: Only one file specified. Use Option 1 for single file processing.{Style.RESET_ALL}")
            choice = input("Continue anyway? (y/n): ").strip().lower()
            if choice != 'y':
                return None

        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"merged_{len(valid_files)}_files_{timestamp}.txt"

        # Get statistics for all files
        total_lines = 0
        print(f"\nAnalyzing {len(valid_files)} files:")
        for i, file_path in enumerate(valid_files, 1):
            lines = FileStats.count_lines(file_path, show_progress=False)
            total_lines += lines
            size = FileStats.get_file_size(file_path)
            print(f"  File {i}: {Fore.GREEN}{lines:,}{Style.RESET_ALL} lines, {size}")

        print(f"\nTotal lines to process: {Fore.YELLOW}{total_lines:,}{Style.RESET_ALL}")

        start_time = datetime.now()

        # Step 1: Clean and concatenate files
        cleaned_files = []
        print(f"{Fore.CYAN}Step 1/{len(valid_files) + 2}: Cleaning files...{Style.RESET_ALL}")

        for i, file_path in enumerate(valid_files, 1):
            cleaned_file = os.path.join(self.temp_dir, f"cleaned_{i:04d}.txt")
            self._clean_file(file_path, cleaned_file)
            cleaned_lines = FileStats.count_lines(cleaned_file, show_progress=False)
            print(f"  Cleaned file {i}: {Fore.GREEN}{cleaned_lines:,}{Style.RESET_ALL} lines")
            cleaned_files.append(cleaned_file)

        # Step 2: Concatenate cleaned files
        cat_file = os.path.join(self.temp_dir, "concatenated_multiple.txt")
        print(f"{Fore.CYAN}Step {len(valid_files) + 1}/{len(valid_files) + 2}: Concatenating files...{Style.RESET_ALL}")

        with open(cat_file, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as outfile:
            for i, cleaned_file in enumerate(cleaned_files, 1):
                with open(cleaned_file, 'r', encoding='utf-8', buffering=BUFFER_SIZE) as infile:
                    shutil.copyfileobj(infile, outfile)
                print(f"  Added file {i} to concatenation")

        # Step 3: Sort and deduplicate
        print(
            f"{Fore.CYAN}Step {len(valid_files) + 2}/{len(valid_files) + 2}: Sorting and deduplicating...{Style.RESET_ALL}")
        self.sorter.external_sort(cat_file, output_file, deduplicate=True, auto_verify=self.auto_verify)

        # Get final statistics
        final_lines = FileStats.count_lines(output_file, show_progress=False)
        duplicates_removed = total_lines - final_lines

        end_time = datetime.now()
        duration = end_time - start_time

        # Cleanup temp files
        for temp_file in cleaned_files + [cat_file]:
            try:
                os.remove(temp_file)
            except:
                pass

        # Print summary
        print("\n" + "=" * 60)
        print(f"{Fore.GREEN}{Style.BRIGHT}MULTI-FILE MERGE COMPLETE!{Style.RESET_ALL}")
        print("=" * 60)
        print(f"Files merged: {Fore.YELLOW}{len(valid_files)}{Style.RESET_ALL}")
        print(f"Total input lines: {Fore.YELLOW}{total_lines:,}{Style.RESET_ALL}")
        print(f"Final unique lines: {Fore.GREEN}{final_lines:,}{Style.RESET_ALL}")
        print(f"Duplicate lines removed: {Fore.RED}{duplicates_removed:,}{Style.RESET_ALL}")
        print(f"Compression: {Fore.CYAN}{(duplicates_removed / total_lines * 100):.2f}%{Style.RESET_ALL} reduction")
        print(f"\nTime taken: {duration}")
        print(f"Processing speed: {Fore.CYAN}{total_lines / duration.total_seconds():,.0f}{Style.RESET_ALL} lines/sec")
        print(f"Output file: {Fore.YELLOW}{output_file}{Style.RESET_ALL}")

        return output_file

    def merge_all_files_in_folder(self, folder_path, output_file=None, file_extension=".txt", filter_characters=None):
        """Merge all files in a folder, remove duplicates, and sort"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}MERGE ALL FILES IN FOLDER{Style.RESET_ALL}")
        print("=" * 60)

        if not os.path.exists(folder_path):
            print(f"{Fore.RED}ERROR: Folder '{folder_path}' not found!{Style.RESET_ALL}")
            return None

        if not os.path.isdir(folder_path):
            print(f"{Fore.RED}ERROR: '{folder_path}' is not a folder!{Style.RESET_ALL}")
            return None

        # Get all files with the specified extension
        all_files = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.endswith(file_extension):
                    all_files.append(os.path.join(root, file))

        if not all_files:
            print(f"{Fore.YELLOW}No files found with extension '{file_extension}' in {folder_path}{Style.RESET_ALL}")

            # Show available extensions
            extensions = {}
            for root, dirs, files in os.walk(folder_path):
                for file in files:
                    ext = os.path.splitext(file)[1]
                    if ext:
                        extensions[ext] = extensions.get(ext, 0) + 1

            if extensions:
                print(f"\nAvailable extensions in folder:")
                for ext, count in sorted(extensions.items()):
                    print(f"  {ext}: {count} files")

                new_ext = input(f"\nEnter extension to use (or press Enter for '.txt'): ").strip()
                if new_ext:
                    if not new_ext.startswith('.'):
                        new_ext = '.' + new_ext
                    file_extension = new_ext

                    # Try again with new extension
                    all_files = []
                    for root, dirs, files in os.walk(folder_path):
                        for file in files:
                            if file.endswith(file_extension):
                                all_files.append(os.path.join(root, file))

            if not all_files:
                return None

        print(f"\nFound {Fore.YELLOW}{len(all_files)}{Style.RESET_ALL} files in folder:")
        for i, file_path in enumerate(all_files[:10], 1):  # Show first 10 files
            print(f"  {i:2}. {os.path.basename(file_path)}")

        if len(all_files) > 10:
            print(f"  ... and {len(all_files) - 10} more files")

        print(f"\nFile extension filter: {Fore.CYAN}{file_extension}{Style.RESET_ALL}")

        # ASK USER ABOUT CHARACTER FILTERING
        if filter_characters is None:
            print(f"\n{Fore.YELLOW}Character filtering:{Style.RESET_ALL}")
            print(f"  Default characters to remove: {Fore.RED}{self.remove_characters}{Style.RESET_ALL}")
            filter_choice = input(f"Remove lines containing these characters? (y/n) [n]: ").strip().lower()

            if filter_choice == 'y':
                custom_chars = input(f"Enter characters to filter [{self.remove_characters}]: ").strip()
                filter_characters = custom_chars if custom_chars else self.remove_characters
                print(f"Will remove lines containing: {Fore.RED}{filter_characters}{Style.RESET_ALL}")
            else:
                filter_characters = ""
        elif filter_characters == "":
            print(f"{Fore.GREEN}Character filtering disabled.{Style.RESET_ALL}")

        # Confirm with user
        print(f"\n{Fore.YELLOW}This will merge ALL {len(all_files)} files from the folder.{Style.RESET_ALL}")
        if filter_characters:
            print(f"{Fore.RED}Lines containing '{filter_characters}' will be removed.{Style.RESET_ALL}")

        confirm = input("Do you want to proceed? (y/n): ").strip().lower()
        if confirm != 'y':
            print(f"{Fore.YELLOW}Operation cancelled.{Style.RESET_ALL}")
            return None

        # Sort files by size (process smaller files first for better progress estimation)
        all_files.sort(key=lambda x: os.path.getsize(x))

        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(folder_path, f"merged_all_files_{timestamp}.txt")

        # Step 1: Clean and filter files
        cleaned_files = []
        print(f"\n{Fore.CYAN}Step 1/{len(all_files) + 2}: Cleaning and filtering files...{Style.RESET_ALL}")

        for i, file_path in enumerate(all_files, 1):
            cleaned_file = os.path.join(self.temp_dir, f"cleaned_filtered_{i:04d}.txt")

            if filter_characters:
                # Clean AND filter lines with characters
                self._clean_and_filter_file(file_path, cleaned_file, filter_characters)
            else:
                # Just clean (original behavior)
                self._clean_file(file_path, cleaned_file)

            cleaned_lines = FileStats.count_lines(cleaned_file, show_progress=False)
            original_lines = FileStats.count_lines(file_path, show_progress=False)

            if filter_characters and original_lines > 0:
                removed = original_lines - cleaned_lines
                print(f"  File {i}: {Fore.GREEN}{cleaned_lines:,}{Style.RESET_ALL} lines "
                      f"(removed {Fore.RED}{removed:,}{Style.RESET_ALL} lines with '{filter_characters}')")
            else:
                print(f"  File {i}: {Fore.GREEN}{cleaned_lines:,}{Style.RESET_ALL} lines")

            cleaned_files.append(cleaned_file)

        # Step 2: Concatenate cleaned files
        cat_file = os.path.join(self.temp_dir, "concatenated_multiple_filtered.txt")
        print(f"\n{Fore.CYAN}Step {len(all_files) + 1}/{len(all_files) + 2}: Concatenating files...{Style.RESET_ALL}")

        with open(cat_file, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as outfile:
            for i, cleaned_file in enumerate(cleaned_files, 1):
                with open(cleaned_file, 'r', encoding='utf-8', buffering=BUFFER_SIZE) as infile:
                    shutil.copyfileobj(infile, outfile)
                print(f"  Added file {i} to concatenation")

        # Step 3: Sort and deduplicate
        print(
            f"\n{Fore.CYAN}Step {len(all_files) + 2}/{len(all_files) + 2}: Sorting and deduplicating...{Style.RESET_ALL}")
        self.sorter.external_sort(cat_file, output_file, deduplicate=True, auto_verify=self.auto_verify)

        # Get final statistics
        final_lines = FileStats.count_lines(output_file, show_progress=False)

        # Calculate total original lines
        total_original_lines = sum(FileStats.count_lines(f, show_progress=False) for f in all_files)
        duplicates_removed = total_original_lines - final_lines

        # Cleanup temp files
        for temp_file in cleaned_files + [cat_file]:
            try:
                os.remove(temp_file)
            except:
                pass

        # Print summary
        print("\n" + "=" * 60)
        print(f"{Fore.GREEN}{Style.BRIGHT}FOLDER MERGE COMPLETE!{Style.RESET_ALL}")
        print("=" * 60)
        print(f"Files merged: {Fore.YELLOW}{len(all_files)}{Style.RESET_ALL}")
        print(f"Total input lines: {Fore.YELLOW}{total_original_lines:,}{Style.RESET_ALL}")
        print(f"Final unique lines: {Fore.GREEN}{final_lines:,}{Style.RESET_ALL}")

        if filter_characters:
            lines_filtered = total_original_lines - sum(
                FileStats.count_lines(f, show_progress=False) for f in cleaned_files)
            print(f"Lines filtered (containing '{filter_characters}'): {Fore.RED}{lines_filtered:,}{Style.RESET_ALL}")

        print(f"Duplicate lines removed: {Fore.RED}{duplicates_removed:,}{Style.RESET_ALL}")
        print(
            f"Total reduction: {Fore.CYAN}{((total_original_lines - final_lines) / total_original_lines * 100):.2f}%{Style.RESET_ALL}")
        print(f"\nOutput file: {Fore.YELLOW}{output_file}{Style.RESET_ALL}")

        return output_file

    def _clean_and_filter_file(self, input_file, output_file, filter_characters):
        """Clean file AND filter lines containing specific characters"""
        with open(input_file, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as infile, \
                open(output_file, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as outfile:

            for line_num, line in enumerate(infile, 1):
                stripped = line.strip()

                # Skip empty lines AND lines containing filter characters
                if stripped and not any(char in stripped for char in filter_characters):
                    outfile.write(stripped + '\n')

                if line_num % 1000000 == 0:
                    print(f"    Processed {Fore.YELLOW}{line_num:,}{Style.RESET_ALL} lines...")

    def deduplicate_and_sort(self, input_file, output_file=None, remove_chars=False):
        """Option 1: Deduplicate and sort main file - Windows compatible"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}OPTION 1: DEDUPLICATE AND SORT MAIN FILE{Style.RESET_ALL}")
        print("=" * 60)

        if not os.path.exists(input_file):
            print(f"{Fore.RED}ERROR: Input file '{input_file}' not found!{Style.RESET_ALL}")
            return None

        if output_file is None:
            output_file = f"{os.path.splitext(input_file)[0]}_deduplicated.txt"

        # Check if output file already exists and is sorted
        if os.path.exists(output_file) and self.auto_verify:
            print(f"{Fore.CYAN}Output file already exists. Checking if it's sorted...{Style.RESET_ALL}")
            is_sorted, lines_checked = self.checker.is_file_sorted(output_file)
            if is_sorted:
                print(f"{Fore.GREEN}✓ Output file already exists and is sorted.{Style.RESET_ALL}")
                print(
                    f"{Fore.YELLOW}Skipping processing. Delete the output file if you want to re-process.{Style.RESET_ALL}")
                return output_file

        # Get initial statistics
        initial_size = FileStats.get_file_size(input_file)
        initial_lines = FileStats.count_lines(input_file)

        print(f"\nInput File: {Fore.YELLOW}{input_file}{Style.RESET_ALL}")
        print(f"Initial size: {Fore.GREEN}{initial_size}{Style.RESET_ALL}")
        print(f"Initial lines: {Fore.GREEN}{initial_lines:,}{Style.RESET_ALL}")

        if initial_lines == 0:
            print(f"{Fore.YELLOW}File is empty!{Style.RESET_ALL}")
            return None

        # Ask if user wants to remove lines with characters
        if remove_chars or input(
                f"\nRemove lines containing these characters '{self.remove_characters}'? (y/n) [n]: ").strip().lower() == 'y':
            temp_file = os.path.join(self.temp_dir, "filtered_before_sort.txt")
            self.remove_lines_with_characters(input_file, temp_file, self.remove_characters)
            input_file = temp_file
            initial_lines = FileStats.count_lines(input_file, show_progress=False)
            print(f"\nAfter filtering: {Fore.GREEN}{initial_lines:,}{Style.RESET_ALL} lines remaining")

        # Check available disk space
        if not self._check_disk_space(input_file):
            return None

        print(f"\nProcessing {initial_lines:,} lines...")
        start_time = datetime.now()

        # Step 1: Clean file (remove empty lines and trim spaces)
        cleaned_file = os.path.join(self.temp_dir, "cleaned.txt")
        print(f"{Fore.CYAN}Step 1/3: Cleaning file (removing empty lines, trimming spaces)...{Style.RESET_ALL}")

        self._clean_file(input_file, cleaned_file)
        cleaned_lines = FileStats.count_lines(cleaned_file, show_progress=False)
        print(f"  After cleaning: {Fore.GREEN}{cleaned_lines:,}{Style.RESET_ALL} lines")

        # Step 2: Sort and deduplicate
        print(f"{Fore.CYAN}Step 2/3: Sorting and deduplicating...{Style.RESET_ALL}")

        self.sorter.external_sort(cleaned_file, output_file, deduplicate=True, auto_verify=self.auto_verify)

        # Step 3: Final statistics
        print(f"{Fore.CYAN}Step 3/3: Calculating final statistics...{Style.RESET_ALL}")

        final_lines = FileStats.count_lines(output_file, show_progress=False)
        final_size = FileStats.get_file_size(output_file)
        empty_lines_removed = initial_lines - cleaned_lines
        duplicates_removed = cleaned_lines - final_lines
        total_removed = initial_lines - final_lines

        end_time = datetime.now()
        duration = end_time - start_time

        # Cleanup temp files
        for temp_file in [cleaned_file]:
            try:
                os.remove(temp_file)
            except:
                pass
        if 'temp_file' in locals() and os.path.exists(temp_file):
            try:
                os.remove(temp_file)
            except:
                pass

        self._print_summary(
            initial_lines, final_lines, total_removed,
            empty_lines_removed, duplicates_removed,
            initial_size, final_size, input_file, output_file, duration
        )

        return output_file

    def merge_files(self, file1, file2, output_file=None):
        """Option 2: Merge two files, remove duplicates, and sort"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}OPTION 2: MERGE TWO FILES, DEDUPLICATE AND SORT{Style.RESET_ALL}")
        print("=" * 60)

        for f in [file1, file2]:
            if not os.path.exists(f):
                print(f"{Fore.RED}ERROR: File '{f}' not found!{Style.RESET_ALL}")
                return None

        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"merged_deduplicated_{timestamp}.txt"

        # Check if output file already exists and is sorted
        if os.path.exists(output_file) and self.auto_verify:
            print(f"{Fore.CYAN}Output file already exists. Checking if it's sorted...{Style.RESET_ALL}")
            is_sorted, lines_checked = self.checker.is_file_sorted(output_file)
            if is_sorted:
                print(f"{Fore.GREEN}✓ Output file already exists and is sorted.{Style.RESET_ALL}")
                print(
                    f"{Fore.YELLOW}Skipping processing. Delete the output file if you want to re-process.{Style.RESET_ALL}")
                return output_file

        # Get statistics
        initial_lines1 = FileStats.count_lines(file1)
        initial_lines2 = FileStats.count_lines(file2)
        total_initial_lines = initial_lines1 + initial_lines2

        print(f"\nFile 1: {Fore.YELLOW}{file1}{Style.RESET_ALL}")
        print(f"  Lines: {Fore.GREEN}{initial_lines1:,}{Style.RESET_ALL}")
        print(f"  Size: {FileStats.get_file_size(file1)}")

        print(f"\nFile 2: {Fore.YELLOW}{file2}{Style.RESET_ALL}")
        print(f"  Lines: {Fore.GREEN}{initial_lines2:,}{Style.RESET_ALL}")
        print(f"  Size: {FileStats.get_file_size(file2)}")

        print(f"\nTotal lines to process: {Fore.YELLOW}{total_initial_lines:,}{Style.RESET_ALL}")

        start_time = datetime.now()

        # Step 1: Clean and concatenate files
        cleaned1 = os.path.join(self.temp_dir, "cleaned1.txt")
        cleaned2 = os.path.join(self.temp_dir, "cleaned2.txt")

        print(f"{Fore.CYAN}Step 1/3: Cleaning files (removing empty lines)...{Style.RESET_ALL}")

        # Clean both files
        self._clean_file(file1, cleaned1)
        self._clean_file(file2, cleaned2)

        cleaned_lines1 = FileStats.count_lines(cleaned1, show_progress=False)
        cleaned_lines2 = FileStats.count_lines(cleaned2, show_progress=False)
        empty_removed = total_initial_lines - (cleaned_lines1 + cleaned_lines2)

        print(f"  Empty lines removed: {Fore.RED}{empty_removed:,}{Style.RESET_ALL}")

        # Step 2: Concatenate cleaned files
        cat_file = os.path.join(self.temp_dir, "concatenated.txt")
        print(f"{Fore.CYAN}Step 2/3: Concatenating cleaned files...{Style.RESET_ALL}")

        with open(cat_file, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as outfile:
            for infile in [cleaned1, cleaned2]:
                with open(infile, 'r', encoding='utf-8', buffering=BUFFER_SIZE) as f:
                    shutil.copyfileobj(f, outfile)

        # Step 3: Sort and deduplicate
        print(f"{Fore.CYAN}Step 3/3: Sorting and deduplicating...{Style.RESET_ALL}")

        self.sorter.external_sort(cat_file, output_file, deduplicate=True, auto_verify=self.auto_verify)

        # Get final statistics
        final_lines = FileStats.count_lines(output_file, show_progress=False)
        duplicates_removed = (cleaned_lines1 + cleaned_lines2) - final_lines
        total_removed = total_initial_lines - final_lines

        end_time = datetime.now()
        duration = end_time - start_time

        # Cleanup temp files
        for temp_file in [cleaned1, cleaned2, cat_file]:
            try:
                os.remove(temp_file)
            except:
                pass

        print("\n" + "=" * 60)
        print(f"{Fore.GREEN}{Style.BRIGHT}MERGE COMPLETE!{Style.RESET_ALL}")
        print("=" * 60)
        print(f"File 1 lines: {Fore.YELLOW}{initial_lines1:,}{Style.RESET_ALL}")
        print(f"File 2 lines: {Fore.YELLOW}{initial_lines2:,}{Style.RESET_ALL}")
        print(f"Total input lines: {Fore.YELLOW}{total_initial_lines:,}{Style.RESET_ALL}")
        print(f"Final unique lines: {Fore.GREEN}{final_lines:,}{Style.RESET_ALL}")
        print(f"Empty lines removed: {Fore.RED}{empty_removed:,}{Style.RESET_ALL}")
        print(f"Duplicate lines removed: {Fore.RED}{duplicates_removed:,}{Style.RESET_ALL}")
        print(f"Total lines removed: {Fore.RED}{total_removed:,}{Style.RESET_ALL}")
        print(f"Compression: {Fore.CYAN}{(total_removed / total_initial_lines * 100):.2f}%{Style.RESET_ALL} reduction")
        print(f"\nTime taken: {duration}")
        print(
            f"Processing speed: {Fore.CYAN}{total_initial_lines / duration.total_seconds():,.0f}{Style.RESET_ALL} lines/sec")
        print(f"Output file: {Fore.YELLOW}{output_file}{Style.RESET_ALL}")

        return output_file

    def search_in_file(self, filepath, search_term, case_sensitive=True, max_results=100):
        """Search for a term in a large file"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}SEARCHING IN: {os.path.basename(filepath)}{Style.RESET_ALL}")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"{Fore.RED}ERROR: File '{filepath}' not found!{Style.RESET_ALL}")
            return

        lines_count = FileStats.count_lines(filepath)
        print(f"Total lines in file: {Fore.GREEN}{lines_count:,}{Style.RESET_ALL}")

        if not case_sensitive:
            search_term = search_term.lower()

        print(f"Searching for: {Fore.YELLOW}'{search_term}'{Style.RESET_ALL}")
        print(f"Case sensitive: {Fore.CYAN}{'Yes' if case_sensitive else 'No'}{Style.RESET_ALL}")
        print(f"Max results to show: {Fore.CYAN}{max_results}{Style.RESET_ALL}")
        print("\nSearching... (Ctrl+C to stop)")

        matches = []
        line_numbers = []

        try:
            with open(filepath, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as f:
                for line_num, line in enumerate(f, 1):
                    line_to_check = line if case_sensitive else line.lower()

                    if search_term in line_to_check:
                        matches.append(line.strip())
                        line_numbers.append(line_num)

                        if len(matches) % 1000 == 0:
                            print(f"  Found {Fore.YELLOW}{len(matches):,}{Style.RESET_ALL} matches so far...")

                        if len(matches) >= max_results * 10:  # Stop early if too many
                            print(
                                f"\nStopping search - found {Fore.YELLOW}{len(matches):,}{Style.RESET_ALL} matches (showing first {max_results})")
                            break

                    if line_num % 1000000 == 0:
                        print(f"  Scanned {Fore.YELLOW}{line_num:,}{Style.RESET_ALL} lines...")

        except KeyboardInterrupt:
            print(f"\n{Fore.YELLOW}Search interrupted by user.{Style.RESET_ALL}")

        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}SEARCH RESULTS:{Style.RESET_ALL}")
        print("=" * 60)

        if matches:
            print(f"Total matches found: {Fore.GREEN}{len(matches):,}{Style.RESET_ALL}")
            print(f"First {min(max_results, len(matches))} matches:")
            print("-" * 40)

            for i, (match, line_num) in enumerate(zip(matches[:max_results], line_numbers[:max_results]), 1):
                # Highlight the search term in the result
                if case_sensitive:
                    highlighted = match.replace(search_term, f"{Fore.RED}{search_term}{Style.RESET_ALL}")
                else:
                    # Case insensitive highlighting
                    import re
                    highlighted = re.sub(f'({re.escape(search_term)})',
                                         f'{Fore.RED}\\1{Style.RESET_ALL}',
                                         match,
                                         flags=re.IGNORECASE)

                print(
                    f"{Fore.CYAN}{i:4}.{Style.RESET_ALL} Line {Fore.YELLOW}{line_num:,}{Style.RESET_ALL}: {highlighted}")
        else:
            print(f"{Fore.YELLOW}No matches found.{Style.RESET_ALL}")

        return matches

    def show_lines(self, filepath, start_line, end_line):
        """Display specific lines from a file"""
        print("\n" + "=" * 60)
        print(
            f"{Fore.CYAN}{Style.BRIGHT}SHOWING LINES {start_line:,} to {end_line:,} FROM: {os.path.basename(filepath)}{Style.RESET_ALL}")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"{Fore.RED}ERROR: File '{filepath}' not found!{Style.RESET_ALL}")
            return

        lines_count = FileStats.count_lines(filepath)
        print(f"Total lines in file: {Fore.GREEN}{lines_count:,}{Style.RESET_ALL}")

        if start_line < 1:
            start_line = 1
        if end_line > lines_count:
            end_line = lines_count

        if start_line > end_line:
            print(f"{Fore.RED}ERROR: Start line must be less than or equal to end line{Style.RESET_ALL}")
            return

        print(
            f"Displaying lines {Fore.YELLOW}{start_line:,}{Style.RESET_ALL} to {Fore.YELLOW}{end_line:,}{Style.RESET_ALL}:")
        print("-" * 40)

        lines_displayed = 0
        with open(filepath, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as f:
            for line_num, line in enumerate(f, 1):
                if line_num < start_line:
                    continue
                if line_num > end_line:
                    break

                print(f"{Fore.CYAN}{line_num:8,}:{Style.RESET_ALL} {line.rstrip()}")
                lines_displayed += 1

        print(f"\nDisplayed {Fore.GREEN}{lines_displayed:,}{Style.RESET_ALL} lines.")

    def delete_characters(self, filepath, characters_to_delete, output_file=None):
        """Delete specific characters from a file"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}DELETING CHARACTERS FROM: {os.path.basename(filepath)}{Style.RESET_ALL}")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"{Fore.RED}ERROR: File '{filepath}' not found!{Style.RESET_ALL}")
            return None

        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{os.path.splitext(filepath)[0]}_cleaned_{timestamp}.txt"

        initial_lines = FileStats.count_lines(filepath)
        print(f"Total lines in file: {Fore.GREEN}{initial_lines:,}{Style.RESET_ALL}")
        print(f"Characters to delete: {Fore.RED}'{characters_to_delete}'{Style.RESET_ALL}")

        print("\nProcessing...")
        start_time = datetime.now()

        # Python implementation (no sed needed)
        with open(filepath, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as infile, \
                open(output_file, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as outfile:

            for line_num, line in enumerate(infile, 1):
                # Remove specified characters
                for char in characters_to_delete:
                    line = line.replace(char, '')
                outfile.write(line)

                if line_num % 1000000 == 0:
                    print(f"  Processed {Fore.YELLOW}{line_num:,}{Style.RESET_ALL} lines...")

        final_lines = FileStats.count_lines(output_file, show_progress=False)
        end_time = datetime.now()
        duration = end_time - start_time

        print("\n" + "=" * 60)
        print(f"{Fore.GREEN}{Style.BRIGHT}CHARACTER DELETION COMPLETE!{Style.RESET_ALL}")
        print("=" * 60)
        print(f"Input file: {Fore.YELLOW}{filepath}{Style.RESET_ALL}")
        print(f"Output file: {Fore.YELLOW}{output_file}{Style.RESET_ALL}")
        print(f"Characters removed: {Fore.RED}'{characters_to_delete}'{Style.RESET_ALL}")
        print(f"Initial lines: {Fore.YELLOW}{initial_lines:,}{Style.RESET_ALL}")
        print(f"Final lines: {Fore.GREEN}{final_lines:,}{Style.RESET_ALL}")
        print(f"Time taken: {duration}")
        print(
            f"Processing speed: {Fore.CYAN}{initial_lines / duration.total_seconds():,.0f}{Style.RESET_ALL} lines/sec")

        return output_file

    def get_statistics(self, filepath):
        """Get detailed statistics about a file"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}STATISTICS FOR: {os.path.basename(filepath)}{Style.RESET_ALL}")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"{Fore.RED}ERROR: File '{filepath}' not found!{Style.RESET_ALL}")
            return

        print("\nCalculating statistics...")

        # Basic file info
        size_bytes = os.path.getsize(filepath)
        size_human = FileStats.get_file_size(filepath)
        lines = FileStats.count_lines(filepath)

        print(f"\nFile Information:")
        print(f"  Path: {Fore.YELLOW}{filepath}{Style.RESET_ALL}")
        print(f"  Size: {Fore.GREEN}{size_human}{Style.RESET_ALL} ({size_bytes:,} bytes)")
        print(f"  Lines: {Fore.GREEN}{lines:,}{Style.RESET_ALL}")

        # Check if file is sorted
        is_sorted, lines_checked = self.checker.is_file_sorted(filepath)
        if is_sorted:
            print(f"  Sorting: {Fore.GREEN}Sorted ✓{Style.RESET_ALL} (checked {lines_checked:,} lines)")
        else:
            print(f"  Sorting: {Fore.RED}NOT sorted ✗{Style.RESET_ALL}")

        # First few lines
        print(f"\nFirst 5 lines:")
        print("-" * 40)
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for i in range(5):
                line = f.readline()
                if not line:
                    break
                print(f"  {Fore.CYAN}{i + 1}:{Style.RESET_ALL} {line.rstrip()}")

        # Last few lines
        print(f"\nLast 5 lines:")
        print("-" * 40)
        try:
            # Read last lines manually
            with open(filepath, 'rb') as f:
                f.seek(-min(5000, os.path.getsize(filepath)), os.SEEK_END)
                last_lines = f.read().decode('utf-8', errors='ignore').split('\n')[-6:-1]
                for i, line in enumerate(last_lines, 1):
                    print(f"  {Fore.CYAN}{i}:{Style.RESET_ALL} {line}")
        except:
            print(f"  {Fore.YELLOW}(Could not read last lines){Style.RESET_ALL}")

        # File age
        mtime = os.path.getmtime(filepath)
        age = datetime.now() - datetime.fromtimestamp(mtime)
        print(
            f"\nFile Age: {Fore.YELLOW}{age.days}{Style.RESET_ALL} days, {Fore.YELLOW}{age.seconds // 3600}{Style.RESET_ALL} hours")

        return {
            'size_bytes': size_bytes,
            'lines': lines,
            'is_sorted': is_sorted
        }

    def check_and_fix_sorting(self, filepath):
        """Check if a file is sorted, and if not, sort it"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}CHECK IF FILE IS SORTED{Style.RESET_ALL}")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"{Fore.RED}ERROR: File '{filepath}' not found!{Style.RESET_ALL}")
            return None

        # Check if file is sorted
        is_sorted, lines_checked = self.checker.is_file_sorted(filepath)

        if is_sorted:
            print(f"\n{Fore.GREEN}✓ File is already sorted. No action needed.{Style.RESET_ALL}")
            return filepath

        print(f"\n{Fore.YELLOW}File is not sorted. Sorting now...{Style.RESET_ALL}")

        # Create sorted version
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sorted_file = f"{os.path.splitext(filepath)[0]}_sorted_{timestamp}.txt"

        # Sort the file (without deduplication to preserve all lines)
        self.sorter.external_sort(filepath, sorted_file, deduplicate=False, auto_verify=True)

        # Ask if user wants to replace original file
        print(f"\n{Fore.YELLOW}Sorting complete!{Style.RESET_ALL}")
        replace = input(f"Do you want to replace the original file with the sorted version? (y/n): ").strip().lower()

        if replace == 'y':
            try:
                # Backup original file
                backup_file = filepath + ".bak"
                shutil.copy2(filepath, backup_file)

                # Replace original with sorted
                os.remove(filepath)
                shutil.move(sorted_file, filepath)

                print(f"{Fore.GREEN}✓ Original file replaced with sorted version.{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}Backup saved as: {backup_file}{Style.RESET_ALL}")
                return filepath
            except Exception as e:
                print(f"{Fore.RED}Error replacing file: {e}{Style.RESET_ALL}")
                print(f"{Fore.YELLOW}Sorted file saved as: {sorted_file}{Style.RESET_ALL}")
                return sorted_file
        else:
            print(f"{Fore.YELLOW}Sorted file saved as: {sorted_file}{Style.RESET_ALL}")
            return sorted_file

    def split_file_by_prefix(self, filepath, output_dir=None):
        """Split file by Bitcoin address prefixes (1, 3, bc1)"""
        return BitcoinAddressProcessor.split_bitcoin_addresses(filepath, output_dir)

    def analyze_bitcoin_file(self, filepath, sample_size=100000):
        """Analyze Bitcoin addresses in file"""
        print("\n" + "=" * 60)
        print(f"{Fore.CYAN}{Style.BRIGHT}BITCOIN ADDRESS ANALYSIS{Style.RESET_ALL}")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"{Fore.RED}ERROR: Input file '{filepath}' not found!{Style.RESET_ALL}")
            return

        print(f"Analyzing Bitcoin addresses in {os.path.basename(filepath)}...")
        print(f"Sampling first {sample_size:,} addresses...")

        address_types = {}
        valid_count = 0
        invalid_count = 0
        total_count = 0

        with open(filepath, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as f:
            for i, line in enumerate(f):
                if i >= sample_size:
                    break

                address = line.strip()
                total_count += 1

                if BitcoinAddressProcessor.is_valid_bitcoin_address(address):
                    valid_count += 1
                    addr_type = BitcoinAddressProcessor.get_address_type(address)
                    address_types[addr_type] = address_types.get(addr_type, 0) + 1
                else:
                    invalid_count += 1

                if i % 10000 == 0 and i > 0:
                    print(f"  Analyzed {Fore.YELLOW}{i:,}{Style.RESET_ALL} addresses...")

        print("\nAnalysis Results:")
        print(f"Total addresses sampled: {Fore.YELLOW}{total_count:,}{Style.RESET_ALL}")
        print(
            f"Valid Bitcoin addresses: {Fore.GREEN}{valid_count:,}{Style.RESET_ALL} ({valid_count / total_count * 100:.2f}%)")
        print(
            f"Invalid/Other addresses: {Fore.RED}{invalid_count:,}{Style.RESET_ALL} ({invalid_count / total_count * 100:.2f}%)")

        if valid_count > 0:
            print(f"\nValid Address Types:")
            for addr_type, count in sorted(address_types.items(), key=lambda x: x[1], reverse=True):
                percentage = count / valid_count * 100
                color = Fore.GREEN if percentage > 30 else Fore.YELLOW if percentage > 10 else Fore.CYAN
                print(f"  {addr_type}: {color}{count:,}{Style.RESET_ALL} ({percentage:.2f}%)")

        return {
            'total': total_count,
            'valid': valid_count,
            'invalid': invalid_count,
            'types': address_types
        }

    def _clean_file(self, input_file, output_file):
        """Clean file: remove empty lines and trim spaces"""
        with open(input_file, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as infile, \
                open(output_file, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as outfile:

            for line_num, line in enumerate(infile, 1):
                stripped = line.strip()
                if stripped:  # Skip empty lines
                    outfile.write(stripped + '\n')

                if line_num % 1000000 == 0:
                    print(f"  Cleaned {Fore.YELLOW}{line_num:,}{Style.RESET_ALL} lines...")

    def _check_disk_space(self, input_file):
        """Check if there's enough disk space"""
        try:
            import ctypes
            free_bytes = ctypes.c_ulonglong(0)
            total_bytes = ctypes.c_ulonglong(0)

            # Get free disk space on Windows
            ctypes.windll.kernel32.GetDiskFreeSpaceExW(
                ctypes.c_wchar_p(self.temp_dir),
                None,
                ctypes.pointer(total_bytes),
                ctypes.pointer(free_bytes)
            )

            free_space = free_bytes.value
            file_size = os.path.getsize(input_file)
            required_space = file_size * 2  # Need space for temp files

            if free_space < required_space:
                print(f"\n{Fore.YELLOW}WARNING: Low disk space in {self.temp_dir}{Style.RESET_ALL}")
                print(f"Required: ~{FileStats._humanize_size(required_space)}")
                print(f"Available: {FileStats._humanize_size(free_space)}")
                print(f"{Fore.YELLOW}Consider using a different TEMP_DIR with more space{Style.RESET_ALL}")
                if input(f"{Fore.RED}Continue anyway? (y/n): {Style.RESET_ALL}").lower() != 'y':
                    return False
        except:
            # Fallback if we can't get disk space info
            print(f"{Fore.YELLOW}Note: Using temp directory: {self.temp_dir}{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}Make sure you have at least 2x the input file size free space.{Style.RESET_ALL}")

        return True

    def _print_summary(self, initial_lines, final_lines, total_removed,
                       empty_removed, duplicates_removed,
                       initial_size, final_size, input_file, output_file, duration):
        """Print processing summary"""
        print("\n" + "=" * 60)
        print(f"{Fore.GREEN}{Style.BRIGHT}PROCESSING COMPLETE!{Style.RESET_ALL}")
        print("=" * 60)
        print(f"Input file: {Fore.YELLOW}{os.path.basename(input_file)}{Style.RESET_ALL}")
        print(f"Output file: {Fore.YELLOW}{os.path.basename(output_file)}{Style.RESET_ALL}")
        print(f"\nInitial lines: {Fore.YELLOW}{initial_lines:,}{Style.RESET_ALL}")
        print(f"Final lines: {Fore.GREEN}{final_lines:,}{Style.RESET_ALL}")
        print(f"Empty lines removed: {Fore.RED}{empty_removed:,}{Style.RESET_ALL}")
        print(f"Duplicate lines removed: {Fore.RED}{duplicates_removed:,}{Style.RESET_ALL}")
        print(f"Total lines removed: {Fore.RED}{total_removed:,}{Style.RESET_ALL}")
        print(f"Compression: {Fore.CYAN}{(total_removed / initial_lines * 100):.2f}%{Style.RESET_ALL} reduction")
        print(f"\nInitial size: {initial_size}")
        print(f"Final size: {final_size}")
        size_saved = os.path.getsize(input_file) - os.path.getsize(output_file)
        print(f"Space saved: {Fore.GREEN}{FileStats._humanize_size(size_saved)}{Style.RESET_ALL}")
        print(f"\nTime taken: {duration}")
        print(
            f"Processing speed: {Fore.CYAN}{initial_lines / duration.total_seconds():,.0f}{Style.RESET_ALL} lines/sec")


def interactive_menu():
    """Interactive menu for the user"""
    processor = LargeFileProcessor()

    while True:
        print("\n" + "=" * 70)
        print(f"{Fore.CYAN}{Style.BRIGHT}WINDOWS LARGE FILE MANAGEMENT TOOL{Style.RESET_ALL}")
        print("=" * 70)
        print(f"Current working directory: {Fore.YELLOW}{os.getcwd()}{Style.RESET_ALL}")
        print(f"Temp directory: {Fore.YELLOW}{TEMP_DIR}{Style.RESET_ALL}")
        print(f"Memory limit: {Fore.GREEN}{MEMORY_LIMIT_GB} GB{Style.RESET_ALL}")
        print(f"Remove lines containing: {Fore.RED}{REMOVE_CHARACTERS}{Style.RESET_ALL}")
        print(
            f"Auto-verify sorting: {Fore.GREEN if AUTO_VERIFY_SORTING else Fore.YELLOW}{AUTO_VERIFY_SORTING}{Style.RESET_ALL}")
        print("=" * 70)

        print(f"\n{Fore.MAGENTA}{Style.BRIGHT}MAIN MENU - What would you like to do?{Style.RESET_ALL}")
        print(f"{Fore.GREEN} 1.{Style.RESET_ALL}  Remove Duplications and sort file (+ Remove lines contained #)")
        print(f"{Fore.GREEN} 2.{Style.RESET_ALL}  Merge two files, Remove Duplications and sort")
        print(f"{Fore.GREEN} 3.{Style.RESET_ALL}  Merge multiple files (2+ files)")  # FIXED THIS LINE
        print(
            f"{Fore.GREEN} 4.{Style.RESET_ALL}  Merge all files in a folder (+ Remove Duplications, Lines contained # and sort)")
        print(f"{Fore.GREEN} 5.{Style.RESET_ALL}  Remove lines containing specific characters")
        print(f"{Fore.GREEN} 6.{Style.RESET_ALL}  Search in file")
        print(f"{Fore.GREEN} 7.{Style.RESET_ALL}  Show specific lines from file")
        print(f"{Fore.GREEN} 8.{Style.RESET_ALL}  Delete characters from file")
        print(f"{Fore.GREEN} 9.{Style.RESET_ALL}  Get file statistics")
        print(f"{Fore.GREEN}10.{Style.RESET_ALL}  Count lines in file")
        print(f"{Fore.GREEN}11.{Style.RESET_ALL}  Split Bitcoin addresses by prefix (1, 3, bc1)")
        print(f"{Fore.GREEN}12.{Style.RESET_ALL}  Analyze Bitcoin addresses in file")
        print(f"{Fore.GREEN}13.{Style.RESET_ALL}  Sort - Check if file is sorted, if not - Sort it! ")
        print(f"{Fore.GREEN}14.{Style.RESET_ALL}  Change configuration")
        print(f"{Fore.GREEN}15.{Style.RESET_ALL}  Exit")

        choice = input(f"\n{Fore.YELLOW}Enter your choice (1-15): {Style.RESET_ALL}").strip()

        if choice == '1':
            file_path = input(f"Input file [{INPUT_FILE}]: ").strip() or INPUT_FILE
            output_path = input(f"Output file [{OUTPUT_FILE}]: ").strip() or OUTPUT_FILE
            processor.deduplicate_and_sort(file_path, output_path)

        elif choice == '2':
            file1 = input(f"First file [{INPUT_FILE}]: ").strip() or INPUT_FILE
            file2 = input(f"Second file [{SECOND_FILE}]: ").strip() or SECOND_FILE
            if not file2:
                print(f"{Fore.RED}Please specify a second file!{Style.RESET_ALL}")
                continue
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_output = f"merged_deduplicated_{timestamp}.txt"
            output_file = input(f"Output file [{default_output}]: ").strip() or default_output
            processor.merge_files(file1, file2, output_file)

        elif choice == '3':
            print(f"{Fore.CYAN}Enter file paths (one per line, empty line to finish):{Style.RESET_ALL}")
            file_list = []
            i = 1
            while True:
                file_path = input(f"File {i} (or press Enter to finish): ").strip()
                if not file_path:
                    if i == 1:
                        print(f"{Fore.YELLOW}No files specified!{Style.RESET_ALL}")
                        break
                    else:
                        break
                if os.path.exists(file_path):
                    file_list.append(file_path)
                    i += 1
                else:
                    print(f"{Fore.RED}File not found: {file_path}{Style.RESET_ALL}")

            if len(file_list) >= 2:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                default_output = f"merged_{len(file_list)}_files_{timestamp}.txt"
                output_file = input(f"Output file [{default_output}]: ").strip() or default_output
                processor.merge_multiple_files(file_list, output_file)


        elif choice == '4':

            folder_path = input("Folder path to merge all files from: ").strip()

            if not folder_path:
                print(f"{Fore.YELLOW}Using current directory: {os.getcwd()}{Style.RESET_ALL}")

                folder_path = os.getcwd()

            file_extension = input("File extension to merge (e.g., .txt, .dat) [.txt]: ").strip()

            if not file_extension:

                file_extension = ".txt"

            elif not file_extension.startswith('.'):

                file_extension = '.' + file_extension

            # Ask about character filtering

            filter_chars = ""

            filter_choice = input(f"Remove lines containing characters like '#'? (y/n) [n]: ").strip().lower()

            if filter_choice == 'y':
                filter_chars = input(f"Characters to filter [{REMOVE_CHARACTERS}]: ").strip() or REMOVE_CHARACTERS

            output_file = input("Output file name [merged_all_files_TIMESTAMP.txt]: ").strip()

            if not output_file:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

                output_file = os.path.join(folder_path, f"merged_all_files_{timestamp}.txt")

            processor.merge_all_files_in_folder(folder_path, output_file, file_extension, filter_chars)

        elif choice == '5':
            file_path = input(f"File [{INPUT_FILE}]: ").strip() or INPUT_FILE
            chars = input(
                f"Characters to remove lines containing them [{REMOVE_CHARACTERS}]: ").strip() or REMOVE_CHARACTERS
            if not chars:
                chars = '#'
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_output = f"{os.path.splitext(file_path)[0]}_filtered_{timestamp}.txt"
            output_file = input(f"Output file [{default_output}]: ").strip() or default_output
            processor.remove_lines_with_characters(file_path, output_file, chars)

        elif choice == '6':
            file_path = input(f"File to search [{INPUT_FILE}]: ").strip() or INPUT_FILE
            search_term = input("Search term: ").strip()
            if not search_term:
                print(f"{Fore.RED}Search term cannot be empty!{Style.RESET_ALL}")
                continue
            case_sensitive = input("Case sensitive? (y/n) [n]: ").strip().lower() != 'y'
            max_results = input("Max results to show [100]: ").strip()
            max_results = int(max_results) if max_results.isdigit() else 100
            processor.search_in_file(file_path, search_term, case_sensitive, max_results)

        elif choice == '7':
            file_path = input(f"File [{INPUT_FILE}]: ").strip() or INPUT_FILE
            start_line = input("Start line [1]: ").strip()
            start_line = int(start_line) if start_line.isdigit() else 1
            end_line = input("End line [100]: ").strip()
            end_line = int(end_line) if end_line.isdigit() else 100
            processor.show_lines(file_path, start_line, end_line)

        elif choice == '8':
            file_path = input(f"File [{INPUT_FILE}]: ").strip() or INPUT_FILE
            chars = input("Characters to delete (e.g., ',;\"): ").strip()
            if not chars:
                print(f"{Fore.RED}Please specify characters to delete!{Style.RESET_ALL}")
                continue
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_output = f"{os.path.splitext(file_path)[0]}_cleaned_{timestamp}.txt"
            output_file = input(f"Output file [{default_output}]: ").strip() or default_output
            processor.delete_characters(file_path, chars, output_file)

        elif choice == '9':
            file_path = input(f"File [{INPUT_FILE}]: ").strip() or INPUT_FILE
            processor.get_statistics(file_path)

        elif choice == '10':
            file_path = input(f"File [{INPUT_FILE}]: ").strip() or INPUT_FILE
            lines = FileStats.count_lines(file_path)
            print(f"\nTotal lines in {os.path.basename(file_path)}: {Fore.GREEN}{lines:,}{Style.RESET_ALL}")

        elif choice == '11':
            file_path = input(f"Bitcoin addresses file [{INPUT_FILE}]: ").strip() or INPUT_FILE
            output_dir = input(f"Output directory [current directory]: ").strip()
            if not output_dir:
                output_dir = os.path.dirname(file_path) or "."
            processor.split_file_by_prefix(file_path, output_dir)

        elif choice == '12':
            file_path = input(f"Bitcoin addresses file [{INPUT_FILE}]: ").strip() or INPUT_FILE
            sample_size = input("Sample size for analysis [100000]: ").strip()
            sample_size = int(sample_size) if sample_size.isdigit() else 100000
            print(f"\nAnalyzing first {sample_size:,} addresses...")
            processor.analyze_bitcoin_file(file_path, sample_size)

        elif choice == '13':
            # NEW: Check and fix sorting
            file_path = input(f"File to check/fix [{INPUT_FILE}]: ").strip() or INPUT_FILE
            processor.check_and_fix_sorting(file_path)

        elif choice == '14':
            print(f"\n{Fore.CYAN}Current configuration:{Style.RESET_ALL}")
            print(f"  INPUT_FILE: {Fore.YELLOW}{INPUT_FILE}{Style.RESET_ALL}")
            print(f"  OUTPUT_FILE: {Fore.YELLOW}{OUTPUT_FILE}{Style.RESET_ALL}")
            print(f"  SECOND_FILE: {Fore.YELLOW}{SECOND_FILE}{Style.RESET_ALL}")
            print(f"  TEMP_DIR: {Fore.YELLOW}{TEMP_DIR}{Style.RESET_ALL}")
            print(f"  MEMORY_LIMIT_GB: {Fore.GREEN}{MEMORY_LIMIT_GB}{Style.RESET_ALL}")
            print(f"  REMOVE_CHARACTERS: {Fore.RED}{REMOVE_CHARACTERS}{Style.RESET_ALL}")
            print(f"  AUTO_VERIFY_SORTING: {Fore.GREEN}{AUTO_VERIFY_SORTING}{Style.RESET_ALL}")
            print(
                f"\n{Fore.YELLOW}Note: To permanently change configuration, edit the variables at the top of the script.{Style.RESET_ALL}")

        elif choice == '15':
            print(f"{Fore.GREEN}Exiting...{Style.RESET_ALL}")
            break

        else:
            print(f"{Fore.RED}Invalid choice!{Style.RESET_ALL}")

        input(f"\n{Fore.CYAN}Press Enter to continue...{Style.RESET_ALL}")


def display_banner():
    """Display program banner"""
    print("\n" + "=" * 70)
    print(f"{Fore.CYAN}{Style.BRIGHT}WINDOWS BITCOIN ADDRESS MANAGER & LARGE FILE PROCESSOR{Style.RESET_ALL}")
    print("=" * 70)
    print("Features:")
    print(f"  {Fore.GREEN}•{Style.RESET_ALL} Deduplicate and sort 70GB+ files (Windows compatible)")
    print(f"  {Fore.GREEN}•{Style.RESET_ALL} Split Bitcoin addresses by type (1, 3, bc1)")
    print(f"  {Fore.GREEN}•{Style.RESET_ALL} Merge multiple files with deduplication")
    print(f"  {Fore.GREEN}•{Style.RESET_ALL} Merge all files in a folder")
    print(f"  {Fore.GREEN}•{Style.RESET_ALL} Remove lines containing specific characters (like #)")
    print(f"  {Fore.GREEN}•{Style.RESET_ALL} Search, analyze, and manipulate large files")
    print(f"  {Fore.GREEN}•{Style.RESET_ALL} External sorting with minimal memory usage")
    print(f"  {Fore.GREEN}•{Style.RESET_ALL} Auto-verify sorting and auto-fix if not sorted")
    print(f"  {Fore.GREEN}•{Style.RESET_ALL} Check any file's sorting status (Option 13)")
    print(f"  {Fore.GREEN}•{Style.RESET_ALL} No Linux commands required - pure Python")
    print("=" * 70)


def main():
    """Main entry point"""
    display_banner()

    print(f"\n{Fore.CYAN}Current Configuration:{Style.RESET_ALL}")
    print(f"  Input file: {Fore.YELLOW}{INPUT_FILE}{Style.RESET_ALL}")
    print(f"  Output file: {Fore.YELLOW}{OUTPUT_FILE}{Style.RESET_ALL}")
    if SECOND_FILE:
        print(f"  Second file: {Fore.YELLOW}{SECOND_FILE}{Style.RESET_ALL}")
    print(f"  Temp directory: {Fore.YELLOW}{TEMP_DIR}{Style.RESET_ALL}")
    print(f"  Memory limit: {Fore.GREEN}{MEMORY_LIMIT_GB} GB{Style.RESET_ALL}")
    print(f"  Remove lines containing: {Fore.RED}{REMOVE_CHARACTERS}{Style.RESET_ALL}")
    print(
        f"  Auto-verify sorting: {Fore.GREEN if AUTO_VERIFY_SORTING else Fore.YELLOW}{AUTO_VERIFY_SORTING}{Style.RESET_ALL}")
    print("=" * 70)

    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"\n{Fore.YELLOW}WARNING: Input file '{INPUT_FILE}' not found!{Style.RESET_ALL}")
        print("You can still use other features or specify a different file.")
        if input(f"{Fore.RED}Continue? (y/n): {Style.RESET_ALL}").lower() != 'y':
            sys.exit(1)

    # Install colorama if not available
    if not HAS_COLORAMA:
        print(f"\n{Fore.YELLOW}Note: For better visibility, install colorama:{Style.RESET_ALL}")
        print(f"  pip install colorama")
        print(f"\n{Fore.YELLOW}Running without colors...{Style.RESET_ALL}")

    # Run interactive menu
    try:
        interactive_menu()
    except KeyboardInterrupt:
        print(f"\n\n{Fore.YELLOW}Program interrupted by user. Exiting...{Style.RESET_ALL}")
        sys.exit(0)
    except Exception as e:
        print(f"\n{Fore.RED}Error: {e}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # Set console to UTF-8 on Windows
    if sys.platform == "win32":
        import io

        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

    main()
