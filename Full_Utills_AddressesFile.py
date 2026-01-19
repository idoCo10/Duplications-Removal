# !/usr/bin/env python3
"""
Windows-Compatible Large File Management Tool
Deduplication, Sorting, Search, Statistics and Bitcoin Address Splitter
Handles files up to 70GB+ with pure Python implementation
"""

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

# ===== CONFIGURATION VARIABLES =====
INPUT_FILE = "Test-receiveAddresses.txt"  # Your main input file
OUTPUT_FILE = "After-Test-receiveAddresses.txt"  # Output file
SECOND_FILE = ""  # Optional: Second file to merge (set to "" if not needed)
TEMP_DIR = gettempdir()  # Windows temp directory
MEMORY_LIMIT_GB = 4  # Memory limit in GB for sorting
BUFFER_SIZE = 1024 * 1024  # 1MB buffer for file operations


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
            print(f"Counting lines in {os.path.basename(filepath)}...")

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
                            print(f"  Counted {count:,} lines...")
        except:
            # Fallback for very large files or permission issues
            count = 0
            with open(filepath, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as f:
                for count, _ in enumerate(f, 1):
                    if show_progress and count % 10000000 == 0:
                        print(f"  Counted {count:,} lines...")

        return count

    @staticmethod
    def get_unique_count(filepath, sample_size=1000000):
        """Estimate unique count by sampling"""
        if not os.path.exists(filepath):
            return 0, 0

        print(f"Estimating unique values in {os.path.basename(filepath)}...")
        sample = set()
        total = 0

        with open(filepath, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as f:
            for i, line in enumerate(f):
                line = line.strip()
                if line:  # Skip empty lines
                    sample.add(line)
                    total += 1
                if i >= sample_size:
                    break

        if total == 0:
            return 0, 0

        uniqueness_rate = len(sample) / total
        return len(sample), uniqueness_rate


class ExternalSorter:
    """External merge sort implementation for Windows"""

    def __init__(self, temp_dir, memory_limit_gb=4):
        self.temp_dir = temp_dir
        self.memory_limit_bytes = memory_limit_gb * 1024 * 1024 * 1024
        self.chunk_size = self.memory_limit_bytes // 2  # Use half of memory for chunks

        # Create temp directory if it doesn't exist
        os.makedirs(self.temp_dir, exist_ok=True)

    def external_sort(self, input_file, output_file, deduplicate=True):
        """External merge sort implementation"""
        print("Starting external sort...")

        # Step 1: Split into sorted chunks
        chunks = self._create_sorted_chunks(input_file)

        # Step 2: Merge chunks
        self._merge_chunks(chunks, output_file, deduplicate)

        # Cleanup temporary chunks
        for chunk_file in chunks:
            try:
                os.remove(chunk_file)
            except:
                pass

    def _create_sorted_chunks(self, input_file):
        """Split large file into sorted chunks that fit in memory"""
        chunks = []
        chunk_num = 0

        print("Step 1: Creating sorted chunks...")

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

                print(f"  Created chunk {chunk_num} with {len(lines):,} lines")

        print(f"Created {len(chunks)} chunks")
        return chunks

    def _merge_chunks(self, chunks, output_file, deduplicate=True):
        """Merge sorted chunks into final file"""
        print("Step 2: Merging chunks...")

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
                        print(f"  Merged {lines_written:,} lines...")

                # Read next line from the chunk we just used
                next_line = chunk_files[min_index].readline()
                if next_line:
                    current_lines[min_index] = next_line.strip()
                else:
                    current_lines[min_index] = None

        # Close all chunk files
        for f in chunk_files:
            f.close()

        print(f"Merge complete. Total lines written: {lines_written:,}")


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
        print("BITCOIN ADDRESS SPLITTER")
        print("=" * 60)

        if not os.path.exists(input_file):
            print(f"ERROR: Input file '{input_file}' not found!")
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

        print(f"Input file: {input_file}")
        print(f"Output directory: {output_dir}")
        print(f"\nOutput files:")
        for prefix, filepath in output_files.items():
            print(f"  {prefix}: {filepath}")

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
                        print(f"  Processed {line_num:,} lines ({rate:,.0f} lines/sec)")
                        print(f"    Addresses starting with '1': {counters['1']:,}")
                        print(f"    Addresses starting with '3': {counters['3']:,}")
                        print(f"    Addresses starting with 'bc1': {counters['bc1']:,}")
                        print(f"    Other addresses: {counters['other']:,}")

        except Exception as e:
            print(f"Error during processing: {e}")
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
        print("SPLITTING COMPLETE!")
        print("=" * 60)
        print(f"Total lines processed: {total_lines:,}")
        print(f"Time taken: {duration}")

        print(f"\nAddress Distribution:")
        print("-" * 40)

        for prefix in ['1', '3', 'bc1', 'other']:
            count = counters[prefix]
            percentage = (count / total_lines * 100) if total_lines > 0 else 0
            print(f"  Starts with '{prefix}': {count:,} ({percentage:.2f}%)")

        print(f"\nOutput files created in: {output_dir}")
        for prefix, filepath in output_files.items():
            if os.path.exists(filepath):
                size = FileStats.get_file_size(filepath)
                lines = FileStats.count_lines(filepath, show_progress=False)
                print(f"  {prefix}: {lines:,} lines, {size}")

        return counters


class LargeFileProcessor:
    """Main processor for large file operations - Windows compatible"""

    def __init__(self, temp_dir=TEMP_DIR, memory_limit_gb=MEMORY_LIMIT_GB):
        self.temp_dir = temp_dir
        self.memory_limit_gb = memory_limit_gb
        self.sorter = ExternalSorter(temp_dir, memory_limit_gb)

        # Create temp directory if it doesn't exist
        os.makedirs(self.temp_dir, exist_ok=True)

    def deduplicate_and_sort(self, input_file, output_file=None):
        """Option 1: Deduplicate and sort main file - Windows compatible"""
        print("\n" + "=" * 60)
        print("OPTION 1: DEDUPLICATE AND SORT MAIN FILE")
        print("=" * 60)

        if not os.path.exists(input_file):
            print(f"ERROR: Input file '{input_file}' not found!")
            return None

        if output_file is None:
            output_file = f"{os.path.splitext(input_file)[0]}_deduplicated.txt"

        # Get initial statistics
        initial_size = FileStats.get_file_size(input_file)
        initial_lines = FileStats.count_lines(input_file)

        print(f"\nInput File: {input_file}")
        print(f"Initial size: {initial_size}")
        print(f"Initial lines: {initial_lines:,}")

        if initial_lines == 0:
            print("File is empty!")
            return None

        # Check available disk space
        if not self._check_disk_space(input_file):
            return None

        print(f"\nProcessing {initial_lines:,} lines...")
        start_time = datetime.now()

        # Step 1: Clean file (remove empty lines and trim spaces)
        cleaned_file = os.path.join(self.temp_dir, "cleaned.txt")
        print("Step 1/3: Cleaning file (removing empty lines, trimming spaces)...")

        self._clean_file(input_file, cleaned_file)
        cleaned_lines = FileStats.count_lines(cleaned_file, show_progress=False)
        print(f"  After cleaning: {cleaned_lines:,} lines")

        # Step 2: Sort and deduplicate
        print("Step 2/3: Sorting and deduplicating...")

        self.sorter.external_sort(cleaned_file, output_file, deduplicate=True)

        # Step 3: Final statistics
        print("Step 3/3: Calculating final statistics...")

        final_lines = FileStats.count_lines(output_file, show_progress=False)
        final_size = FileStats.get_file_size(output_file)
        empty_lines_removed = initial_lines - cleaned_lines
        duplicates_removed = cleaned_lines - final_lines
        total_removed = initial_lines - final_lines

        end_time = datetime.now()
        duration = end_time - start_time

        # Cleanup temp file
        try:
            os.remove(cleaned_file)
        except:
            pass

        self._print_summary(
            initial_lines, final_lines, total_removed,
            empty_lines_removed, duplicates_removed,
            initial_size, final_size, input_file, output_file, duration
        )

        return output_file

    def _clean_file(self, input_file, output_file):
        """Clean file: remove empty lines and trim spaces"""
        with open(input_file, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as infile, \
                open(output_file, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as outfile:

            for line_num, line in enumerate(infile, 1):
                stripped = line.strip()
                if stripped:  # Skip empty lines
                    outfile.write(stripped + '\n')

                if line_num % 1000000 == 0:
                    print(f"  Cleaned {line_num:,} lines...")

    def merge_files(self, file1, file2, output_file=None):
        """Option 2: Merge two files, remove duplicates, and sort"""
        print("\n" + "=" * 60)
        print("OPTION 2: MERGE TWO FILES, DEDUPLICATE AND SORT")
        print("=" * 60)

        for f in [file1, file2]:
            if not os.path.exists(f):
                print(f"ERROR: File '{f}' not found!")
                return None

        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"merged_deduplicated_{timestamp}.txt"

        # Get statistics
        initial_lines1 = FileStats.count_lines(file1)
        initial_lines2 = FileStats.count_lines(file2)
        total_initial_lines = initial_lines1 + initial_lines2

        print(f"\nFile 1: {file1}")
        print(f"  Lines: {initial_lines1:,}")
        print(f"  Size: {FileStats.get_file_size(file1)}")

        print(f"\nFile 2: {file2}")
        print(f"  Lines: {initial_lines2:,}")
        print(f"  Size: {FileStats.get_file_size(file2)}")

        print(f"\nTotal lines to process: {total_initial_lines:,}")

        start_time = datetime.now()

        # Step 1: Clean and concatenate files
        cleaned1 = os.path.join(self.temp_dir, "cleaned1.txt")
        cleaned2 = os.path.join(self.temp_dir, "cleaned2.txt")

        print(f"Step 1/3: Cleaning files (removing empty lines)...")

        # Clean both files
        self._clean_file(file1, cleaned1)
        self._clean_file(file2, cleaned2)

        cleaned_lines1 = FileStats.count_lines(cleaned1, show_progress=False)
        cleaned_lines2 = FileStats.count_lines(cleaned2, show_progress=False)
        empty_removed = total_initial_lines - (cleaned_lines1 + cleaned_lines2)

        print(f"  Empty lines removed: {empty_removed:,}")

        # Step 2: Concatenate cleaned files
        cat_file = os.path.join(self.temp_dir, "concatenated.txt")
        print(f"Step 2/3: Concatenating cleaned files...")

        with open(cat_file, 'w', encoding='utf-8', buffering=BUFFER_SIZE) as outfile:
            for infile in [cleaned1, cleaned2]:
                with open(infile, 'r', encoding='utf-8', buffering=BUFFER_SIZE) as f:
                    shutil.copyfileobj(f, outfile)

        # Step 3: Sort and deduplicate
        print(f"Step 3/3: Sorting and deduplicating...")

        self.sorter.external_sort(cat_file, output_file, deduplicate=True)

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
        print("MERGE COMPLETE!")
        print("=" * 60)
        print(f"File 1 lines: {initial_lines1:,}")
        print(f"File 2 lines: {initial_lines2:,}")
        print(f"Total input lines: {total_initial_lines:,}")
        print(f"Final unique lines: {final_lines:,}")
        print(f"Empty lines removed: {empty_removed:,}")
        print(f"Duplicate lines removed: {duplicates_removed:,}")
        print(f"Total lines removed: {total_removed:,}")
        print(f"Compression: {(total_removed / total_initial_lines * 100):.2f}% reduction")
        print(f"\nTime taken: {duration}")
        print(f"Processing speed: {total_initial_lines / duration.total_seconds():,.0f} lines/sec")
        print(f"Output file: {output_file}")

        return output_file

    def search_in_file(self, filepath, search_term, case_sensitive=True, max_results=100):
        """Search for a term in a large file"""
        print("\n" + "=" * 60)
        print(f"SEARCHING IN: {os.path.basename(filepath)}")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"ERROR: File '{filepath}' not found!")
            return

        lines_count = FileStats.count_lines(filepath)
        print(f"Total lines in file: {lines_count:,}")

        if not case_sensitive:
            search_term = search_term.lower()

        print(f"Searching for: '{search_term}'")
        print(f"Case sensitive: {'Yes' if case_sensitive else 'No'}")
        print(f"Max results to show: {max_results}")
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
                            print(f"  Found {len(matches):,} matches so far...")

                        if len(matches) >= max_results * 10:  # Stop early if too many
                            print(f"\nStopping search - found {len(matches):,} matches (showing first {max_results})")
                            break

                    if line_num % 1000000 == 0:
                        print(f"  Scanned {line_num:,} lines...")

        except KeyboardInterrupt:
            print("\nSearch interrupted by user.")

        print("\n" + "=" * 60)
        print("SEARCH RESULTS:")
        print("=" * 60)

        if matches:
            print(f"Total matches found: {len(matches):,}")
            print(f"First {min(max_results, len(matches))} matches:")
            print("-" * 40)

            for i, (match, line_num) in enumerate(zip(matches[:max_results], line_numbers[:max_results]), 1):
                print(f"{i:4}. Line {line_num:,}: {match}")
        else:
            print("No matches found.")

        return matches

    def show_lines(self, filepath, start_line, end_line):
        """Display specific lines from a file"""
        print("\n" + "=" * 60)
        print(f"SHOWING LINES {start_line:,} to {end_line:,} FROM: {os.path.basename(filepath)}")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"ERROR: File '{filepath}' not found!")
            return

        lines_count = FileStats.count_lines(filepath)
        print(f"Total lines in file: {lines_count:,}")

        if start_line < 1:
            start_line = 1
        if end_line > lines_count:
            end_line = lines_count

        if start_line > end_line:
            print("ERROR: Start line must be less than or equal to end line")
            return

        print(f"Displaying lines {start_line:,} to {end_line:,}:")
        print("-" * 40)

        lines_displayed = 0
        with open(filepath, 'r', encoding='utf-8', errors='ignore', buffering=BUFFER_SIZE) as f:
            for line_num, line in enumerate(f, 1):
                if line_num < start_line:
                    continue
                if line_num > end_line:
                    break

                print(f"{line_num:8,}: {line.rstrip()}")
                lines_displayed += 1

        print(f"\nDisplayed {lines_displayed:,} lines.")

    def delete_characters(self, filepath, characters_to_delete, output_file=None):
        """Delete specific characters from a file"""
        print("\n" + "=" * 60)
        print(f"DELETING CHARACTERS FROM: {os.path.basename(filepath)}")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"ERROR: File '{filepath}' not found!")
            return None

        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{os.path.splitext(filepath)[0]}_cleaned_{timestamp}.txt"

        initial_lines = FileStats.count_lines(filepath)
        print(f"Total lines in file: {initial_lines:,}")
        print(f"Characters to delete: '{characters_to_delete}'")

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
                    print(f"  Processed {line_num:,} lines...")

        final_lines = FileStats.count_lines(output_file, show_progress=False)
        end_time = datetime.now()
        duration = end_time - start_time

        print("\n" + "=" * 60)
        print("CHARACTER DELETION COMPLETE!")
        print("=" * 60)
        print(f"Input file: {filepath}")
        print(f"Output file: {output_file}")
        print(f"Characters removed: '{characters_to_delete}'")
        print(f"Initial lines: {initial_lines:,}")
        print(f"Final lines: {final_lines:,}")
        print(f"Time taken: {duration}")
        print(f"Processing speed: {initial_lines / duration.total_seconds():,.0f} lines/sec")

        return output_file

    def get_statistics(self, filepath):
        """Get detailed statistics about a file"""
        print("\n" + "=" * 60)
        print(f"STATISTICS FOR: {os.path.basename(filepath)}")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"ERROR: File '{filepath}' not found!")
            return

        print("\nCalculating statistics...")

        # Basic file info
        size_bytes = os.path.getsize(filepath)
        size_human = FileStats.get_file_size(filepath)
        lines = FileStats.count_lines(filepath)

        print(f"\nFile Information:")
        print(f"  Path: {filepath}")
        print(f"  Size: {size_human} ({size_bytes:,} bytes)")
        print(f"  Lines: {lines:,}")

        # Estimate unique count
        unique_sample, uniqueness_rate = FileStats.get_unique_count(filepath)

        print(f"\nUniqueness Estimate (based on 1M sample):")
        print(f"  Unique values in sample: {unique_sample:,}")
        print(f"  Uniqueness rate: {uniqueness_rate:.2%}")
        print(f"  Estimated unique total: {int(lines * uniqueness_rate):,}")

        # First few lines
        print(f"\nFirst 5 lines:")
        print("-" * 40)
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            for i in range(5):
                line = f.readline()
                if not line:
                    break
                print(f"  {i + 1}: {line.rstrip()}")

        # Last few lines
        print(f"\nLast 5 lines:")
        print("-" * 40)
        try:
            # Read last lines manually
            with open(filepath, 'rb') as f:
                f.seek(-min(5000, os.path.getsize(filepath)), os.SEEK_END)
                last_lines = f.read().decode('utf-8', errors='ignore').split('\n')[-6:-1]
                for i, line in enumerate(last_lines, 1):
                    print(f"  {i}: {line}")
        except:
            print("  (Could not read last lines)")

        # File age
        mtime = os.path.getmtime(filepath)
        age = datetime.now() - datetime.fromtimestamp(mtime)
        print(f"\nFile Age: {age.days} days, {age.seconds // 3600} hours")

        return {
            'size_bytes': size_bytes,
            'lines': lines,
            'uniqueness_rate': uniqueness_rate
        }

    def split_file_by_prefix(self, filepath, output_dir=None):
        """Split file by Bitcoin address prefixes (1, 3, bc1)"""
        return BitcoinAddressProcessor.split_bitcoin_addresses(filepath, output_dir)

    def analyze_bitcoin_file(self, filepath, sample_size=100000):
        """Analyze Bitcoin addresses in file"""
        print("\n" + "=" * 60)
        print("BITCOIN ADDRESS ANALYSIS")
        print("=" * 60)

        if not os.path.exists(filepath):
            print(f"ERROR: Input file '{filepath}' not found!")
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
                    print(f"  Analyzed {i:,} addresses...")

        print("\nAnalysis Results:")
        print(f"Total addresses sampled: {total_count:,}")
        print(f"Valid Bitcoin addresses: {valid_count:,} ({valid_count / total_count * 100:.2f}%)")
        print(f"Invalid/Other addresses: {invalid_count:,} ({invalid_count / total_count * 100:.2f}%)")

        if valid_count > 0:
            print(f"\nValid Address Types:")
            for addr_type, count in sorted(address_types.items(), key=lambda x: x[1], reverse=True):
                percentage = count / valid_count * 100
                print(f"  {addr_type}: {count:,} ({percentage:.2f}%)")

        return {
            'total': total_count,
            'valid': valid_count,
            'invalid': invalid_count,
            'types': address_types
        }

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
                print(f"\nWARNING: Low disk space in {self.temp_dir}")
                print(f"Required: ~{FileStats._humanize_size(required_space)}")
                print(f"Available: {FileStats._humanize_size(free_space)}")
                print("Consider using a different TEMP_DIR with more space")
                if input("Continue anyway? (y/n): ").lower() != 'y':
                    return False
        except:
            # Fallback if we can't get disk space info
            print(f"Note: Using temp directory: {self.temp_dir}")
            print("Make sure you have at least 2x the input file size free space.")

        return True

    def _print_summary(self, initial_lines, final_lines, total_removed,
                       empty_removed, duplicates_removed,
                       initial_size, final_size, input_file, output_file, duration):
        """Print processing summary"""
        print("\n" + "=" * 60)
        print("PROCESSING COMPLETE!")
        print("=" * 60)
        print(f"Input file: {os.path.basename(input_file)}")
        print(f"Output file: {os.path.basename(output_file)}")
        print(f"\nInitial lines: {initial_lines:,}")
        print(f"Final lines: {final_lines:,}")
        print(f"Empty lines removed: {empty_removed:,}")
        print(f"Duplicate lines removed: {duplicates_removed:,}")
        print(f"Total lines removed: {total_removed:,}")
        print(f"Compression: {(total_removed / initial_lines * 100):.2f}% reduction")
        print(f"\nInitial size: {initial_size}")
        print(f"Final size: {final_size}")
        size_saved = os.path.getsize(input_file) - os.path.getsize(output_file)
        print(f"Space saved: {FileStats._humanize_size(size_saved)}")
        print(f"\nTime taken: {duration}")
        print(f"Processing speed: {initial_lines / duration.total_seconds():,.0f} lines/sec")


def interactive_menu():
    """Interactive menu for the user"""
    processor = LargeFileProcessor()

    while True:
        print("\n" + "=" * 60)
        print("WINDOWS LARGE FILE MANAGEMENT TOOL")
        print("=" * 60)
        print(f"Current working directory: {os.getcwd()}")
        print(f"Temp directory: {TEMP_DIR}")
        print(f"Memory limit: {MEMORY_LIMIT_GB} GB")
        print("=" * 60)

        print("\nMAIN MENU - What would you like to do?")
        print("1.  Deduplicate and sort main file")
        print("2.  Merge two files, deduplicate and sort")
        print("3.  Search in file")
        print("4.  Show specific lines from file")
        print("5.  Delete characters from file")
        print("6.  Get file statistics")
        print("7.  Count lines in file")
        print("8.  Split Bitcoin addresses by prefix (1, 3, bc1)")
        print("9.  Analyze Bitcoin addresses in file")
        print("10. Change configuration")
        print("11. Exit")

        choice = input("\nEnter your choice (1-11): ").strip()

        if choice == '1':
            file_path = input(f"Input file [{INPUT_FILE}]: ").strip() or INPUT_FILE
            output_path = input(f"Output file [{OUTPUT_FILE}]: ").strip() or OUTPUT_FILE
            processor.deduplicate_and_sort(file_path, output_path)

        elif choice == '2':
            file1 = input(f"First file [{INPUT_FILE}]: ").strip() or INPUT_FILE
            file2 = input(f"Second file [{SECOND_FILE}]: ").strip() or SECOND_FILE
            if not file2:
                print("Please specify a second file!")
                continue
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_output = f"merged_deduplicated_{timestamp}.txt"
            output_file = input(f"Output file [{default_output}]: ").strip() or default_output
            processor.merge_files(file1, file2, output_file)

        elif choice == '3':
            file_path = input(f"File to search [{INPUT_FILE}]: ").strip() or INPUT_FILE
            search_term = input("Search term: ").strip()
            if not search_term:
                print("Search term cannot be empty!")
                continue
            case_sensitive = input("Case sensitive? (y/n) [n]: ").strip().lower() != 'y'
            max_results = input("Max results to show [100]: ").strip()
            max_results = int(max_results) if max_results.isdigit() else 100
            processor.search_in_file(file_path, search_term, case_sensitive, max_results)

        elif choice == '4':
            file_path = input(f"File [{INPUT_FILE}]: ").strip() or INPUT_FILE
            start_line = input("Start line [1]: ").strip()
            start_line = int(start_line) if start_line.isdigit() else 1
            end_line = input("End line [100]: ").strip()
            end_line = int(end_line) if end_line.isdigit() else 100
            processor.show_lines(file_path, start_line, end_line)

        elif choice == '5':
            file_path = input(f"File [{INPUT_FILE}]: ").strip() or INPUT_FILE
            chars = input("Characters to delete (e.g., ',;\"): ").strip()
            if not chars:
                print("Please specify characters to delete!")
                continue
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            default_output = f"{os.path.splitext(file_path)[0]}_cleaned_{timestamp}.txt"
            output_file = input(f"Output file [{default_output}]: ").strip() or default_output
            processor.delete_characters(file_path, chars, output_file)

        elif choice == '6':
            file_path = input(f"File [{INPUT_FILE}]: ").strip() or INPUT_FILE
            processor.get_statistics(file_path)

        elif choice == '7':
            file_path = input(f"File [{INPUT_FILE}]: ").strip() or INPUT_FILE
            lines = FileStats.count_lines(file_path)
            print(f"\nTotal lines in {os.path.basename(file_path)}: {lines:,}")

        elif choice == '8':
            file_path = input(f"Bitcoin addresses file [{INPUT_FILE}]: ").strip() or INPUT_FILE
            output_dir = input(f"Output directory [current directory]: ").strip()
            if not output_dir:
                output_dir = os.path.dirname(file_path) or "."
            processor.split_file_by_prefix(file_path, output_dir)

        elif choice == '9':
            file_path = input(f"Bitcoin addresses file [{INPUT_FILE}]: ").strip() or INPUT_FILE
            sample_size = input("Sample size for analysis [100000]: ").strip()
            sample_size = int(sample_size) if sample_size.isdigit() else 100000
            print(f"\nAnalyzing first {sample_size:,} addresses...")
            processor.analyze_bitcoin_file(file_path, sample_size)

        elif choice == '10':
            print("\nCurrent configuration:")
            print(f"  INPUT_FILE: {INPUT_FILE}")
            print(f"  OUTPUT_FILE: {OUTPUT_FILE}")
            print(f"  SECOND_FILE: {SECOND_FILE}")
            print(f"  TEMP_DIR: {TEMP_DIR}")
            print(f"  MEMORY_LIMIT_GB: {MEMORY_LIMIT_GB}")
            print("\nNote: To permanently change configuration, edit the variables at the top of the script.")

        elif choice == '11':
            print("Exiting...")
            break

        else:
            print("Invalid choice!")

        input("\nPress Enter to continue...")


def display_banner():
    """Display program banner"""
    print("\n" + "=" * 60)
    print("WINDOWS BITCOIN ADDRESS MANAGER & LARGE FILE PROCESSOR")
    print("=" * 60)
    print("Features:")
    print("  • Deduplicate and sort 70GB+ files (Windows compatible)")
    print("  • Split Bitcoin addresses by type (1, 3, bc1)")
    print("  • Search, analyze, and manipulate large files")
    print("  • Merge files while removing duplicates")
    print("  • External sorting with minimal memory usage")
    print("  • No Linux commands required - pure Python")
    print("=" * 60)


def main():
    """Main entry point"""
    display_banner()

    print(f"\nCurrent Configuration:")
    print(f"  Input file: {INPUT_FILE}")
    print(f"  Output file: {OUTPUT_FILE}")
    if SECOND_FILE:
        print(f"  Second file: {SECOND_FILE}")
    print(f"  Temp directory: {TEMP_DIR}")
    print(f"  Memory limit: {MEMORY_LIMIT_GB} GB")
    print("=" * 60)

    # Check if input file exists
    if not os.path.exists(INPUT_FILE):
        print(f"\nWARNING: Input file '{INPUT_FILE}' not found!")
        print("You can still use other features or specify a different file.")
        if input("Continue? (y/n): ").lower() != 'y':
            sys.exit(1)

    # Run interactive menu
    try:
        interactive_menu()
    except KeyboardInterrupt:
        print("\n\nProgram interrupted by user. Exiting...")
        sys.exit(0)
    except Exception as e:
        print(f"\nError: {e}")
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