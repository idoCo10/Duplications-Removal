from collections import Counter

# Define the file path
file_path = "duply.txt"

# First pass: Count occurrences of each line without loading the entire file into memory
line_counts = Counter()

with open(file_path, 'r') as file:
    for line in file:
        stripped_line = line.strip()
        line_counts[stripped_line] += 1

# Identify duplicates
duplicates = {line: count for line, count in line_counts.items() if count > 1}

# Track the total number of duplicates deleted
total_deleted = sum(count - 1 for count in duplicates.values())

# Print duplicate counts
if duplicates:
    for line, count in duplicates.items():
        print(f"{line:<40} {count} duplications.")
else:
    print("No duplicates found.")
    exit()

# Second pass: Write back only unique lines
seen = set()

with open(file_path, 'r') as file, open("Receiving_Addresses_without_duplications.txt", 'w') as output_file:
    for line in file:
        stripped_line = line.strip()
        if stripped_line not in seen:
            output_file.write(line)
            seen.add(stripped_line)

# Print the total number of deleted duplicates
print(f"\nDeleted {total_deleted} duplications in total.")
