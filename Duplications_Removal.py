from collections import Counter

# Define the file path
file_path = "dupli.txt"

# Open the file and read all lines
with open(file_path, 'r') as file:
    lines = file.readlines()

# Strip the newline characters from each line to handle lines consistently
stripped_lines = [line.strip() for line in lines]

# Count occurrences of each stripped line
line_counts = Counter(stripped_lines)

# Find duplicates
duplicates = {line: count for line, count in line_counts.items() if count > 1}

# Track the total number of duplicates deleted
total_deleted = 0

if duplicates:
    # Print duplicates and their counts
    for line, count in duplicates.items():
        print(f"{line:<40} {count} duplications.")
        total_deleted += (count - 1)  # Subtract 1 to account for keeping the first occurrence
else:
    print("No duplicates found.")

# Remove duplicates while keeping the first occurrence
unique_lines = []
seen = set()

for line, stripped_line in zip(lines, stripped_lines):
    if stripped_line not in seen:
        unique_lines.append(line)
        seen.add(stripped_line)

# Write the unique lines back to the file
with open(file_path, 'w') as file:
    file.writelines(unique_lines)

# Print the total number of deleted duplicates
if total_deleted > 0:
    print(f"\nDeleted {total_deleted} duplications in total.")
else:
    print("\nNo duplications to delete.")
