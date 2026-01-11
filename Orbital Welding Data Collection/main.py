# simple_unique_count.py

# Replace 'your_file.txt' with the path to your text file
file_path = 'fix.txt'

# Use a set to automatically keep only unique items
unique_items = set()

# Open the file and read line by line
with open(file_path, 'r', encoding='utf-8') as file:
    for line in file:
        # Strip whitespace and newlines
        line = line.strip()
        if line:  # ignore empty lines
            unique_items.add(line)

# Print the count of unique items
print("Total unique items:", len(unique_items))

# Optional: print all unique items
# for item in unique_items:
#     print(item)

