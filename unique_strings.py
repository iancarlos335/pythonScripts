from collections import Counter
import re

def find_unique_strings(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        # Read the file and split by any non-whitespace characters
        text = file.read().lower()  # Convert to lowercase for case-insensitive matching
        words = re.findall(r'\S+', text)  # Match any sequence of non-whitespace characters
    
    # Count occurrences of each word/string
    word_counts = Counter(words)
    
    # Filter strings with only one occurrence
    unique_words = {word: count for word, count in word_counts.items() if count == 1}

    # Display results
    if unique_words:
        print("Unique strings (appearing only once):")
        for word in unique_words:
            print(word)
    else:
        print("No unique strings found.")

# Example usage
file_path = "codigos.txt"  # Replace with the path to your file
find_unique_strings(file_path)
