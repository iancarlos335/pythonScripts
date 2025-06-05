from collections import Counter
import re

def find_repeated_strings(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        # Read the file and split by words using regex (to remove punctuation)
        text = file.read().lower()  # Convert to lowercase for case-insensitive matching
        words = re.findall(r'\S+', text)  # Match any sequence of non-whitespace characters
    
    # Count occurrences of each word
    word_counts = Counter(words)
    
    # Filter words with more than one occurrence
    repeated_words = {word: count for word, count in word_counts.items() if count > 1}

    # Display results
    if repeated_words:
        print("Repeated strings and their occurrences:")
        for word, count in repeated_words.items():
            print(f"{word}: {count}")
    else:
        print("No repeated strings found.")

# Example usage
file_path = "your_file.txt"  # Replace with the path to your file
find_repeated_strings(file_path)
