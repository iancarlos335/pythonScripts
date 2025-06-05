import argparse
from collections import Counter
import re
import sys # Import sys to handle script arguments

def process_file_strings(file_path, mode):
    """
    Processes a file to find either repeated or unique strings.

    Args:
        file_path (str): The path to the input file.
        mode (str): The operation mode, either "repeated" or "unique".
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Read the file and split by words using regex (to remove punctuation and handle various non-whitespace characters)
            text = file.read().lower()  # Convert to lowercase for case-insensitive matching
            # \S+ matches any sequence of non-whitespace characters.
            # This is a general way to get "words" or "strings" separated by spaces, tabs, newlines, etc.
            words = re.findall(r'\S+', text)
    except FileNotFoundError:
        print(f"Error: File not found at '{file_path}'")
        return
    except Exception as e:
        print(f"An error occurred while reading the file: {e}")
        return

    if not words:
        print("No words found in the file.")
        return

    # Count occurrences of each word
    word_counts = Counter(words)

    if mode == "repeated":
        # Filter words with more than one occurrence
        repeated_words = {word: count for word, count in word_counts.items() if count > 1}
        if repeated_words:
            print("Repeated strings and their occurrences:")
            for word, count in sorted(repeated_words.items()): # Sort for consistent output
                print(f"{word}: {count}")
        else:
            print("No repeated strings found.")
    elif mode == "unique":
        # Filter strings with only one occurrence
        unique_words = {word for word, count in word_counts.items() if count == 1} # Using a set for unique words
        if unique_words:
            print("Unique strings (appearing only once):")
            for word in sorted(list(unique_words)): # Sort for consistent output
                print(word)
        else:
            print("No unique strings found.")
    else:
        # This case should ideally be caught by argparse choices, but as a fallback:
        print(f"Error: Invalid mode '{mode}'. Choose 'repeated' or 'unique'.")

def main():
    """
    Main function to parse arguments and call the processing function.
    """
    # Create an ArgumentParser object
    parser = argparse.ArgumentParser(description="Find repeated or unique strings in a file.")

    # Add arguments
    # The first argument is the file path (positional)
    parser.add_argument("file_path", help="The path to the text file to analyze.")
    # The second argument is the mode (positional), with choices
    parser.add_argument("mode",
                        choices=["repeated", "unique"],
                        help="Operation mode: 'repeated' to find repeated strings, 'unique' to find unique strings.")

    # Parse the arguments from the command line
    # If running in an environment where sys.argv might be empty or not what's expected (e.g. some IDEs for testing)
    # you might need to provide arguments directly to parse_args for testing, e.g., parser.parse_args(['my_file.txt', 'repeated'])
    # However, for command-line execution, sys.argv is used by default.
    if len(sys.argv) <= 1: # If no arguments are passed (only script name)
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    # Call the function with the parsed arguments
    process_file_strings(args.file_path, args.mode)

if __name__ == "__main__":
    # This ensures that main() is called only when the script is executed directly
    # and not when it's imported as a module into another script.
    main()