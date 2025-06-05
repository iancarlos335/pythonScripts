import re

def clean_cnpj(cnpj):
    return re.sub(r'[^\d]', '', cnpj)

def find_repeated_strings(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        # Read the file and split by words using regex (to remove punctuation)
        text = file.read().lower()  # Convert to lowercase for case-insensitive matching
        words = re.findall(r'\S+', text)  # Match any sequence of non-whitespace characters

        for word in words:
            print(clean_cnpj(word))

# Example usage
file_path = "codigos.txt"  # Replace with the path to your file
find_repeated_strings(file_path)