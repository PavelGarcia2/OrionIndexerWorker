import re
from collections import defaultdict
from typing import Any

import nltk
import logging
import pandas as pd
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer

from src.entities.Page import Page
from src.infrastructure.OrionDBClient import OrionDBClient

# Download required NLTK resources
nltk.download('stopwords')

# Setup logging
logging.basicConfig(level=logging.INFO)
db_config = {
    "dbname": "orion_se",
    "user": "postgres",
    "password": "",
    "host": "localhost",
    "port": "5432"
}

# Initialize NLP tools
stop_words = set(stopwords.words('english'))
stemmer = PorterStemmer()

db = OrionDBClient(db_config)

def clean_and_tokenize(text: str) -> tuple[list[Any], list[Any]]:
    """Clean, tokenize, remove stopwords, and stem."""
    # Remove emojis, punctuation, symbols
    cleaned = re.sub(r'[^A-Za-z0-9\s]', '', text)
    cleaned = cleaned.lower()

    # Tokenize
    tokens = re.findall(r'\b\w+\b', cleaned)

    # Remove stopwords (keep digits if length > 1)
    filtered = [
        t for t in tokens
        if t not in stop_words and (not t.isdigit() or len(t) > 1)
    ]

    # Stemming
    stemmed = [stemmer.stem(t) for t in filtered]
    return filtered, stemmed

def extract_term_positions(tokens: list[str]) -> dict[str, list[int]]:
    term_positions = defaultdict(list)
    for i, token in enumerate(tokens):
        term_positions[token].append(i)
    return term_positions

def process_page(page: Page, doc_number: int):
    """Process and display token analysis of a single page."""
    combined_text = f"{page.title or ''} {page.summary or ''} {page.content or ''}"
    filtered, stemmed = clean_and_tokenize(combined_text)
    term_data = extract_term_positions(stemmed)
    db.insert_terms( url_id=page.url_id, term_data=term_data)
    df = pd.DataFrame({
        'Original Token': filtered,
        'Stemmed': stemmed
    })

    print(f"\n=== Document {doc_number} (url_id={page.url_id}) ===")
    print(df.to_string(index=False))


def main():

    last_id = 0
    doc_num = 1
    batch_size = 5

    print('Starting the process')
    while True:

        pages, last_id = db.get_next_pages(last_id, limit=batch_size)
        print(f'Retrieving pages: {[page.url_id for page in pages]}')

        if not pages:
            print('All pages handled')
            break

        for page in pages:
            process_page(page, doc_num)
            doc_num += 1


if __name__ == "__main__":
    main()
