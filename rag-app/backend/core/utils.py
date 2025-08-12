"""
Shared utilities for text processing and embeddings
"""

from typing import List
import tiktoken


def split_text_by_tokens(
    text: str, max_tokens: int = 500, overlap_tokens: int = 50
) -> List[str]:
    """Split text into chunks by token count with overlap"""
    encoding = tiktoken.get_encoding("cl100k_base")
    tokens = encoding.encode(text)

    if len(tokens) <= max_tokens:
        return [text]

    chunks = []
    start = 0
    while start < len(tokens):
        end = start + max_tokens
        chunk_tokens = tokens[start:end]
        chunk_text = encoding.decode(chunk_tokens)
        chunks.append(chunk_text)
        start = end - overlap_tokens

    return chunks


def calculate_token_count(text: str) -> int:
    """Calculate the number of tokens in a text"""
    encoding = tiktoken.get_encoding("cl100k_base")
    return len(encoding.encode(text))


def truncate_text_to_tokens(text: str, max_tokens: int) -> str:
    """Truncate text to fit within token limit"""
    encoding = tiktoken.get_encoding("cl100k_base")
    tokens = encoding.encode(text)
    
    if len(tokens) <= max_tokens:
        return text
    
    truncated_tokens = tokens[:max_tokens]
    return encoding.decode(truncated_tokens)
