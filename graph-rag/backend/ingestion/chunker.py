"""Text chunking utilities."""

from typing import List


def chunk_text(
    text: str,
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
) -> List[str]:
    """Split text into overlapping chunks.

    Args:
        text: Input text
        chunk_size: Maximum characters per chunk
        chunk_overlap: Overlap between chunks

    Returns:
        List of text chunks
    """
    if not text:
        return []

    if len(text) <= chunk_size:
        return [text]

    chunks: List[str] = []
    start = 0

    while start < len(text):
        end = start + chunk_size

        # Try to break at sentence boundary
        if end < len(text):
            # Look for sentence endings
            for delimiter in [". ", ".\n", "! ", "? "]:
                last_delimiter = text[start:end].rfind(delimiter)
                if last_delimiter > chunk_size // 2:
                    end = start + last_delimiter + len(delimiter)
                    break

        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)

        # Move start position with overlap
        start = end - chunk_overlap
        if start >= len(text):
            break

    return chunks
