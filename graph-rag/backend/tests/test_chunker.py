from ingestion.chunker import chunk_text

def test_chunk_text_empty():
    assert chunk_text("") == []

def test_chunk_text_small():
    text = "Short text."
    assert chunk_text(text) == [text]

def test_chunk_text_split():
    text = "A" * 1500
    chunks = chunk_text(text, chunk_size=1000, chunk_overlap=100)
    assert len(chunks) > 1
    assert len(chunks[0]) <= 1000

def test_chunk_text_sentence_boundary():
    # Create text with clear sentence boundaries
    s1 = "This is the first sentence. "
    s2 = "This is the second sentence. "
    s3 = "This is the third sentence."
    text = s1 + s2 + s3
    
    # Force split in middle of s2 if we didn't respect boundaries
    # s1 is ~28 chars. s2 is ~29 chars.
    # Set chunk size to cover s1 and part of s2
    chunks = chunk_text(text, chunk_size=40, chunk_overlap=10)
    
    # Should ideally split after s1
    assert chunks[0] == s1.strip()
    assert s2.strip() in chunks[1]
