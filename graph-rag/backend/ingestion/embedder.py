"""Embedding service for generating text embeddings via OpenAI."""

import asyncio
from typing import Optional

from openai import AsyncOpenAI

from core.config import settings


class EmbeddingService:
    """Service for generating text embeddings."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        batch_size: int = 100,
        max_concurrent_batches: int = 5,
    ):
        """Initialize embedding service.

        Args:
            api_key: OpenAI API key
            model: Embedding model name
            batch_size: Maximum texts per batch
            max_concurrent_batches: Maximum number of batches to process concurrently
        """
        self.api_key = api_key or settings.openai_api_key
        self.model = model or settings.openai_embedding_model
        self.batch_size = batch_size
        self.max_concurrent_batches = max_concurrent_batches

        if not self.api_key:
            raise ValueError("OpenAI API key is required")

        self.client = AsyncOpenAI(api_key=self.api_key)
        self._cache: dict[str, list[float]] = {}

    async def generate_embedding(self, text: str) -> list[float]:
        """Generate embedding for a single text.

        Args:
            text: Input text

        Returns:
            Embedding vector
        """
        # Check cache
        if text in self._cache:
            return self._cache[text]

        # Generate embedding with optional dimensions parameter
        kwargs = {"model": self.model, "input": text}
        if settings.openai_embedding_dimensions:
            kwargs["dimensions"] = settings.openai_embedding_dimensions

        response = await self.client.embeddings.create(**kwargs)

        embedding = response.data[0].embedding

        # Cache result
        self._cache[text] = embedding

        return embedding

    async def generate_embeddings_batch(
        self, texts: list[str]
    ) -> list[list[float]]:
        """Generate embeddings for multiple texts in batches with parallel processing.

        Args:
            texts: List of input texts

        Returns:
            List of embedding vectors in the same order as input
        """
        if not texts:
            return []

        # Deduplicate while preserving order
        unique_texts = list(dict.fromkeys(texts))

        # Check cache first
        uncached_texts: list[str] = []
        cached_embeddings: dict[str, list[float]] = {}

        for text in unique_texts:
            if text in self._cache:
                cached_embeddings[text] = self._cache[text]
            else:
                uncached_texts.append(text)

        # If all cached, return immediately
        if not uncached_texts:
            return [cached_embeddings[text] for text in texts]

        # Split uncached texts into batches
        batches = [
            uncached_texts[i : i + self.batch_size]
            for i in range(0, len(uncached_texts), self.batch_size)
        ]

        # Process batches concurrently with semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.max_concurrent_batches)

        async def process_batch(batch: list[str]) -> dict[str, list[float]]:
            """Process a single batch with rate limiting."""
            async with semaphore:
                kwargs = {"model": self.model, "input": batch}
                if settings.openai_embedding_dimensions:
                    kwargs["dimensions"] = settings.openai_embedding_dimensions

                response = await self.client.embeddings.create(**kwargs)

                batch_embeddings = {}
                for text, data in zip(batch, response.data):
                    embedding = data.embedding
                    batch_embeddings[text] = embedding
                    self._cache[text] = embedding

                return batch_embeddings

        # Process all batches concurrently
        batch_results = await asyncio.gather(
            *[process_batch(batch) for batch in batches]
        )

        # Combine all batch results
        new_embeddings = {}
        for batch_result in batch_results:
            new_embeddings.update(batch_result)

        # Combine cached and new embeddings
        all_embeddings = {**cached_embeddings, **new_embeddings}

        # Return in original order (including duplicates)
        return [all_embeddings[text] for text in texts]

    def clear_cache(self) -> None:
        """Clear the embedding cache."""
        self._cache.clear()

    def get_cache_size(self) -> int:
        """Get number of cached embeddings.

        Returns:
            Cache size
        """
        return len(self._cache)
