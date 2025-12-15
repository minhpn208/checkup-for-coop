-- init.sql
CREATE EXTENSION IF NOT EXISTS vector;

-- Create daily_checkups table with vector support
CREATE TABLE IF NOT EXISTS daily_checkups (
    checkup TEXT NOT NULL,
    embedding vector(1536),  -- OpenAI text-embedding-3-small dimension
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB
);

-- Create index for vector similarity search (HNSW is faster than IVFFlat for most cases)
CREATE INDEX IF NOT EXISTS daily_checkups_embedding_idx 
ON daily_checkups USING hnsw (embedding vector_cosine_ops);

-- Create index for date queries
CREATE INDEX IF NOT EXISTS daily_checkups_entry_time_idx 
ON daily_checkups (entry_time);

-- Fix collation version mismatch
ALTER DATABASE "checkup-for-coop" REFRESH COLLATION VERSION;