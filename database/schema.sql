-- database/schema.sql

-- Enum Types
CREATE TYPE job_status_enum AS ENUM (
    'QUEUED',
    'PROCESSING',
    'COMPLETED',
    'FAILED'
);

CREATE TYPE race_category_enum AS ENUM (
    'FULL_MARATHON',
    'HALF_MARATHON',
    '10K',
    '5K'
);

-- upload_jobs table
CREATE TABLE IF NOT EXISTS upload_jobs (
    id UUID PRIMARY KEY,
    file_name VARCHAR(255) NOT NULL,
    file_hash VARCHAR(64) UNIQUE NOT NULL,
    status job_status_enum DEFAULT 'QUEUED' NOT NULL,
    total_rows INT DEFAULT 0,
    processed_rows INT DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- marathon_participants table
CREATE TABLE IF NOT EXISTS marathon_participants (
    id SERIAL PRIMARY KEY,
    job_id UUID REFERENCES upload_jobs(id) ON DELETE CASCADE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    bib_number VARCHAR(50),
    category race_category_enum NOT NULL,
    registration_fee NUMERIC(10, 2)
);

CREATE TABLE IF NOT EXISTS job_errors (
    id SERIAL PRIMARY KEY,
    job_id UUID REFERENCES upload_jobs(id) ON DELETE CASCADE,
    row_number INT NOT NULL,
    error_message TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);