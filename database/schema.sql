-- database/schema.sql

-- Enum Types
DO $$ BEGIN
    CREATE TYPE job_status_enum AS ENUM ('QUEUED', 'PROCESSING', 'COMPLETED', 'FAILED');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE race_category_enum AS ENUM ('FULL_MARATHON', 'HALF_MARATHON', '10K', '5K');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- upload_jobs table
CREATE TABLE IF NOT EXISTS upload_jobs (
    id              UUID PRIMARY KEY,
    file_name       VARCHAR(255) NOT NULL,
    blob_name       VARCHAR(300) NOT NULL,
    status          job_status_enum NOT NULL DEFAULT 'QUEUED',
    total_rows      INT NOT NULL DEFAULT 0,  
    processed_rows  INT NOT NULL DEFAULT 0,   
    inserted_rows   INT NOT NULL DEFAULT 0,
    duplicate_rows  INT NOT NULL DEFAULT 0,
    invalid_rows    INT NOT NULL DEFAULT 0,
    attempt_count   INT NOT NULL DEFAULT 0,   
    error_message   TEXT,                    
    created_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP, 
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_upload_jobs_status ON upload_jobs (status);

-- marathon_participants table
CREATE TABLE IF NOT EXISTS marathon_participants (
    id                SERIAL PRIMARY KEY,
    job_id            UUID NOT NULL REFERENCES upload_jobs(id) ON DELETE CASCADE,
    row_number        INT NOT NULL,           -- Excel row the record came from
    first_name        VARCHAR(100) NOT NULL,
    last_name         VARCHAR(100) NOT NULL,
    email             VARCHAR(255) NOT NULL,  -- stored lower-case, trimmed
    bib_number        VARCHAR(50)  NOT NULL,  -- stored upper-case, trimmed
    category          race_category_enum NOT NULL,
    registration_fee  NUMERIC(10, 2),
    CONSTRAINT uq_participant_email_bib UNIQUE (email, bib_number)
);

CREATE INDEX IF NOT EXISTS idx_participants_job ON marathon_participants (job_id);

-- job_errors: row-level problems (invalid data or duplicates)
CREATE TABLE IF NOT EXISTS job_errors (
    id             SERIAL PRIMARY KEY,
    job_id         UUID NOT NULL REFERENCES upload_jobs(id) ON DELETE CASCADE,
    row_number     INT NOT NULL,
    error_message  TEXT NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_job_errors_job ON job_errors (job_id, row_number);