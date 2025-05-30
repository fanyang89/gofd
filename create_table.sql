CREATE SEQUENCE IF NOT EXISTS file_ids START 1;

CREATE TABLE IF NOT EXISTS files(
    "id" BIGINT PRIMARY KEY DEFAULT nextval('file_ids'),
    "path" VARCHAR UNIQUE NOT NULL,
    "hash" UBIGINT
);

CREATE TABLE IF NOT EXISTS file_chunks(
    "file_id" BIGINT,
    "offset" UBIGINT,
    "len" UBIGINT,
    "hash" VARCHAR,
    FOREIGN KEY (file_id) REFERENCES files (id)
);
