# scripts/create_log_metadata_tables.py
from sqlalchemy import create_engine, text
import os

url = os.environ["SQLALCHEMY_DATABASE_URI"]
engine = create_engine(url)

ddl = """
CREATE TABLE IF NOT EXISTS log_metadata_header (
  id INT AUTO_INCREMENT PRIMARY KEY,
  raw_data_id INT NOT NULL,
  log_uid VARCHAR(255) NOT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  source_json JSON NULL,
  UNIQUE KEY _log_metadata_header_raw_data_uc (raw_data_id),
  INDEX ix_lmh_raw_data_id (raw_data_id),
  INDEX ix_lmh_log_uid (log_uid),
  CONSTRAINT fk_lmh_raw_data FOREIGN KEY (raw_data_id) REFERENCES raw_data(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS log_metadata_attr (
  id INT AUTO_INCREMENT PRIMARY KEY,
  header_id INT NOT NULL,
  attr_key VARCHAR(255) NOT NULL,
  attr_value VARCHAR(1024) NOT NULL,
  created_at DATETIME NULL,
  updated_at DATETIME NULL,
  UNIQUE KEY _log_metadata_attr_uc (header_id, attr_key, attr_value),
  INDEX ix_lma_header_id (header_id),
  INDEX ix_lma_key (attr_key),
  INDEX ix_lma_value (attr_value),
  CONSTRAINT fk_lma_header FOREIGN KEY (header_id) REFERENCES log_metadata_header(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

with engine.begin() as conn:
    for stmt in ddl.strip().split(";\n\n"):
        if stmt.strip():
            conn.execute(text(stmt))
print("Tables created/ensured.")
