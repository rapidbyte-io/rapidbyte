ALTER TABLE agents ADD COLUMN supported_operations TEXT[] NOT NULL DEFAULT '{}';
