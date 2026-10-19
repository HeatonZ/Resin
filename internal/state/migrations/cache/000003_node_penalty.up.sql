ALTER TABLE nodes_dynamic
ADD COLUMN penalty_units INTEGER NOT NULL DEFAULT 0;

ALTER TABLE nodes_dynamic
ADD COLUMN penalty_updated_at_ns INTEGER NOT NULL DEFAULT 0;
