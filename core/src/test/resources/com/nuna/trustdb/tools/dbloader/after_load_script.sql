--ALTER TABLE ${schema_name}.item__latest ADD PRIMARY KEY (id, secondary_id);
CREATE INDEX item_by_name ON ${schema_name}.item__latest(name);
CREATE INDEX item_by_date ON ${schema_name}.item__latest(date);
