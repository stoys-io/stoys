CREATE OR REPLACE TEMPORARY VIEW tmp_dummy AS
  (SELECT value FROM tagged_value);

WITH tmp_tagged_values AS
  (SELECT value, tags FROM tagged_value)
SELECT
  value AS value,
  value * value AS squared_value
FROM
  tmp_tagged_values
WHERE
  ARRAY_CONTAINS(tags, "${tag}")
;


-- COMMAND ----------


/*
-- other cells
SELECT * FROM foo;
*/
