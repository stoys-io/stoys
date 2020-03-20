CREATE OR REPLACE TEMPORARY VIEW tagged_value__create_view_statement AS (
SELECT value, tag FROM tagged_value
);

WITH
tagged_values__with_statement AS (
SELECT value, tag FROM tagged_value__create_view_statement
)
SELECT
  value,
  value * value AS squared_value
FROM
  tagged_values__with_statement
WHERE
  tag = "${tag}"
;


-- COMMAND ----------


/*
-- other cells
SELECT * FROM non_existing_table_is_fine_in_comment;
*/
