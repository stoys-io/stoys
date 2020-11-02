WITH
latest_date AS (
  SELECT MAX(date) AS latest_date FROM epidemiology
),
epidemiology_level1_last30days AS (
  SELECT
    key,
    SUBSTRING(key, 0, 2) AS country,
    SUBSTRING(key, 4) AS region,
    SUM(new_tested) AS new_tested,
    SUM(new_confirmed) AS new_confirmed,
    SUM(new_deceased) AS new_deceased
  FROM epidemiology
  WHERE
    date BETWEEN DATE_SUB((SELECT MAX(latest_date) FROM latest_date), 30) AND (SELECT MAX(latest_date) FROM latest_date)
  AND
    key RLIKE '^[^_]*_[^_]*$'
  GROUP BY key
),
epidemiology_level1_last30days_numbered AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY new_confirmed DESC) AS row_number
  FROM epidemiology_level1_last30days
),
epidemiology_level1_last30days_top10 AS (
  SELECT
    CONCAT("epidemiology_", country, ".xlsx") AS __path__,
    "Top 10 Regions in Last 30 Days" AS __sheet__,
    key,
    new_tested,
    new_confirmed,
    new_deceased
  FROM epidemiology_level1_last30days_numbered
  WHERE row_number <= 10
)
SELECT * FROM epidemiology_level1_last30days_top10
