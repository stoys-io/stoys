WITH
populations AS (
  SELECT key AS country, population FROM demographics WHERE LENGTH(key) = 2
),
countries AS (
  SELECT DISTINCT key AS country FROM epidemiology WHERE LENGTH(key) = 2
),
latest_date AS (
  SELECT MAX(date) AS latest_date FROM epidemiology
),
last_year_dates AS (
  SELECT EXPLODE(SEQUENCE(DATE_SUB(ld.latest_date, 365), ld.latest_date, INTERVAL 1 DAY)) as date FROM latest_date AS ld
),
epidemiology_per_country_last_year AS (
  SELECT
    CONCAT("epidemiology_", c.country, ".xlsx") AS __path__,
    "data" AS __sheet__,
    lyd.date AS date,
    NVL(e.new_tested, 0) AS new_tested,
    NVL(e.new_confirmed, 0) AS new_confirmed,
    NVL(e.new_deceased, 0) / (p.population / 100000) AS new_deceased_per_100k_population
  FROM last_year_dates AS lyd
  FULL JOIN countries AS c
  LEFT JOIN epidemiology AS e ON lyd.date = e.date AND c.country = e.key
  LEFT JOIN populations AS p ON c.country = p.country
  ORDER BY c.country, lyd.date DESC
)
SELECT * FROM epidemiology_per_country_last_year
