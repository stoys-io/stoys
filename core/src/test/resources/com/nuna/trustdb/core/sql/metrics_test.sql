SELECT
  MAP("grouped_by", tag) AS __labels__,
  COUNT(*) AS count,
  AVG(value) AS avg_value
FROM
  tagged_value
GROUP BY
  tag
;
