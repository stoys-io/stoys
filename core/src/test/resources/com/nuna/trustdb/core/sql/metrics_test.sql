SELECT
  tag AS __metric_tag__,
  MAP("grouped_by", tag) AS __metric_labels__,
  COUNT(*) AS count,
  AVG(value) AS avg_value
FROM
  tagged_value
GROUP BY
  tag
;
