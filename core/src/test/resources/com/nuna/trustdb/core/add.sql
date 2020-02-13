SELECT foo.id AS id, foo.value + bar.value AS value FROM foo INNER JOIN bar ON foo.id = bar.id;
