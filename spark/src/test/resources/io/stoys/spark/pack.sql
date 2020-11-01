SELECT
  add.id AS id,
  CAST(${add_multiplier} * add.value + ${multiply_multiplier} * multiply.value AS LONG) AS value
FROM add INNER JOIN multiply ON add.id = multiply.id;
