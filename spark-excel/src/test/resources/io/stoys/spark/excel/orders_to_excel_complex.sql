WITH
order_indexed AS (
  SELECT monotonically_increasing_id() AS __rowid__, * FROM order
),
all_orders AS (
  SELECT COLLECT_LIST(STRUCT(*)) AS rows FROM order_indexed GROUP BY true
),
orders_by_customer AS (
  SELECT customer, COLLECT_LIST(STRUCT(*)) AS rows FROM order_indexed GROUP BY customer
),
cost_by_customer_and_item AS (
  SELECT
    monotonically_increasing_id() AS __rowid__,
    customer,
    item,
    SUM(cost_cents) AS cost_cents
  FROM order_indexed
  GROUP BY customer, item
  ORDER BY customer, item
),
spend_per_item_by_customer AS (
  SELECT customer, COLLECT_LIST(STRUCT(*)) AS rows FROM cost_by_customer_and_item GROUP BY customer
)
SELECT
  CONCAT(orders_by_customer.customer, "/", "orders_", REGEXP_REPLACE(orders_by_customer.customer, "\\W+", "_"), ".xlsx")
      AS path,
  to_excel(
    NAMED_STRUCT(
      "Orders", all_orders.rows,
      "Orders By This Customer", orders_by_customer.rows,
      "Spend Per Item By This Customer", spend_per_item_by_customer.rows
    ),
    MAP("template_xlsx", (SELECT FIRST(content) FROM orders_template_xlsx))
  ) AS content
FROM orders_by_customer
LEFT JOIN spend_per_item_by_customer ON spend_per_item_by_customer.customer = orders_by_customer.customer
CROSS JOIN all_orders
