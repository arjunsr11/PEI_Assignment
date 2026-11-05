-- Task 5d: Profit by Customer + Year
-- Per-customer yearly profit (orders without a matched customer are excluded).
SELECT customer_name AS customer,
       order_year AS year,
       ROUND(SUM(profit), 2) AS total_profit
FROM curated.profit_aggregates
WHERE customer_name IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2