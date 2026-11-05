-- Task 5c: Profit by Customer
-- Top customers by lifetime profit (orders without a matched customer are excluded).
SELECT customer_name AS customer,
       ROUND(SUM(profit), 2) AS total_profit
FROM curated.profit_aggregates
WHERE customer_name IS NOT NULL
GROUP BY 1
ORDER BY total_profit DESC