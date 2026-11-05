-- Task 5a: Profit by Year
-- Rolls the Task 4 aggregate (curated.profit_aggregates) up to calendar year.
SELECT order_year AS year,
       ROUND(SUM(profit), 2) AS total_profit
FROM curated.profit_aggregates
GROUP BY 1
ORDER BY 1