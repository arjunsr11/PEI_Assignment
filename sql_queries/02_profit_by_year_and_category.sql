-- Task 5b: Profit by Year + Product Category
-- (orders whose product is unknown are shown as 'N/A' so no profit is lost from the total)
SELECT order_year AS year,
       COALESCE(category, 'N/A') AS product_category,
       ROUND(SUM(profit), 2) AS total_profit
FROM curated.profit_aggregates
GROUP BY 1, 2
ORDER BY 1, 2