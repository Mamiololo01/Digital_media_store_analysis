-------Task 1.1 – Revenue Analysis
----Calculate the total revenue generated by each country

WITH RevPerCountry AS (
    SELECT c.Country, SUM(i.Total) AS Revenue
    FROM Invoice i
    JOIN Customer c ON i.customer_id = c.customer_id
    GROUP BY c.Country
)
SELECT 
    c.Country,
    SUM(i.Total) AS Revenue,
    ROUND((SUM(i.Total) / (SELECT SUM(Total) FROM Invoice)) * 100, 2) AS RevenuePct
FROM Invoice i
JOIN Customer c ON i.customer_id = c.customer_id
GROUP BY c.Country
ORDER BY Revenue DESC;

--------Task 1.2 – Customer Segmentation
------Identify the top 10 customers based on total spend.

WITH customer_spend AS (
    SELECT
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        SUM(i.Total) AS total_spend,
        RANK() OVER (ORDER BY SUM(i.Total) DESC) AS customer_rank
    FROM Invoice i
    JOIN Customer c ON i.customer_id = c.customer_id
    GROUP BY c.Customer_id
)
SELECT
    customer_name,
    total_spend,
    customer_rank
FROM customer_spend
WHERE customer_rank <= 10;