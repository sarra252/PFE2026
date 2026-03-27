SELECT customer_id, SUM(amount) total_amount FROM invoices GROUP BY 1 QUALIFY ROW_NUMBER() OVER (ORDER BY total_amount DESC) <= 10;
SELECT billing_month, SUM(amount) revenue FROM invoices GROUP BY 1 ORDER BY 1;
SELECT s.region, SUM(u.data_mb) total_data_mb FROM usage_events u JOIN subscribers s ON u.subscriber_id=s.subscriber_id GROUP BY 1 ORDER BY 2 DESC;
