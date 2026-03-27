-- Top 10 customers by billed amount (before tax)
SELECT
  i.customer_id,
  SUM(i.amount) AS total_amount
FROM invoices i
GROUP BY 1
QUALIFY ROW_NUMBER() OVER (ORDER BY total_amount DESC) <= 10;

-- Monthly billed revenue (before and after tax)
SELECT
  i.billing_month,
  SUM(i.amount) AS revenue_before_tax,
  SUM(i.amount_with_tax) AS revenue_after_tax
FROM invoices i
GROUP BY 1
ORDER BY 1;

-- Data usage by region
SELECT
  s.region,
  SUM(u.data_mb) AS total_data_mb
FROM usage_events u
JOIN subscribers s ON u.subscriber_id = s.subscriber_id
GROUP BY 1
ORDER BY 2 DESC;

-- Plan distribution and active subscriber count
SELECT
  p.plan_name,
  COUNT(*) AS active_subscribers
FROM subscribers s
JOIN plans p ON s.plan_id = p.plan_id
WHERE s.status = 'active'
GROUP BY 1
ORDER BY 2 DESC;

-- Abonnements les plus populaires (forfaits les plus utilises)
SELECT
  p.plan_name,
  COUNT(*) AS abonnement_count
FROM subscribers s
JOIN plans p ON s.plan_id = p.plan_id
GROUP BY 1
ORDER BY abonnement_count DESC;

-- Invoice payment coverage ratio by billing month
SELECT
  i.billing_month,
  SUM(COALESCE(pay.paid_amount, 0)) AS total_paid_amount,
  SUM(i.amount_with_tax) AS total_billed_amount,
  CASE
    WHEN SUM(i.amount_with_tax) = 0 THEN 0
    ELSE SUM(COALESCE(pay.paid_amount, 0)) / SUM(i.amount_with_tax)
  END AS payment_coverage_ratio
FROM invoices i
LEFT JOIN payments pay ON pay.invoice_id = i.invoice_id
GROUP BY 1
ORDER BY 1;


WITH popular_plans AS 
  (SELECT p.plan_name, COUNT(*) AS abonnement_count 
   FROM subscribers s 
   JOIN plans p ON s.plan_id = p.plan_id 
   WHERE s.subscribe_date >= DATEADD(MONTH, -5, CURRENT_DATE) 
   GROUP BY 1) 
SELECT plan_name, abonnement_count 
FROM popular_plans 
ORDER BY abonnement_count DESC