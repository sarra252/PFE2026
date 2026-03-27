-- VIEWS
DROP VIEW stg_customers_v;
DROP VIEW stg_invoices_v;
DROP VIEW stg_payments_v;
DROP VIEW stg_plans_v;
DROP VIEW stg_subscribers_v;
DROP VIEW stg_usage_events_v;

-- TABLES TECHNIQUES TPT
DROP TABLE stg_customers_log;
DROP TABLE stg_customers_e1;
DROP TABLE stg_customers_e2;

DROP TABLE stg_invoices_log;
DROP TABLE stg_invoices_e1;
DROP TABLE stg_invoices_e2;

DROP TABLE stg_payments_log;
DROP TABLE stg_payments_e1;
DROP TABLE stg_payments_e2;

DROP TABLE stg_plans_log;
DROP TABLE stg_plans_e1;
DROP TABLE stg_plans_e2;

DROP TABLE stg_subscribers_log;
DROP TABLE stg_subscribers_e1;
DROP TABLE stg_subscribers_e2;

DROP TABLE stg_usage_events_log;
DROP TABLE stg_usage_events_e1;
DROP TABLE stg_usage_events_e2;

-- TABLES STAGING
DROP TABLE stg_customers;
DROP TABLE stg_invoices;
DROP TABLE stg_payments;
DROP TABLE stg_plans;
DROP TABLE stg_subscribers;
DROP TABLE stg_usage_events;

-- TABLES FINALES (si presentes)
DROP TABLE customers;
DROP TABLE invoices;
DROP TABLE payments;
DROP TABLE plans;
DROP TABLE subscribers;
DROP TABLE usage_events;


DROP TABLE stg_usage_events_e1;
DROP TABLE stg_usage_events_e2;
DROP TABLE stg_usage_events;


DATABASE demo_user;

SELECT
  TableName,
  TableKind
FROM DBC.TablesV
WHERE DatabaseName = 'demo_user'
ORDER BY TableKind, TableName;

SELECT COUNT(*) FROM stg_usage_events;


SELECT COUNT(*) FROM stg_usage_events;


-- Droit d'accéder au schema
GRANT SELECT ON demo_user TO other_user;

-- Droits sur tables (lecture)
GRANT SELECT ON demo_user.stg_customers TO other_user;
GRANT SELECT ON demo_user.stg_invoices TO other_user;
GRANT SELECT ON demo_user.stg_payments TO other_user;
GRANT SELECT ON demo_user.stg_plans TO other_user;
GRANT SELECT ON demo_user.stg_subscribers TO other_user;
GRANT SELECT ON demo_user.stg_usage_events TO other_user;
