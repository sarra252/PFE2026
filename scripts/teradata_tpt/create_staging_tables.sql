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


DROP TABLE stg_customers;
DROP TABLE stg_invoices;
DROP TABLE stg_payments;
DROP TABLE stg_plans;
DROP TABLE stg_subscribers;
DROP TABLE stg_usage_events;


CREATE MULTISET TABLE stg_customers (
  customer_id VARCHAR(20),
  cust_id VARCHAR(30),
  customer_name VARCHAR(200),
  segment VARCHAR(20),
  region VARCHAR(20),
  status VARCHAR(20),
  created_at DATE,
  channel VARCHAR(50),
  risk_score DECIMAL(9, 4),
  revenue_tier VARCHAR(30),
  contact_email VARCHAR(320),
  contact_phone VARCHAR(30)
) NO PRIMARY INDEX;


CREATE MULTISET TABLE stg_invoices (
  invoice_id VARCHAR(20),
  customer_id VARCHAR(20),
  billing_month CHAR(7),
  issued_at DATE,
  due_date DATE,
  amount DECIMAL(18, 2),
  currency CHAR(3),
  invoice_status VARCHAR(20),
  tax_rate DECIMAL(5, 4),
  amount_with_tax DECIMAL(18, 2)
) NO PRIMARY INDEX;


CREATE MULTISET TABLE stg_payments (
  payment_id VARCHAR(20),
  invoice_id VARCHAR(20),
  customer_ref VARCHAR(20),
  paid_at DATE,
  paid_amount DECIMAL(18, 2),
  payment_method VARCHAR(40),
  payment_status VARCHAR(20)
) NO PRIMARY INDEX;

CREATE MULTISET TABLE stg_plans (
  plan_id VARCHAR(40),
  plan_name VARCHAR(100),
  plan_family VARCHAR(20),
  monthly_fee DECIMAL(18, 2),
  data_quota_gb INTEGER,
  voice_quota_min INTEGER,
  sms_quota INTEGER,
  currency CHAR(3)
) NO PRIMARY INDEX;


CREATE MULTISET TABLE stg_subscribers (
  subscriber_id VARCHAR(20),
  subscriber_key VARCHAR(30),
  client_id VARCHAR(20),
  subscriber_id_alt VARCHAR(30),
  msisdn VARCHAR(20),
  imsi VARCHAR(20),
  plan_id VARCHAR(40),
  status VARCHAR(20),
  activation_date DATE,
  churn_date VARCHAR(10),
  region VARCHAR(20)
) NO PRIMARY INDEX;


CREATE MULTISET TABLE stg_usage_events (
  event_id VARCHAR(20),
  subscriber_id VARCHAR(20),
  event_ts VARCHAR(19),
  event_type VARCHAR(20),
  data_mb DECIMAL(18, 2),
  voice_min DECIMAL(18, 2),
  sms_count INTEGER,
  cell_id VARCHAR(30),
  roaming_flag BYTEINT
) NO PRIMARY INDEX;