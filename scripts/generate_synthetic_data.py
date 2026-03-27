#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
import random
import statistics
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data_synth"

PLANS = [
    {"plan_id": "P_BASIC_10", "plan_name": "Basic 10GB", "plan_family": "B2C", "monthly_fee": 9.99, "data_quota_gb": 10},
    {"plan_id": "P_PLUS_40", "plan_name": "Plus 40GB", "plan_family": "B2C", "monthly_fee": 19.99, "data_quota_gb": 40},
    {"plan_id": "P_PREMIUM_120", "plan_name": "Premium 120GB", "plan_family": "B2C", "monthly_fee": 39.99, "data_quota_gb": 120},
    {"plan_id": "P_BIZ_200", "plan_name": "Business 200GB", "plan_family": "B2B", "monthly_fee": 89.00, "data_quota_gb": 200},
]


@dataclass
class Cfg:
    seed: int = 42
    output_root: str = str(DEFAULT_OUTPUT_ROOT)
    start_date: str = "2024-01-01"
    end_date: str = "2025-12-31"
    customers: int = 12000
    max_subscribers: int = 6
    events_per_subscriber_per_month: int = 22
    max_usage_events: int = 5_000_000
    orphan_subscriber_rate: float = 0.08
    orphan_usage_rate: float = 0.06
    null_rate: float = 0.04
    duplicate_msisdn_rate: float = 0.015
    id_noise_rate: float = 0.03
    outlier_usage_rate: float = 0.002
    parquet: bool = False


def parse_args() -> Cfg:
    p = argparse.ArgumentParser(description="Generate enriched telecom synthetic data with metadata and quality reports.")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    p.add_argument("--start-date", default="2024-01-01")
    p.add_argument("--end-date", default="2025-12-31")
    p.add_argument("--customers", type=int, default=12000)
    p.add_argument("--max-subscribers", type=int, default=6)
    p.add_argument("--events-per-subscriber-per-month", type=int, default=22)
    p.add_argument("--max-usage-events", type=int, default=5_000_000)
    p.add_argument("--orphan-subscriber-rate", type=float, default=0.08)
    p.add_argument("--orphan-usage-rate", type=float, default=0.06)
    p.add_argument("--null-rate", type=float, default=0.04)
    p.add_argument("--duplicate-msisdn-rate", type=float, default=0.015)
    p.add_argument("--id-noise-rate", type=float, default=0.03)
    p.add_argument("--outlier-usage-rate", type=float, default=0.002)
    p.add_argument("--parquet", action="store_true")
    a = p.parse_args()
    return Cfg(
        a.seed,
        a.output_root,
        a.start_date,
        a.end_date,
        a.customers,
        a.max_subscribers,
        a.events_per_subscriber_per_month,
        a.max_usage_events,
        a.orphan_subscriber_rate,
        a.orphan_usage_rate,
        a.null_rate,
        a.duplicate_msisdn_rate,
        a.id_noise_rate,
        a.outlier_usage_rate,
        a.parquet,
    )


def months_between(start: date, end: date) -> list[date]:
    out = []
    cursor = date(start.year, start.month, 1)
    stop = date(end.year, end.month, 1)
    while cursor <= stop:
        out.append(cursor)
        cursor = date(cursor.year + 1, 1, 1) if cursor.month == 12 else date(cursor.year, cursor.month + 1, 1)
    return out


def rand_date(r: random.Random, start: date, end: date) -> date:
    return start + timedelta(days=r.randint(0, max((end - start).days, 0)))


def rand_ts_month(r: random.Random, month: date) -> datetime:
    nxt = date(month.year + 1, 1, 1) if month.month == 12 else date(month.year, month.month + 1, 1)
    return datetime.combine(month, datetime.min.time()) + timedelta(seconds=r.randint(0, int((nxt - month).total_seconds()) - 1))


def write_csv(path: Path, rows: list[dict], cols: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def maybe_write_parquet(path: Path, rows: list[dict]) -> str | None:
    try:
        import pandas as pd  # type: ignore

        pd.DataFrame(rows).to_parquet(path, index=False)
        return None
    except Exception as exc:
        return str(exc)


def null_ratio(rows: list[dict], col: str) -> float:
    return round(sum(1 for r in rows if r.get(col) in (None, "")) / len(rows), 5) if rows else 0.0


def dup_ratio(rows: list[dict], col: str) -> float:
    vals = [r.get(col) for r in rows if r.get(col) not in (None, "")]
    return round((len(vals) - len(set(vals))) / len(vals), 5) if vals else 0.0


def orphan_ratio(rows: list[dict], col: str, parent_values: set[str]) -> float:
    return round(sum(1 for r in rows if str(r.get(col, "")).strip().upper() not in parent_values) / len(rows), 5) if rows else 0.0


def numeric_stats(rows: list[dict], col: str) -> dict[str, float]:
    vals = [float(r[col]) for r in rows if r.get(col) not in (None, "")]
    if not vals:
        return {"mean": 0.0, "median": 0.0, "p95": 0.0, "max": 0.0}
    sorted_vals = sorted(vals)
    idx = min(len(sorted_vals) - 1, int(0.95 * len(sorted_vals)))
    return {
        "mean": round(statistics.fmean(vals), 4),
        "median": round(statistics.median(vals), 4),
        "p95": round(sorted_vals[idx], 4),
        "max": round(sorted_vals[-1], 4),
    }


def log_step(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def main() -> None:
    cfg = parse_args()
    log_step("Start synthetic data generation")
    log_step(f"Config: customers={cfg.customers}, events_per_subscriber_per_month={cfg.events_per_subscriber_per_month}, parquet={cfg.parquet}")
    log_step(f"Usage cap: max_usage_events={cfg.max_usage_events}")
    log_step(f"Date range: {cfg.start_date} -> {cfg.end_date}")

    r = random.Random(cfg.seed)
    start = date.fromisoformat(cfg.start_date)
    end = date.fromisoformat(cfg.end_date)
    if end < start:
        raise ValueError("end-date must be >= start-date")

    root = Path(cfg.output_root)
    if not root.is_absolute():
        root = PROJECT_ROOT / root
    root = root.resolve()
    raw = root / "raw"
    meta = root / "metadata"
    rep = root / "reports"
    raw.mkdir(parents=True, exist_ok=True)
    meta.mkdir(parents=True, exist_ok=True)
    rep.mkdir(parents=True, exist_ok=True)
    log_step(f"Output directories ready: raw={raw}, metadata={meta}, reports={rep}")

    regions = ["NORTH", "SOUTH", "EAST", "WEST", "CENTER", "METRO"]
    channels = ["online", "retail_store", "call_center", "partner"]
    month_list = months_between(start, end)

    cid = lambda i: f"C{i:07d}"
    sid = lambda i: f"S{i:08d}"
    iid = lambda i: f"I{i:09d}"
    pid = lambda i: f"PM{i:09d}"
    eid = lambda i: f"E{i:011d}"
    msisdn = lambda: "33" + "".join(str(r.randint(0, 9)) for _ in range(9))
    maybe = lambda value, p: None if r.random() < p else value

    log_step("Generating plans...")
    plans = []
    for p in PLANS:
        plans.append(
            {
                **p,
                "voice_quota_min": 120 if p["plan_id"] == "P_BASIC_10" else (300 if p["plan_id"] == "P_PLUS_40" else (1200 if p["plan_id"] == "P_PREMIUM_120" else 2000)),
                "sms_quota": 100 if p["plan_id"] == "P_BASIC_10" else (300 if p["plan_id"] == "P_PLUS_40" else (1000 if p["plan_id"] == "P_PREMIUM_120" else 5000)),
                "currency": "EUR",
            }
        )

    log_step("Generating customers...")
    customers = []
    for i in range(1, cfg.customers + 1):
        segment = r.choices(["B2C", "SME", "ENTERPRISE"], weights=[0.78, 0.16, 0.06], k=1)[0]
        customers.append(
            {
                "customer_id": cid(i),
                "cust_id": f"CUST-{i:06d}",
                "customer_name": f"customer_{i:06d}",
                "segment": segment,
                "region": r.choice(regions),
                "status": "active" if r.random() < 0.88 else r.choice(["suspended", "churned"]),
                "created_at": rand_date(r, start - timedelta(days=730), end - timedelta(days=30)).isoformat(),
                "channel": r.choice(channels),
                "risk_score": round(max(0.0, min(1.0, r.gauss(0.35, 0.22))), 4),
                "revenue_tier": "high" if segment in ("SME", "ENTERPRISE") and r.random() < 0.45 else "standard",
                "contact_email": maybe(f"customer_{i:06d}@example.test", cfg.null_rate),
                "contact_phone": maybe("+" + msisdn(), cfg.null_rate),
            }
        )

    log_step("Generating subscribers...")
    subscribers = []
    s_counter = 1
    for c in customers:
        seg = c["segment"]
        n_subs = min(cfg.max_subscribers, 1 + (r.randint(0, 2) if seg == "B2C" else (r.randint(1, 3) if seg == "SME" else r.randint(3, 6))))
        for _ in range(n_subs):
            plan_id = r.choices(
                ["P_BASIC_10", "P_PLUS_40", "P_PREMIUM_120", "P_BIZ_200"],
                weights=[0.28, 0.42, 0.2, 0.1] if seg == "B2C" else ([0.05, 0.30, 0.40, 0.25] if seg == "SME" else [0.0, 0.08, 0.35, 0.57]),
                k=1,
            )[0]
            status = r.choices(["active", "suspended", "churned"], weights=[0.82, 0.08, 0.10], k=1)[0]
            activation = rand_date(r, date.fromisoformat(c["created_at"]), end).isoformat()
            churn = rand_date(r, date.fromisoformat(activation), end).isoformat() if status == "churned" else None
            client_ref = c["customer_id"]
            if r.random() < cfg.id_noise_rate:
                client_ref = r.choice([client_ref.lower(), client_ref.replace("C", ""), f" {client_ref} "])
            subscribers.append(
                {
                    "subscriber_id": sid(s_counter),
                    "subscriber_key": f"SUB-{s_counter:08d}",
                    "client_id": client_ref,
                    "subscriber_id_alt": maybe(f"SUBSCR-{s_counter:07d}", cfg.null_rate / 2),
                    "msisdn": msisdn(),
                    "imsi": maybe("".join(str(r.randint(0, 9)) for _ in range(15)), cfg.null_rate / 2),
                    "plan_id": plan_id,
                    "status": status,
                    "activation_date": activation,
                    "churn_date": churn,
                    "region": c["region"],
                }
            )
            s_counter += 1

    log_step("Injecting subscriber data quality issues (orphans, duplicate msisdn)...")
    for i in r.sample(range(len(subscribers)), k=int(len(subscribers) * cfg.orphan_subscriber_rate)):
        subscribers[i]["client_id"] = f"C{r.randint(9999990, 9999999)}"
    k_dup = int(len(subscribers) * cfg.duplicate_msisdn_rate)
    for src, dst in zip(r.sample(range(len(subscribers)), k=k_dup), r.sample(range(len(subscribers)), k=k_dup)):
        subscribers[dst]["msisdn"] = subscribers[src]["msisdn"]

    log_step(f"Generated customers: {len(customers)}")
    log_step(f"Generated subscribers: {len(subscribers)}")

    plan_map = {p["plan_id"]: p for p in plans}
    log_step("Generating usage_events (this is usually the longest step)...")
    usage_events = []
    e_counter = 1
    usage_cap_reached = False
    for sub in subscribers:
        if len(usage_events) >= cfg.max_usage_events:
            usage_cap_reached = True
            break
        activation = date.fromisoformat(sub["activation_date"])
        churn = date.fromisoformat(sub["churn_date"]) if sub["churn_date"] else end
        family = plan_map[sub["plan_id"]]["plan_family"]
        for month in [m for m in month_list if activation <= m <= churn]:
            if len(usage_events) >= cfg.max_usage_events:
                usage_cap_reached = True
                break
            seasonality = 1.22 if month.month in (7, 8, 12) else (0.90 if month.month in (1, 2) else (1.08 if month.month in (5, 6, 9, 10) else 1.0))
            event_count = max(1, int(r.gauss(cfg.events_per_subscriber_per_month * seasonality * (1.12 if family == "B2B" else 1.0), 6)))
            for _ in range(event_count):
                if len(usage_events) >= cfg.max_usage_events:
                    usage_cap_reached = True
                    break
                usage_events.append(
                    {
                        "event_id": eid(e_counter),
                        "subscriber_id": sub["subscriber_id"],
                        "event_ts": rand_ts_month(r, month).isoformat(timespec="seconds"),
                        "event_type": r.choices(["data", "voice", "sms", "mixed"], weights=[0.48, 0.19, 0.12, 0.21], k=1)[0],
                        "data_mb": round(max(0.1, r.lognormvariate(math.log(max(plan_map[sub["plan_id"]]["data_quota_gb"], 1)), 0.9)), 3),
                        "voice_min": round(max(0.0, r.gammavariate(2.2, 1.9)), 2),
                        "sms_count": int(max(0, r.gammavariate(1.7, 2.1))),
                        "cell_id": f"CELL_{r.randint(1, 850):04d}",
                        "roaming_flag": 1 if r.random() < 0.08 else 0,
                    }
                )
                e_counter += 1
            if usage_cap_reached:
                break
        if usage_cap_reached:
            break

    log_step("Injecting usage anomalies (orphans, outliers, nulls)...")
    for i in r.sample(range(len(usage_events)), k=int(len(usage_events) * cfg.orphan_usage_rate)):
        usage_events[i]["subscriber_id"] = sid(r.randint(99999990, 99999999))
    for i in r.sample(range(len(usage_events)), k=int(len(usage_events) * cfg.outlier_usage_rate)):
        usage_events[i]["data_mb"] = round(float(usage_events[i]["data_mb"]) * r.uniform(35.0, 180.0), 3)
    for i in r.sample(range(len(usage_events)), k=max(1, len(usage_events) // 5000)):
        usage_events[i]["cell_id"] = None
    for i in r.sample(range(len(usage_events)), k=max(1, len(usage_events) // 9000)):
        usage_events[i]["event_type"] = "unknown_type"

    log_step(f"Generated usage_events: {len(usage_events)}")
    log_step("Aggregating usage by customer/month...")
    customer_map = {c["customer_id"]: c for c in customers}
    customer_subscribers = {}
    for sub in subscribers:
        clean_ref = str(sub["client_id"]).strip().upper()
        if clean_ref in customer_map:
            customer_subscribers.setdefault(clean_ref, []).append(sub)

    sub_to_customer = {sub["subscriber_id"]: cust for cust, subs in customer_subscribers.items() for sub in subs}
    usage_agg = {}
    for e in usage_events:
        cust = sub_to_customer.get(e["subscriber_id"])
        if not cust:
            continue
        key = (cust, e["event_ts"][:7])
        bucket = usage_agg.setdefault(key, {"data_mb": 0.0, "voice_min": 0.0, "sms_count": 0.0})
        bucket["data_mb"] += float(e["data_mb"])
        bucket["voice_min"] += float(e["voice_min"])
        bucket["sms_count"] += float(e["sms_count"])

    log_step("Generating invoices and payments...")
    invoices, payments = [], []
    i_counter, p_counter = 1, 1
    for c in customers:
        subs = customer_subscribers.get(c["customer_id"], [])
        if not subs:
            continue
        for month in month_list:
            active = [s for s in subs if s["activation_date"] <= month.isoformat() and (s["churn_date"] is None or s["churn_date"] >= month.isoformat())]
            if not active:
                continue
            fixed_fee = sum(float(plan_map[s["plan_id"]]["monthly_fee"]) for s in active)
            usage = usage_agg.get((c["customer_id"], f"{month.year:04d}-{month.month:02d}"), {"data_mb": 0.0, "voice_min": 0.0, "sms_count": 0.0})
            quota = sum(float(plan_map[s["plan_id"]]["data_quota_gb"]) for s in active)
            amount = round(max(0.0, fixed_fee + max(0.0, usage["data_mb"] / 1024.0 - quota) * 1.5 + usage["voice_min"] * 0.01 + usage["sms_count"] * 0.002), 2)
            due = date(month.year, month.month, 1) + timedelta(days=27)
            status = r.choices(["issued", "paid", "partially_paid", "overdue"], weights=[0.2, 0.62, 0.08, 0.10], k=1)[0]
            invoices.append(
                {
                    "invoice_id": iid(i_counter),
                    "customer_id": c["customer_id"],
                    "billing_month": f"{month.year:04d}-{month.month:02d}",
                    "issued_at": date(month.year, month.month, 2).isoformat(),
                    "due_date": due.isoformat(),
                    "amount": amount,
                    "currency": "EUR",
                    "invoice_status": status,
                    "tax_rate": 0.2,
                    "amount_with_tax": round(amount * 1.2, 2),
                }
            )
            pay_prob = 0.95 if c["segment"] == "ENTERPRISE" else (0.90 if c["segment"] == "SME" else 0.84)
            if status in ("paid", "partially_paid", "overdue") and r.random() < pay_prob:
                paid = amount if status != "partially_paid" else round(amount * r.uniform(0.35, 0.9), 2)
                payments.append(
                    {
                        "payment_id": pid(p_counter),
                        "invoice_id": iid(i_counter),
                        "customer_ref": c["customer_id"] if r.random() > 0.03 else c["customer_id"].replace("C", "CUS-"),
                        "paid_at": (due + timedelta(days=int(max(0, r.gauss(12, 9))))).isoformat(),
                        "paid_amount": paid,
                        "payment_method": r.choice(["card", "transfer", "cash", "direct_debit"]),
                        "payment_status": "ok" if paid >= amount * 0.99 else "partial",
                    }
                )
                p_counter += 1
            i_counter += 1

    if payments:
        for k in r.sample(range(len(payments)), k=max(1, len(payments) // 500)):
            payments[k]["invoice_id"] = iid(r.randint(999000000, 999999999))

    log_step(f"Generated invoices: {len(invoices)}")
    log_step(f"Generated payments: {len(payments)}")

    schema = {
        "plans": ["plan_id", "plan_name", "plan_family", "monthly_fee", "data_quota_gb", "voice_quota_min", "sms_quota", "currency"],
        "customers": ["customer_id", "cust_id", "customer_name", "segment", "region", "status", "created_at", "channel", "risk_score", "revenue_tier", "contact_email", "contact_phone"],
        "subscribers": ["subscriber_id", "subscriber_key", "client_id", "subscriber_id_alt", "msisdn", "imsi", "plan_id", "status", "activation_date", "churn_date", "region"],
        "usage_events": ["event_id", "subscriber_id", "event_ts", "event_type", "data_mb", "voice_min", "sms_count", "cell_id", "roaming_flag"],
        "invoices": ["invoice_id", "customer_id", "billing_month", "issued_at", "due_date", "amount", "currency", "invoice_status", "tax_rate", "amount_with_tax"],
        "payments": ["payment_id", "invoice_id", "customer_ref", "paid_at", "paid_amount", "payment_method", "payment_status"],
    }
    tables = {
        "plans": plans,
        "customers": customers,
        "subscribers": subscribers,
        "usage_events": usage_events,
        "invoices": invoices,
        "payments": payments,
    }

    log_step("Writing CSV/Parquet files...")
    parquet_notes = []
    for name, rows in tables.items():
        log_step(f"Writing CSV: {name}.csv ({len(rows)} rows)")
        write_csv(raw / f"{name}.csv", rows, schema[name])
        if cfg.parquet:
            log_step(f"Writing Parquet: {name}.parquet")
            msg = maybe_write_parquet(raw / f"{name}.parquet", rows)
            if msg:
                parquet_notes.append(f"{name}: {msg}")

    log_step("Writing metadata files...")
    table_catalog = {
        "customers": {"pk": "customer_id"},
        "subscribers": {"pk": "subscriber_id", "potential_fk": "client_id -> customers.customer_id (not strict)"},
        "plans": {"pk": "plan_id"},
        "usage_events": {"pk": "event_id", "potential_fk": "subscriber_id -> subscribers.subscriber_id (not strict)"},
        "invoices": {"pk": "invoice_id"},
        "payments": {"pk": "payment_id", "potential_fk": "invoice_id -> invoices.invoice_id (not strict)"},
    }
    synonyms = {
        "customer_identifiers": ["customer_id", "client_id", "cust_id", "customer_ref"],
        "subscriber_identifiers": ["subscriber_id", "subscriber_id_alt", "subscriber_key", "msisdn", "imsi"],
        "billing_time": ["billing_month", "issued_at", "due_date", "paid_at"],
    }

    (meta / "table_catalog.json").write_text(json.dumps(table_catalog, indent=2), encoding="utf-8")
    (meta / "column_synonyms.json").write_text(json.dumps(synonyms, indent=2), encoding="utf-8")
    (meta / "business_rules.md").write_text(
        "# Business Rules\n\n- Seasonality on usage.\n- Segment behavior differs.\n- Noisy identifiers and orphan rows are intentional.\n- Partial/missing data is intentional.\n",
        encoding="utf-8",
    )
    (meta / "query_library.sql").write_text(
        "SELECT customer_id, SUM(amount) total_amount FROM invoices GROUP BY 1 QUALIFY ROW_NUMBER() OVER (ORDER BY total_amount DESC) <= 10;\n"
        "SELECT billing_month, SUM(amount) revenue FROM invoices GROUP BY 1 ORDER BY 1;\n"
        "SELECT s.region, SUM(u.data_mb) total_data_mb FROM usage_events u JOIN subscribers s ON u.subscriber_id=s.subscriber_id GROUP BY 1 ORDER BY 2 DESC;\n",
        encoding="utf-8",
    )
    (meta / "data_dictionary.md").write_text(
        "# Data Dictionary\n\n- customers: master customer data.\n- subscribers: lines with noisy client_id.\n- plans: commercial catalog.\n- usage_events: event-level facts.\n- invoices: monthly billing.\n- payments: payment transactions.\n",
        encoding="utf-8",
    )

    log_step("Computing quality report...")
    quality = {
        "row_counts": {k: len(v) for k, v in tables.items()},
        "null_ratios": {
            "customers.contact_email": null_ratio(customers, "contact_email"),
            "customers.contact_phone": null_ratio(customers, "contact_phone"),
            "subscribers.imsi": null_ratio(subscribers, "imsi"),
            "usage_events.cell_id": null_ratio(usage_events, "cell_id"),
        },
        "duplicate_ratios": {"subscribers.msisdn": dup_ratio(subscribers, "msisdn")},
        "orphan_ratios": {
            "subscribers.client_id_vs_customers.customer_id": orphan_ratio(subscribers, "client_id", {c["customer_id"] for c in customers}),
            "usage_events.subscriber_id_vs_subscribers.subscriber_id": orphan_ratio(usage_events, "subscriber_id", {s["subscriber_id"] for s in subscribers}),
            "payments.invoice_id_vs_invoices.invoice_id": orphan_ratio(payments, "invoice_id", {x["invoice_id"] for x in invoices}),
        },
        "numeric_profiles": {
            "usage_events.data_mb": numeric_stats(usage_events, "data_mb"),
            "usage_events.voice_min": numeric_stats(usage_events, "voice_min"),
            "invoices.amount": numeric_stats(invoices, "amount"),
            "payments.paid_amount": numeric_stats(payments, "paid_amount"),
        },
    }
    (rep / "quality_report.json").write_text(json.dumps(quality, indent=2), encoding="utf-8")

    log_step("Writing generation summary...")
    summary = {
        "config": vars(cfg),
        "row_counts": quality["row_counts"],
        "parquet_notes": parquet_notes,
        "paths": {"raw": str(raw.resolve()), "metadata": str(meta.resolve()), "reports": str(rep.resolve())},
    }
    (rep / "generation_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    log_step("Generation completed successfully")
    print("Synthetic telecom data generated.")
    print(json.dumps(summary["row_counts"]))


if __name__ == "__main__":
    main()







