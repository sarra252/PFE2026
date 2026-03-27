import re


TABLE_PROJECTIONS = {
    "customers": "customer_id, customer_name, segment, region",
    "subscribers": "subscriber_id, client_id, msisdn, status",
    "invoices": "invoice_id, customer_id, billing_month, amount",
    "payments": "payment_id, invoice_id, paid_amount, paid_at",
    "usage_events": "event_id, subscriber_id, event_ts, data_mb",
    "plans": "plan_id, plan_name, monthly_fee, data_quota_gb",
}


def _detect_primary_table(sql: str) -> str | None:
    match = re.search(r"\bFROM\s+([A-Za-z0-9_\.]+)", sql, flags=re.IGNORECASE)
    if not match:
        return None
    table = match.group(1).split(".")[-1].strip().lower()
    return table


def optimize_sql(sql: str) -> dict:
    optimized_sql = sql.strip()
    rationale = []
    cautions = []

    if "SELECT *" in optimized_sql.upper():
        table = _detect_primary_table(optimized_sql)
        projection = TABLE_PROJECTIONS.get(table or "")
        if projection:
            optimized_sql = re.sub(r"SELECT\s+\*", f"SELECT {projection}", optimized_sql, count=1, flags=re.IGNORECASE)
            rationale.append("Remplace SELECT * par une projection explicite adaptee a la table pour reduire I/O.")
        else:
            cautions.append("SELECT * detecte mais table non reconnue: definir une projection explicite manuellement.")

    if "ORDER BY" not in optimized_sql.upper():
        cautions.append("Ajouter ORDER BY uniquement si necessaire, car il peut etre couteux.")

    if "QUALIFY" not in optimized_sql.upper() and "ROW_NUMBER" in optimized_sql.upper():
        rationale.append("Utiliser QUALIFY pour filtrer les fonctions de fenetre en Teradata.")

    if "UPPER(TRIM(" in optimized_sql.upper():
        cautions.append("La normalisation d'identifiants en join peut couter cher: envisager un champ canonicalise pre-calcule.")

    rationale.append("Verifier les statistiques Teradata et l'indexation des colonnes de jointure.")

    return {
        "optimized_sql": optimized_sql,
        "rationale": rationale,
        "cautions": cautions,
    }
