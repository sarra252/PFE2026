import re

import sqlparse

BLOCKED_KEYWORDS = {"DROP", "DELETE", "UPDATE", "INSERT", "MERGE", "ALTER", "TRUNCATE", "CREATE"}


class SqlValidationError(ValueError):
    pass


def is_plausible_sql(sql: str) -> bool:
    parsed = sqlparse.parse(sql)
    return len(parsed) > 0 and bool(parsed[0].tokens)


def starts_with_select_or_with(sql: str) -> bool:
    cleaned = sql.strip().upper()
    return cleaned.startswith("SELECT") or cleaned.startswith("WITH")


def has_blocked_keywords(sql: str) -> list[str]:
    hits = []
    uppercase_sql = sql.upper()
    for keyword in BLOCKED_KEYWORDS:
        pattern = rf"\b{re.escape(keyword)}\b"
        if re.search(pattern, uppercase_sql):
            hits.append(keyword)
    return sorted(hits)


def has_multiple_statements(sql: str) -> bool:
    statements = [s.strip() for s in sqlparse.split(sql) if s.strip()]
    return len(statements) > 1


def validate_sql_readonly(sql: str) -> list[str]:
    if not is_plausible_sql(sql):
        raise SqlValidationError("SQL invalide ou vide.")

    if has_multiple_statements(sql):
        raise SqlValidationError("Les requetes multiples ne sont pas autorisees.")

    if not starts_with_select_or_with(sql):
        raise SqlValidationError("Seules les requetes de lecture (SELECT/WITH) sont autorisees.")

    blocked = has_blocked_keywords(sql)
    if blocked:
        raise SqlValidationError(f"Mots-cles interdits detectes: {', '.join(blocked)}")

    warnings = []
    if "SELECT *" in sql.upper():
        warnings.append("Eviter SELECT * pour de meilleures performances et une meilleure stabilite.")
    return warnings