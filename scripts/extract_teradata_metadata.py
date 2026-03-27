#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data_synth" / "metadata"
SYSTEM_DATABASES = {"DBC", "Sys_Calendar", "TD_SYSFNLIB", "SQLJ", "All"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract Teradata metadata into files used by the local RAG retriever."
    )
    parser.add_argument("--host", default=os.getenv("TERADATA_HOST", ""))
    parser.add_argument("--user", default=os.getenv("TERADATA_USER", ""))
    parser.add_argument("--password", default=os.getenv("TERADATA_PASSWORD", ""))
    parser.add_argument("--database", default=os.getenv("TERADATA_DATABASE", ""))
    parser.add_argument(
        "--databases",
        default="",
        help="Comma-separated list of databases to include. If empty, uses --database when provided.",
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Target folder for table_catalog.json, column_synonyms.json, business_rules.md, query_library.sql",
    )
    parser.add_argument(
        "--max-tables",
        type=int,
        default=300,
        help="Maximum number of tables to export.",
    )
    parser.add_argument(
        "--table-kinds",
        default="T,V",
        help="Comma-separated TableKind values from DBC.TablesV (example: T,V).",
    )
    parser.add_argument(
        "--include-system-db",
        action="store_true",
        help="Include system databases such as DBC and Sys_Calendar.",
    )
    return parser.parse_args()


def require_runtime_inputs(args: argparse.Namespace) -> None:
    missing = []
    if not args.host:
        missing.append("TERADATA_HOST / --host")
    if not args.user:
        missing.append("TERADATA_USER / --user")
    if not args.password:
        missing.append("TERADATA_PASSWORD / --password")
    if missing:
        raise ValueError("Missing required connection settings: " + ", ".join(missing))


def get_connection(args: argparse.Namespace):
    try:
        import teradatasql  # type: ignore
    except Exception as exc:
        raise RuntimeError("teradatasql is not installed. Run: pip install teradatasql") from exc

    kwargs = {
        "host": args.host,
        "user": args.user,
        "password": args.password,
    }
    if args.database:
        kwargs["database"] = args.database
    return teradatasql.connect(**kwargs)


def normalize_databases(databases_csv: str, fallback_database: str) -> list[str]:
    if databases_csv.strip():
        return [x.strip() for x in databases_csv.split(",") if x.strip()]
    if fallback_database.strip():
        return [fallback_database.strip()]
    return []


def build_where_clause(databases: list[str], include_system_db: bool) -> tuple[str, list[Any]]:
    where = ["1=1"]
    params: list[Any] = []
    if databases:
        placeholders = ",".join("?" for _ in databases)
        where.append(f"DatabaseName IN ({placeholders})")
        params.extend(databases)
    elif not include_system_db:
        banned = sorted(SYSTEM_DATABASES)
        placeholders = ",".join("?" for _ in banned)
        where.append(f"DatabaseName NOT IN ({placeholders})")
        params.extend(banned)
    return " AND ".join(where), params


def normalize_table_kinds(table_kinds_csv: str) -> list[str]:
    raw = [x.strip().upper() for x in table_kinds_csv.split(",") if x.strip()]
    return raw or ["T", "V"]


def fetch_tables(
    cursor,
    where_clause: str,
    params: list[Any],
    max_tables: int,
    table_kinds: list[str],
) -> list[dict[str, str]]:
    kind_placeholders = ",".join("?" for _ in table_kinds)
    sql = f"""
        SELECT DatabaseName, TableName, TableKind
        FROM DBC.TablesV
        WHERE TableKind IN ({kind_placeholders}) AND {where_clause}
        ORDER BY DatabaseName, TableName
    """
    cursor.execute(sql, [*table_kinds, *params])
    rows = cursor.fetchall()
    out = [{"database": r[0], "table": r[1], "kind": r[2]} for r in rows]
    return out[:max_tables]


def fetch_candidate_databases(cursor, include_system_db: bool) -> list[str]:
    if include_system_db:
        cursor.execute("SELECT DISTINCT DatabaseName FROM DBC.TablesV ORDER BY 1")
    else:
        banned = sorted(SYSTEM_DATABASES)
        placeholders = ",".join("?" for _ in banned)
        cursor.execute(
            f"SELECT DISTINCT DatabaseName FROM DBC.TablesV WHERE DatabaseName NOT IN ({placeholders}) ORDER BY 1",
            banned,
        )
    return [str(r[0]) for r in cursor.fetchall()]


def fetch_columns(cursor, tables: list[dict[str, str]]) -> dict[tuple[str, str], list[dict[str, str]]]:
    by_table: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for t in tables:
        cursor.execute(
            """
            SELECT ColumnName, ColumnType
            FROM DBC.ColumnsV
            WHERE DatabaseName = ? AND TableName = ?
            ORDER BY ColumnId
            """,
            [t["database"], t["table"]],
        )
        for col_name, col_type in cursor.fetchall():
            by_table[(t["database"], t["table"])].append(
                {"name": str(col_name), "type": str(col_type)}
            )
    return by_table


def singularize(name: str) -> str:
    low = name.lower()
    if low.endswith("ies") and len(low) > 3:
        return low[:-3] + "y"
    if low.endswith("s") and len(low) > 1:
        return low[:-1]
    return low


def infer_primary_key(table_name: str, columns: list[dict[str, str]]) -> str:
    col_names = [c["name"] for c in columns]
    col_set = {c.lower() for c in col_names}
    canonical = f"{singularize(table_name)}_id"
    if canonical in col_set:
        for c in col_names:
            if c.lower() == canonical:
                return c
    for c in col_names:
        if c.lower().endswith("_id"):
            return c
    return col_names[0] if col_names else "id"


def infer_fk_candidates(
    table_keys: list[tuple[str, str]],
    columns_by_table: dict[tuple[str, str], list[dict[str, str]]],
    pk_by_table: dict[tuple[str, str], str],
) -> dict[tuple[str, str], str]:
    fk_map: dict[tuple[str, str], str] = {}
    pk_lookup = {}
    for (db, tbl), pk in pk_by_table.items():
        pk_lookup.setdefault(pk.lower(), []).append((db, tbl, pk))

    for key in table_keys:
        cols = columns_by_table.get(key, [])
        this_pk = pk_by_table.get(key, "")
        for c in cols:
            col_name = c["name"]
            low = col_name.lower()
            if low == this_pk.lower():
                continue
            if not low.endswith("_id"):
                continue
            matches = pk_lookup.get(low, [])
            if not matches:
                continue
            ref_db, ref_tbl, ref_pk = matches[0]
            if (ref_db, ref_tbl) == key:
                continue
            fk_map[key] = f"{col_name} -> {ref_db}.{ref_tbl}.{ref_pk} (inferred)"
            break
    return fk_map


def build_table_catalog(
    tables: list[dict[str, str]],
    columns_by_table: dict[tuple[str, str], list[dict[str, str]]],
) -> dict[str, dict[str, Any]]:
    table_keys = [(t["database"], t["table"]) for t in tables]
    pk_by_table: dict[tuple[str, str], str] = {}
    for key in table_keys:
        _, table_name = key
        pk_by_table[key] = infer_primary_key(table_name, columns_by_table.get(key, []))

    fk_by_table = infer_fk_candidates(table_keys, columns_by_table, pk_by_table)

    catalog: dict[str, dict[str, Any]] = {}
    kind_by_table = {(t["database"], t["table"]): t.get("kind", "T") for t in tables}
    for db, table in table_keys:
        key_name = f"{db}.{table}"
        cols = columns_by_table.get((db, table), [])
        entry: dict[str, Any] = {
            "database": db,
            "table": table,
            "object_kind": kind_by_table.get((db, table), "T"),
            "pk": pk_by_table[(db, table)],
            "columns": [c["name"] for c in cols],
            "column_types": {c["name"]: c["type"] for c in cols},
        }
        fk = fk_by_table.get((db, table))
        if fk:
            entry["potential_fk"] = fk
        catalog[key_name] = entry
    return catalog


def build_synonyms(columns_by_table: dict[tuple[str, str], list[dict[str, str]]]) -> dict[str, list[str]]:
    all_columns = {
        c["name"].lower()
        for cols in columns_by_table.values()
        for c in cols
    }

    groups = {
        "customer_identifiers": ["customer_id", "client_id", "cust_id", "customer_ref"],
        "subscriber_identifiers": ["subscriber_id", "msisdn", "imsi", "line_id"],
        "billing_time": ["billing_month", "invoice_date", "issued_at", "due_date", "paid_at"],
    }

    out: dict[str, list[str]] = {}
    for group, candidates in groups.items():
        present = [c for c in candidates if c in all_columns]
        if present:
            out[group] = present

    return out


def write_metadata_files(
    output_dir: Path,
    table_catalog: dict[str, dict[str, Any]],
    column_synonyms: dict[str, list[str]],
    selected_databases: list[str],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "table_catalog.json").write_text(
        json.dumps(table_catalog, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    (output_dir / "column_synonyms.json").write_text(
        json.dumps(column_synonyms, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )

    db_text = ", ".join(selected_databases) if selected_databases else "ALL_NON_SYSTEM_DATABASES"
    business_rules = (
        "# Business Rules\n\n"
        "- Metadata extracted from Teradata hosted environment.\n"
        "- Potential foreign keys are inferred (not strict constraints).\n"
        "- Review joins manually for critical workloads.\n"
        f"- Extraction scope: {db_text}.\n"
    )
    (output_dir / "business_rules.md").write_text(business_rules, encoding="utf-8")

    query_library_path = output_dir / "query_library.sql"
    if not query_library_path.exists():
        starter_sql = (
            "-- Seed query library for RAG examples\n"
            "-- Add validated business queries here.\n"
            "SELECT 1 AS smoke_test;\n"
        )
        query_library_path.write_text(starter_sql, encoding="utf-8")


def main() -> None:
    args = parse_args()
    require_runtime_inputs(args)

    selected_databases = normalize_databases(args.databases, args.database)
    table_kinds = normalize_table_kinds(args.table_kinds)
    where_clause, params = build_where_clause(
        databases=selected_databases,
        include_system_db=args.include_system_db,
    )

    conn = get_connection(args)
    cursor = None
    try:
        cursor = conn.cursor()
        tables = fetch_tables(cursor, where_clause, params, args.max_tables, table_kinds)
        if not tables:
            db_hint = fetch_candidate_databases(cursor, args.include_system_db)
            preview = ", ".join(db_hint[:12]) if db_hint else "none"
            raise RuntimeError(
                "No table/view found with provided filters. "
                f"Check database name/case or use --databases. Available databases preview: {preview}"
            )

        columns_by_table = fetch_columns(cursor, tables)
        table_catalog = build_table_catalog(tables, columns_by_table)
        column_synonyms = build_synonyms(columns_by_table)
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass

    out_dir = Path(args.output_dir)
    if not out_dir.is_absolute():
        out_dir = (PROJECT_ROOT / out_dir).resolve()
    write_metadata_files(out_dir, table_catalog, column_synonyms, selected_databases)

    print(f"Metadata export completed to: {out_dir}")
    print(f"- tables exported: {len(table_catalog)}")
    print(f"- synonym groups: {len(column_synonyms)}")


if __name__ == "__main__":
    main()
