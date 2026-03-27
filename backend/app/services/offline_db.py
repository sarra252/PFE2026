from pathlib import Path

import duckdb


class OfflineExecutionError(ValueError):
    pass


def _normalize_sql(sql: str) -> str:
    cleaned = sql.strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    return cleaned


def _resolve_table_files(data_dir: Path) -> tuple[dict[str, Path], str]:
    parquet_files = sorted(data_dir.glob("*.parquet"))
    if parquet_files:
        return {f.stem: f for f in parquet_files}, "parquet"

    csv_files = sorted(data_dir.glob("*.csv"))
    if csv_files:
        return {f.stem: f for f in csv_files}, "csv"

    raise OfflineExecutionError(
        f"Aucune table trouvee dans {data_dir}. Genere d'abord des fichiers .parquet ou .csv."
    )


def run_readonly_sql_offline(sql: str, data_dir: str, row_limit: int = 100) -> dict:
    if row_limit <= 0:
        raise OfflineExecutionError("row_limit doit etre > 0")

    raw_sql = _normalize_sql(sql)
    if not raw_sql:
        raise OfflineExecutionError("SQL vide")

    base_dir = Path(data_dir)
    if not base_dir.exists() or not base_dir.is_dir():
        raise OfflineExecutionError(f"Repertoire introuvable: {base_dir}")

    tables, source_format = _resolve_table_files(base_dir)

    conn = duckdb.connect(database=":memory:")
    try:
        for table_name, file_path in tables.items():
            normalized = file_path.as_posix().replace("'", "''")
            if source_format == "parquet":
                conn.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{normalized}')")
            else:
                conn.execute(
                    f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_csv_auto('{normalized}', header=true)"
                )

        limited_sql = f"SELECT * FROM ({raw_sql}) AS _q LIMIT {int(row_limit)}"
        cursor = conn.execute(limited_sql)
        rows = cursor.fetchall()
        columns = [d[0] for d in cursor.description]

        return {
            "columns": columns,
            "rows": [list(r) for r in rows],
            "row_count": len(rows),
            "row_limit": row_limit,
            "source_format": source_format,
            "tables_loaded": sorted(tables.keys()),
        }
    except duckdb.Error as exc:
        raise OfflineExecutionError(f"Execution SQL offline impossible: {exc}") from exc
    finally:
        conn.close()
