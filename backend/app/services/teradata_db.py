from dataclasses import dataclass


class TeradataConfigError(ValueError):
    pass


class TeradataExecutionError(ValueError):
    pass


@dataclass(frozen=True)
class TeradataConnectionConfig:
    host: str
    user: str
    password: str
    database: str = ""


def _normalize_sql(sql: str) -> str:
    cleaned = sql.strip()
    if cleaned.endswith(";"):
        cleaned = cleaned[:-1].strip()
    return cleaned


def teradata_config_from_settings(settings_obj) -> TeradataConnectionConfig:
    host = (settings_obj.teradata_host or "").strip()
    user = (settings_obj.teradata_user or "").strip()
    password = settings_obj.teradata_password or ""
    database = (settings_obj.teradata_database or "").strip()

    missing = []
    if not host:
        missing.append("TERADATA_HOST")
    if not user:
        missing.append("TERADATA_USER")
    if not password:
        missing.append("TERADATA_PASSWORD")
    if missing:
        raise TeradataConfigError(f"Configuration Teradata incomplete: {', '.join(missing)}")

    return TeradataConnectionConfig(
        host=host,
        user=user,
        password=password,
        database=database,
    )


def _import_teradata_driver():
    try:
        import teradatasql  # type: ignore
    except Exception as exc:
        raise TeradataExecutionError(
            "Driver teradatasql introuvable. Installez-le avec: pip install teradatasql"
        ) from exc
    return teradatasql


def run_readonly_sql_teradata(sql: str, config: TeradataConnectionConfig, row_limit: int = 100) -> dict:
    if row_limit <= 0:
        raise TeradataExecutionError("row_limit doit etre > 0")

    raw_sql = _normalize_sql(sql)
    if not raw_sql:
        raise TeradataExecutionError("SQL vide")

    teradatasql = _import_teradata_driver()

    connect_kwargs = {
        "host": config.host,
        "user": config.user,
        "password": config.password,
    }
    if config.database:
        connect_kwargs["database"] = config.database

    conn = None
    cursor = None
    try:
        conn = teradatasql.connect(**connect_kwargs)
        cursor = conn.cursor()
        cursor.execute(raw_sql)
        rows = cursor.fetchmany(int(row_limit))
        columns = [d[0] for d in (cursor.description or [])]
        return {
            "columns": columns,
            "rows": [list(r) for r in rows],
            "row_count": len(rows),
            "row_limit": row_limit,
            "source_format": "teradata",
            "tables_loaded": [],
        }
    except Exception as exc:
        raise TeradataExecutionError(f"Execution SQL Teradata impossible: {exc}") from exc
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
