import re


def document_sql(sql: str) -> dict:
    upper_sql = sql.upper()

    tables = sorted(set(re.findall(r"(?:FROM|JOIN)\s+([A-Z0-9_\.]+)", upper_sql)))
    columns = sorted(set(re.findall(r"SELECT\s+(.*?)\s+FROM", upper_sql, flags=re.DOTALL)))

    normalized_columns: list[str] = []
    if columns:
        raw_cols = columns[0].replace("\n", " ")
        normalized_columns = [c.strip() for c in raw_cols.split(",") if c.strip()]

    clause_logic = []
    if "WHERE" in upper_sql:
        clause_logic.append("WHERE: applique les filtres de selection.")
    if "GROUP BY" in upper_sql:
        clause_logic.append("GROUP BY: realise les aggregations par dimensions.")
    if "QUALIFY" in upper_sql:
        clause_logic.append("QUALIFY: filtre apres calcul fenetre, specifique Teradata.")
    if "ORDER BY" in upper_sql:
        clause_logic.append("ORDER BY: trie les resultats finaux.")

    summary = "Documente la logique principale de la requete SQL fournie."
    return {
        "summary": summary,
        "tables": tables,
        "columns": normalized_columns,
        "clause_logic": clause_logic,
    }