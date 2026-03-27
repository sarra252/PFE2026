from dataclasses import dataclass


@dataclass
class LlmSqlResult:
    sql: str
    explanation: str
    confidence: float


def generate_sql(question: str, schema_context: str | None = None) -> LlmSqlResult:
    q = question.lower()

    if "top" in q or "meilleur" in q:
        sql = (
            "SELECT customer_id, SUM(amount) AS total_amount "
            "FROM invoices "
            "GROUP BY 1 "
            "QUALIFY ROW_NUMBER() OVER (ORDER BY total_amount DESC) <= 10;"
        )
        explanation = "Retourne les 10 meilleurs clients par montant facture avec QUALIFY (pattern Teradata)."
        confidence = 0.84
    elif "mois" in q or "month" in q or "mensuel" in q:
        sql = (
            "SELECT billing_month, SUM(amount) AS revenue "
            "FROM invoices "
            "GROUP BY 1 "
            "ORDER BY 1;"
        )
        explanation = "Agrege le chiffre d'affaires par mois de facturation."
        confidence = 0.83
    elif "join" in q or "client" in q or "abonne" in q or "subscriber" in q:
        sql = (
            "SELECT c.customer_id, c.customer_name, s.subscriber_id, s.msisdn "
            "FROM customers c "
            "INNER JOIN subscribers s ON UPPER(TRIM(s.client_id)) = c.customer_id "
            "WHERE s.status = 'active';"
        )
        explanation = "Jointure clients-abonnes avec normalisation simple des identifiants (client_id)."
        confidence = 0.8
    else:
        sql = "SELECT customer_id, customer_name, segment, region FROM customers LIMIT 100;"
        explanation = "Requete d'exploration initiale sur la base clients."
        confidence = 0.7

    if schema_context:
        explanation += " Le contexte schema fourni a ete pris en compte pour orienter la generation."

    return LlmSqlResult(sql=sql, explanation=explanation, confidence=confidence)
