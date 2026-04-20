import json
import os
import sqlite3
from datetime import datetime

from audits.product_audit import run_product_audit


def load_sample_opportunities():
    db_path = os.environ.get("TASADOR_DB_PATH", "tasador.db")
    opportunities = []

    try:
        with sqlite3.connect(db_path) as connection:
            connection.row_factory = sqlite3.Row
            columns = {
                row["name"]
                for row in connection.execute("PRAGMA table_info(listings)").fetchall()
            }

            if not columns:
                print("No existe tabla listings en DB")
                print("Cargados 0 listings desde DB")
                return []

            def col(name, default="NULL"):
                return name if name in columns else default

            status_filter = "WHERE status = 'active'" if "status" in columns else ""
            query = f"""
                SELECT
                    {col("id")} AS listing_id,
                    {col("precio_clp")} AS precio_publicado,
                    {col("m2_construidos")} AS m2,
                    {col("score", "0")} AS score,
                    {col("comparables", "0")} AS comparables,
                    {col("confianza", "0")} AS confianza,
                    {col("porcentaje_campos_faltantes", "0")} AS porcentaje_campos_faltantes
                FROM listings
                {status_filter}
                LIMIT 10
            """
            print("QUERY:", query)
            rows = connection.execute(query).fetchall()

        for row in rows:
            opportunities.append(
                {
                    "listing_id": row["listing_id"],
                    "precio_publicado": row["precio_publicado"],
                    "m2": row["m2"],
                    "score": row["score"] or 0,
                    "comparables": row["comparables"] or 0,
                    "confianza": row["confianza"] or 0,
                    "porcentaje_campos_faltantes": row["porcentaje_campos_faltantes"] or 0,
                }
            )
    except Exception as exc:
        print("Error cargando listings desde DB:", exc)
        opportunities = []

    print(f"Cargados {len(opportunities)} listings desde DB")
    return opportunities


def main():
    print("Running product audit runner...")

    opportunities = load_sample_opportunities()

    result = run_product_audit(opportunities)

    os.makedirs("reports", exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"reports/product_audit_{timestamp}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print("Product audit completado")
    print(f"Reporte generado: {path}")


if __name__ == "__main__":
    main()
