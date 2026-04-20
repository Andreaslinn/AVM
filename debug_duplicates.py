from sqlalchemy import text

from database import SessionLocal


def revisar_duplicados():
    with SessionLocal() as db:
        duplicados_url = db.execute(
            text(
                """
                SELECT url, COUNT(*) as veces
                FROM listings
                GROUP BY url
                HAVING COUNT(*) > 1
                ORDER BY veces DESC
                LIMIT 20;
                """
            )
        ).fetchall()

        if not duplicados_url:
            print("No hay duplicados por URL")
        else:
            print("Duplicados por URL:")
            for url, veces in duplicados_url:
                print(f"{url} -> {veces}")

        print("==============================")

        duplicados_titulo = db.execute(
            text(
                """
                SELECT titulo, COUNT(*) as veces
                FROM listings
                GROUP BY titulo
                HAVING COUNT(*) > 2
                ORDER BY veces DESC
                LIMIT 20;
                """
            )
        ).fetchall()

        if not duplicados_titulo:
            print("No hay duplicados por título")
        else:
            print("Duplicados por título:")
            for titulo, veces in duplicados_titulo:
                print(f"{titulo} -> {veces}")


if __name__ == "__main__":
    revisar_duplicados()
