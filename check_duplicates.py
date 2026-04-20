import sqlite3


DATABASE_PATH = "tasador.db"


QUERY = """
SELECT fuente, link, COUNT(*) AS veces
FROM listings
WHERE link IS NOT NULL
GROUP BY fuente, link
HAVING COUNT(*) > 1
ORDER BY veces DESC
LIMIT 20;
"""


def check_duplicates():
    with sqlite3.connect(DATABASE_PATH) as connection:
        rows = connection.execute(QUERY).fetchall()

    if not rows:
        print("Sin duplicados por fuente+link")
        return

    print("Duplicados por fuente+link:")
    for fuente, link, veces in rows:
        print(f"{fuente} | {link} -> {veces}")


if __name__ == "__main__":
    check_duplicates()
