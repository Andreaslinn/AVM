from database import SessionLocal
from comparables import calcular_tasacion_comparables


db = SessionLocal()

try:
    property_data = {
        "comuna": "Providencia",
        "m2_construidos": 85,
        "dormitorios": 3,
        "banos": 2,
    }

    resultado = calcular_tasacion_comparables(db, property_data)

    print("\nRESULTADO FINAL:")
    print(resultado)
finally:
    db.close()
