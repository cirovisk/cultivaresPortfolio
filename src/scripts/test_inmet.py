import pandas as pd
from pipeline.inmet import InmetExtractor
import logging

logging.basicConfig(level=logging.INFO)

def test_inmet_connection():
    # Testa apenas uma estação (A001 - Brasília) por 7 dias
    station_ids = ["A001"]
    extractor = InmetExtractor(days_history=7)
    
    print(f"Testando conexão INMET para: {station_ids}")
    data = extractor.extract(station_ids)
    
    if data and "A001" in data:
        df = data["A001"]
        print(f"Sucesso! Recebidos {len(df)} registros.")
        print(df.head())
    else:
        print("Falha: Nenhum dado recebido.")

if __name__ == "__main__":
    test_inmet_connection()
