import requests
import pandas as pd
from datetime import datetime
import time
from dotenv import load_dotenv
import os

load_dotenv()

# Tu API Key de Binance (solo lectura)
API_KEY = os.getenv("API_KEY")

# Lista de monedas con sus IDs de CoinGecko
monedas = [
    {"id": 1, "binance_id": "BTCUSDT", "coingecko_id": "bitcoin"},
    {"id": 2, "binance_id": "ETHUSDT", "coingecko_id": "ethereum"},
    {"id": 3, "binance_id": "BNBUSDT", "coingecko_id": "binancecoin"},
    {"id": 4, "binance_id": "SOLUSDT", "coingecko_id": "solana"},
    {"id": 5, "binance_id": "XRPUSDT", "coingecko_id": "ripple"},
    {"id": 6, "binance_id": "ADAUSDT", "coingecko_id": "cardano"},
    {"id": 7, "binance_id": "DOGEUSDT", "coingecko_id": "dogecoin"},
    {"id": 8, "binance_id": "AVAXUSDT", "coingecko_id": "avalanche-2"},
    {"id": 9, "binance_id": "DOTUSDT", "coingecko_id": "polkadot"},
    {"id": 10, "binance_id": "TRXUSDT", "coingecko_id": "tron"},
    {"id": 11, "binance_id": "MATICUSDT", "coingecko_id": "polygon"},
    {"id": 12, "binance_id": "LINKUSDT", "coingecko_id": "chainlink"},
    {"id": 13, "binance_id": "LTCUSDT", "coingecko_id": "litecoin"},
    {"id": 14, "binance_id": "ATOMUSDT", "coingecko_id": "cosmos"},
    {"id": 15, "binance_id": "BCHUSDT", "coingecko_id": "bitcoin-cash"},
    {"id": 16, "binance_id": "XLMUSDT", "coingecko_id": "stellar"},
    {"id": 17, "binance_id": "NEARUSDT", "coingecko_id": "near"},
    {"id": 18, "binance_id": "FILUSDT", "coingecko_id": "filecoin"},
    {"id": 19, "binance_id": "APTUSDT", "coingecko_id": "aptos"},
    {"id": 20, "binance_id": "HBARUSDT", "coingecko_id": "hedera-hashgraph"},
]


def obtener_metricas_coingecko(coingecko_id, intentos=10):
    url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}"
    headers = {"x-cg-api-key": API_KEY}

    for intento in range(1, intentos + 1):
        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                data = response.json()
                market_data = data.get("market_data", {})
                return {
                    "total_supply": market_data.get("total_supply"),
                    "circulating_supply": market_data.get("circulating_supply"),
                    "max_supply": market_data.get("max_supply"),
                    "cambio_24h": market_data.get("price_change_percentage_24h"),
                    "cambio_7d": market_data.get("price_change_percentage_7d"),
                }
            elif response.status_code == 429:
                wait_time = 5 * intento
                print(f"[429] Demasiadas solicitudes. Reintentando en {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(
                    f"Error {response.status_code} al obtener datos para {coingecko_id}"
                )
                return None
        except Exception as e:
            print(f"Excepción en intento {intento} para {coingecko_id}: {e}")
            time.sleep(5)
    print(
        f"No se pudieron obtener los datos de {coingecko_id} después de varios intentos."
    )
    return None


# Lista para almacenar las métricas
metricas = []

# Fecha actual
fecha_actual = datetime.now().strftime("%Y-%m-%d")

for moneda in monedas:
    print(f"Obteniendo métricas para {moneda['coingecko_id']}...")
    datos = obtener_metricas_coingecko(moneda["coingecko_id"])
    if datos:
        metricas.append(
            {
                "moneda_id": moneda["id"],
                "fecha": fecha_actual,
                "total_supply": datos["total_supply"],
                "circulating_supply": datos["circulating_supply"],
                "max_supply": datos["max_supply"],
                "sentiment_score": None,
                "cambio_24h": datos["cambio_24h"],
                "cambio_7d": datos["cambio_7d"],
            }
        )
    time.sleep(5)

# Guardar las métricas en un archivo CSV
df_metricas = pd.DataFrame(metricas)
df_metricas.to_csv("metricas_extra.csv", index=False)
print("Archivo 'metricas_extra.csv' generado exitosamente.")
