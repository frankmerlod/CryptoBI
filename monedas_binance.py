import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# Top 20 monedas (pares con USDT en Binance)
monedas_base = [
    {
        "binance_id": "BTCUSDT",
        "nombre": "Bitcoin",
        "simbolo": "BTC",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2009-01-03",
    },
    {
        "binance_id": "ETHUSDT",
        "nombre": "Ethereum",
        "simbolo": "ETH",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2015-07-30",
    },
    {
        "binance_id": "BNBUSDT",
        "nombre": "BNB",
        "simbolo": "BNB",
        "categoria": "Exchange",
        "fecha_lanzamiento": "2017-07-25",
    },
    {
        "binance_id": "SOLUSDT",
        "nombre": "Solana",
        "simbolo": "SOL",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2020-03-20",
    },
    {
        "binance_id": "XRPUSDT",
        "nombre": "XRP",
        "simbolo": "XRP",
        "categoria": "Payments",
        "fecha_lanzamiento": "2012-01-01",
    },
    {
        "binance_id": "ADAUSDT",
        "nombre": "Cardano",
        "simbolo": "ADA",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2017-09-29",
    },
    {
        "binance_id": "DOGEUSDT",
        "nombre": "Dogecoin",
        "simbolo": "DOGE",
        "categoria": "Meme",
        "fecha_lanzamiento": "2013-12-06",
    },
    {
        "binance_id": "AVAXUSDT",
        "nombre": "Avalanche",
        "simbolo": "AVAX",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2020-09-21",
    },
    {
        "binance_id": "DOTUSDT",
        "nombre": "Polkadot",
        "simbolo": "DOT",
        "categoria": "Layer 0",
        "fecha_lanzamiento": "2020-08-19",
    },
    {
        "binance_id": "TRXUSDT",
        "nombre": "Tron",
        "simbolo": "TRX",
        "categoria": "Smart Contract",
        "fecha_lanzamiento": "2017-09-13",
    },
    {
        "binance_id": "MATICUSDT",
        "nombre": "Polygon",
        "simbolo": "MATIC",
        "categoria": "Layer 2",
        "fecha_lanzamiento": "2019-04-26",
    },
    {
        "binance_id": "LINKUSDT",
        "nombre": "Chainlink",
        "simbolo": "LINK",
        "categoria": "Oracles",
        "fecha_lanzamiento": "2017-09-19",
    },
    {
        "binance_id": "LTCUSDT",
        "nombre": "Litecoin",
        "simbolo": "LTC",
        "categoria": "Payments",
        "fecha_lanzamiento": "2011-10-13",
    },
    {
        "binance_id": "ATOMUSDT",
        "nombre": "Cosmos",
        "simbolo": "ATOM",
        "categoria": "Layer 0",
        "fecha_lanzamiento": "2019-03-14",
    },
    {
        "binance_id": "BCHUSDT",
        "nombre": "Bitcoin Cash",
        "simbolo": "BCH",
        "categoria": "Payments",
        "fecha_lanzamiento": "2017-08-01",
    },
    {
        "binance_id": "XLMUSDT",
        "nombre": "Stellar",
        "simbolo": "XLM",
        "categoria": "Payments",
        "fecha_lanzamiento": "2014-07-31",
    },
    {
        "binance_id": "NEARUSDT",
        "nombre": "NEAR Protocol",
        "simbolo": "NEAR",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2020-10-13",
    },
    {
        "binance_id": "FILUSDT",
        "nombre": "Filecoin",
        "simbolo": "FIL",
        "categoria": "Storage",
        "fecha_lanzamiento": "2020-10-15",
    },
    {
        "binance_id": "APTUSDT",
        "nombre": "Aptos",
        "simbolo": "APT",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2022-10-17",
    },
    {
        "binance_id": "HBARUSDT",
        "nombre": "Hedera",
        "simbolo": "HBAR",
        "categoria": "Hashgraph",
        "fecha_lanzamiento": "2019-09-17",
    },
]


def obtener_fecha_inicio_binance(par, intervalo="1h"):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": par,
        "interval": intervalo,
        "startTime": 0,  # Epoch 1970-01-01, mínimo tiempo posible
        "limit": 1,
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"Error al obtener fecha inicio para {par}: {response.text}")
        return None

    data = response.json()
    if not data:
        print(f"No hay datos para {par}")
        return None

    primer_timestamp = data[0][0]  # timestamp en ms
    fecha_inicio = datetime.fromtimestamp(primer_timestamp / 1000).strftime("%Y-%m-%d")
    return fecha_inicio


# Función para obtener datos históricos OHLCV
def obtener_transacciones(par, moneda_id, fecha_inicio_str, intervalo="1h"):
    url = "https://api.binance.com/api/v3/klines"
    datos = []

    try:
        start_date = datetime.strptime(fecha_inicio_str, "%Y-%m-%d")
        now = datetime.now()

        while start_date < now:
            start_ms = int(start_date.timestamp() * 1000)
            params = {
                "symbol": par,
                "interval": intervalo,
                "startTime": start_ms,
                "limit": 1000,
            }

            response = requests.get(url, params=params)
            if response.status_code != 200:
                print(f"Error con {par}: {response.text}")
                break

            candles = response.json()
            if not candles:
                break

            for c in candles:
                datos.append(
                    {
                        "moneda_id": moneda_id,
                        "fecha": datetime.fromtimestamp(c[0] / 1000),
                        "precio_apertura": float(c[1]),
                        "precio_max": float(c[2]),
                        "precio_min": float(c[3]),
                        "precio_cierre": float(c[4]),
                        "volumen": float(c[5]),
                        "precio": float(c[4]),
                        "market_cap": None,
                    }
                )

            # Avanzar al siguiente batch de datos
            last_timestamp = candles[-1][0]
            start_date = datetime.fromtimestamp((last_timestamp + 1) / 1000)

            time.sleep(1)

    except Exception as e:
        print(f"❌ Error inesperado para {par}: {e}")

    return datos  # <- SIEMPRE retorna una lista (aunque sea vacía)


# =====================
#  EJECUCIÓN PRINCIPAL
# =====================
monedas_df = pd.DataFrame(monedas_base)
monedas_df["id"] = monedas_df.index + 1  # Simula ID autoincremental

transacciones = []
metricas = []

for _, moneda in monedas_df.iterrows():
    print(f"Obteniendo fecha de inicio real en Binance para {moneda['binance_id']}...")
    fecha_inicio_real = obtener_fecha_inicio_binance(moneda["binance_id"])
    if not fecha_inicio_real:
        fecha_inicio_real = moneda["fecha_lanzamiento"]

    print(f"Procesando {moneda['binance_id']} desde {fecha_inicio_real}...")
    transac = obtener_transacciones(
        moneda["binance_id"], moneda["id"], fecha_inicio_real
    )
    if transac:
        transacciones.extend(transac)
    else:
        print(f"⚠️ No se encontraron transacciones para {moneda['binance_id']}")


# Guardar a CSV
monedas_df[
    ["id", "binance_id", "nombre", "simbolo", "categoria", "fecha_lanzamiento"]
].to_csv("monedas.csv", index=False)
pd.DataFrame(transacciones).to_csv("transacciones_moneda.csv", index=False)

print("✅ Archivos CSV generados exitosamente.")
