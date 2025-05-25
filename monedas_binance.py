import requests 
import pandas as pd
from datetime import datetime, timedelta
import time

# Top 20 monedas (pares con USDT en Binance)
monedas_base = [
    {"binance_id": "BTCUSDT", "nombre": "Bitcoin", "simbolo": "BTC", "categoria": "Layer 1", "fecha_lanzamiento": "2009-01-03"},
    {"binance_id": "ETHUSDT", "nombre": "Ethereum", "simbolo": "ETH", "categoria": "Layer 1", "fecha_lanzamiento": "2015-07-30"},
    {"binance_id": "BNBUSDT", "nombre": "BNB", "simbolo": "BNB", "categoria": "Exchange", "fecha_lanzamiento": "2017-07-25"},
    {"binance_id": "SOLUSDT", "nombre": "Solana", "simbolo": "SOL", "categoria": "Layer 1", "fecha_lanzamiento": "2020-03-20"},
    {"binance_id": "XRPUSDT", "nombre": "XRP", "simbolo": "XRP", "categoria": "Payments", "fecha_lanzamiento": "2012-01-01"},
    {"binance_id": "ADAUSDT", "nombre": "Cardano", "simbolo": "ADA", "categoria": "Layer 1", "fecha_lanzamiento": "2017-09-29"},
    {"binance_id": "DOGEUSDT", "nombre": "Dogecoin", "simbolo": "DOGE", "categoria": "Meme", "fecha_lanzamiento": "2013-12-06"},
    {"binance_id": "AVAXUSDT", "nombre": "Avalanche", "simbolo": "AVAX", "categoria": "Layer 1", "fecha_lanzamiento": "2020-09-21"},
    {"binance_id": "DOTUSDT", "nombre": "Polkadot", "simbolo": "DOT", "categoria": "Layer 0", "fecha_lanzamiento": "2020-08-19"},
    {"binance_id": "TRXUSDT", "nombre": "Tron", "simbolo": "TRX", "categoria": "Smart Contract", "fecha_lanzamiento": "2017-09-13"},
    {"binance_id": "MATICUSDT", "nombre": "Polygon", "simbolo": "MATIC", "categoria": "Layer 2", "fecha_lanzamiento": "2019-04-26"},
    {"binance_id": "LINKUSDT", "nombre": "Chainlink", "simbolo": "LINK", "categoria": "Oracles", "fecha_lanzamiento": "2017-09-19"},
    {"binance_id": "LTCUSDT", "nombre": "Litecoin", "simbolo": "LTC", "categoria": "Payments", "fecha_lanzamiento": "2011-10-13"},
    {"binance_id": "ATOMUSDT", "nombre": "Cosmos", "simbolo": "ATOM", "categoria": "Layer 0", "fecha_lanzamiento": "2019-03-14"},
    {"binance_id": "BCHUSDT", "nombre": "Bitcoin Cash", "simbolo": "BCH", "categoria": "Payments", "fecha_lanzamiento": "2017-08-01"},
    {"binance_id": "XLMUSDT", "nombre": "Stellar", "simbolo": "XLM", "categoria": "Payments", "fecha_lanzamiento": "2014-07-31"},
    {"binance_id": "NEARUSDT", "nombre": "NEAR Protocol", "simbolo": "NEAR", "categoria": "Layer 1", "fecha_lanzamiento": "2020-10-13"},
    {"binance_id": "FILUSDT", "nombre": "Filecoin", "simbolo": "FIL", "categoria": "Storage", "fecha_lanzamiento": "2020-10-15"},
    {"binance_id": "APTUSDT", "nombre": "Aptos", "simbolo": "APT", "categoria": "Layer 1", "fecha_lanzamiento": "2022-10-17"},
    {"binance_id": "HBARUSDT", "nombre": "Hedera", "simbolo": "HBAR", "categoria": "Hashgraph", "fecha_lanzamiento": "2019-09-17"},
]

# Función para obtener datos históricos OHLCV
def obtener_transacciones(par, moneda_id, intervalo='1d'):
    url = 'https://api.binance.com/api/v3/klines'
    datos = []
    start_date = datetime(2017, 1, 1)
    now = datetime.now()

    while start_date < now:
        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int((start_date + timedelta(days=1000)).timestamp() * 1000)

        params = {
            'symbol': par,
            'interval': intervalo,
            'startTime': start_ms,
            'endTime': end_ms,
            'limit': 1000
        }

        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"Error con {par}: {response.text}")
            break

        candles = response.json()
        if not candles:
            break

        for c in candles:
            datos.append({
                'moneda_id': moneda_id,
                'fecha': datetime.fromtimestamp(c[0] / 1000),
                'precio_apertura': float(c[1]),
                'precio_max': float(c[2]),
                'precio_min': float(c[3]),
                'precio_cierre': float(c[4]),
                'volumen': float(c[5]),
                'precio': float(c[4]),  # Usamos cierre como "precio"
                'market_cap': None  # Binance no da market cap directo
            })

        start_date += timedelta(days=1000)
        time.sleep(0.3)

    return datos

# Simulamos métricas extra (ya que Binance no ofrece supply ni sentimiento)
def generar_metricas_dummy(moneda_id, fechas):
    metricas = []
    for fecha in fechas:
        metricas.append({
            'moneda_id': moneda_id,
            'fecha': fecha,
            'total_supply': None,
            'circulating_supply': None,
            'max_supply': None,
            'sentiment_score': None,
            'cambio_24h': None,
            'cambio_7d': None
        })
    return metricas

# =====================
#  EJECUCIÓN PRINCIPAL
# =====================
monedas_df = pd.DataFrame(monedas_base)
monedas_df['id'] = monedas_df.index + 1  # Simula ID autoincremental

transacciones = []
metricas = []

for _, moneda in monedas_df.iterrows():
    print(f"Procesando {moneda['binance_id']}...")
    transac = obtener_transacciones(moneda['binance_id'], moneda['id'])
    transacciones.extend(transac)

    fechas = [t['fecha'] for t in transac]
    metricas.extend(generar_metricas_dummy(moneda['id'], fechas))

# Guardar a CSV
monedas_df[['id', 'binance_id', 'nombre', 'simbolo', 'categoria', 'fecha_lanzamiento']].to_csv('monedas.csv', index=False)
pd.DataFrame(transacciones).to_csv('transacciones_moneda.csv', index=False)
pd.DataFrame(metricas).to_csv('metricas_extra.csv', index=False)

print("✅ Archivos CSV generados exitosamente.")
