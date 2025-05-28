import requests
import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import shutil

load_dotenv()
API_KEY = os.getenv("API_KEY")
SERVER = 'localhost\\SQLEXPRESS'
DATABASE = 'CryptoBI'


# conexion y carga a la base de datos
def get_connection():
    print(f"🗄️ 🟢 Estableciendo Conexión a la Base de Datos")
    connection_string = ('mssql+pyodbc://@' + SERVER + '/' + DATABASE + '?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server')
    # Crear el engine
    engine = create_engine(connection_string)
    return engine

def verificar_datos_nuevos(df, nombre_tabla):
    engine = get_connection()
    # Filtrando el df para cargar solamente datos nuevos
    print(f"🗄️ 🔍 Buscando nuevos registros en el Dataframe")
    consulta = f"""
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
    WHERE TABLE_NAME = '{nombre_tabla}';
    """
    resultado = pd.read_sql(consulta, engine)
    resultado = resultado["COLUMN_NAME"].iloc[0] if not resultado.empty else None
    ids_existentes = pd.read_sql(f"SELECT {resultado} FROM {nombre_tabla}", engine)
    ids_set = set(ids_existentes[resultado])
    df_nuevos = df[~df[resultado].isin(ids_set)]
    return df_nuevos

def cargar_a_sql(df, nombre_tabla):
    try:
        engine = get_connection()
        # Verificar si existen nuevos registros
        df_nuevos = verificar_datos_nuevos(df, nombre_tabla)
        if len(df_nuevos) > 0:
            df_nuevos.to_sql(nombre_tabla, con=engine, if_exists='append', index=False)
            print(f"🗄️ ✅ Datos cargados a la tabla {nombre_tabla}\n")
        else:
            print(f"🗄️ ❌ No hay nuevos registros para agregar en la tabla {nombre_tabla}\n")
    except SQLAlchemyError as e:
        print(f'Error en la Base de datos: {e}')
        

# Top 20 monedas (pares con USDT en Binance)
monedas_base = [
    {
        "binance_id": "BTCUSDT",
        "nombre": "Bitcoin",
        "simbolo": "BTC",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2009-01-03",
        "coingecko_id": "bitcoin"
    },
    {
        "binance_id": "ETHUSDT",
        "nombre": "Ethereum",
        "simbolo": "ETH",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2015-07-30",
        "coingecko_id": "ethereum"
    },
    {
        "binance_id": "BNBUSDT",
        "nombre": "BNB",
        "simbolo": "BNB",
        "categoria": "Exchange",
        "fecha_lanzamiento": "2017-07-25",
        "coingecko_id": "binancecoin"
    },
    {
        "binance_id": "SOLUSDT",
        "nombre": "Solana",
        "simbolo": "SOL",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2020-03-20",
        "coingecko_id": "solana"
    },
    {
        "binance_id": "XRPUSDT",
        "nombre": "XRP",
        "simbolo": "XRP",
        "categoria": "Payments",
        "fecha_lanzamiento": "2012-01-01",
        "coingecko_id": "ripple"
    },
    {
        "binance_id": "ADAUSDT",
        "nombre": "Cardano",
        "simbolo": "ADA",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2017-09-29",
        "coingecko_id": "cardano"
    },
    {
        "binance_id": "DOGEUSDT",
        "nombre": "Dogecoin",
        "simbolo": "DOGE",
        "categoria": "Meme",
        "fecha_lanzamiento": "2013-12-06",
        "coingecko_id": "dogecoin"
    },
    {
        "binance_id": "AVAXUSDT",
        "nombre": "Avalanche",
        "simbolo": "AVAX",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2020-09-21",
        "coingecko_id": "avalanche-2"
    },
    {
        "binance_id": "DOTUSDT",
        "nombre": "Polkadot",
        "simbolo": "DOT",
        "categoria": "Layer 0",
        "fecha_lanzamiento": "2020-08-19",
        "coingecko_id": "polkadot"
    },
    {
        "binance_id": "TRXUSDT",
        "nombre": "Tron",
        "simbolo": "TRX",
        "categoria": "Smart Contract",
        "fecha_lanzamiento": "2017-09-13",
        "coingecko_id": "tron"
    },
    {
        "binance_id": "MATICUSDT",
        "nombre": "Polygon",
        "simbolo": "MATIC",
        "categoria": "Layer 2",
        "fecha_lanzamiento": "2019-04-26",
        "coingecko_id": "matic-network"
    },
    {
        "binance_id": "LINKUSDT",
        "nombre": "Chainlink",
        "simbolo": "LINK",
        "categoria": "Oracles",
        "fecha_lanzamiento": "2017-09-19",
        "coingecko_id": "chainlink"
    },
    {
        "binance_id": "LTCUSDT",
        "nombre": "Litecoin",
        "simbolo": "LTC",
        "categoria": "Payments",
        "fecha_lanzamiento": "2011-10-13",
        "coingecko_id": "litecoin"
    },
    {
        "binance_id": "ATOMUSDT",
        "nombre": "Cosmos",
        "simbolo": "ATOM",
        "categoria": "Layer 0",
        "fecha_lanzamiento": "2019-03-14",
        "coingecko_id": "cosmos"
    },
    {
        "binance_id": "BCHUSDT",
        "nombre": "Bitcoin Cash",
        "simbolo": "BCH",
        "categoria": "Payments",
        "fecha_lanzamiento": "2017-08-01",
        "coingecko_id": "bitcoin-cash"
    },
    {
        "binance_id": "XLMUSDT",
        "nombre": "Stellar",
        "simbolo": "XLM",
        "categoria": "Payments",
        "fecha_lanzamiento": "2014-07-31",
        "coingecko_id": "stellar"
    },
    {
        "binance_id": "NEARUSDT",
        "nombre": "NEAR Protocol",
        "simbolo": "NEAR",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2020-10-13",
        "coingecko_id": "near"
    },
    {
        "binance_id": "FILUSDT",
        "nombre": "Filecoin",
        "simbolo": "FIL",
        "categoria": "Storage",
        "fecha_lanzamiento": "2020-10-15",
        "coingecko_id": "filecoin"
    },
    {
        "binance_id": "APTUSDT",
        "nombre": "Aptos",
        "simbolo": "APT",
        "categoria": "Layer 1",
        "fecha_lanzamiento": "2022-10-17",
        "coingecko_id": "aptos"
    },
    {
        "binance_id": "HBARUSDT",
        "nombre": "Hedera",
        "simbolo": "HBAR",
        "categoria": "Hashgraph",
        "fecha_lanzamiento": "2019-09-17",
        "coingecko_id": "hedera-hashgraph"
    },
]

# Verificar desde donde hay registros de la moneda en Binance
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
                wait_time = 15 * intento
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

# =====================
#  EJECUCIÓN PRINCIPAL
# =====================
if __name__ == '__main__':
    monedas_df = pd.DataFrame(monedas_base)
    monedas_df["id"] = monedas_df.index + 1  # Simula ID autoincremental

    # Fecha actual
    fecha_actual = datetime.now().strftime("%Y-%m-%d")

    # Generando trasacciones desde Binance
    transacciones = []
    
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
            
    # Generando metricas desde CoinGecko
    metricas = []

    for moneda in monedas_base:
        print(f"Obteniendo métricas para {moneda['coingecko_id']}...")
        datos = obtener_metricas_coingecko(moneda["coingecko_id"])
        if datos:
            metricas.append(
                {
                    "moneda_id": monedas_df['id'].loc[monedas_df['coingecko_id'] == moneda["coingecko_id"]],
                    "fecha": fecha_actual,
                    "total_supply": datos["total_supply"],
                    "circulating_supply": datos["circulating_supply"],
                    "max_supply": datos["max_supply"],
                    "sentiment_score": None,
                    "cambio_24h": datos["cambio_24h"],
                    "cambio_7d": datos["cambio_7d"],
                }
            )
        time.sleep(10)

    # Guardar las transacciones y métricas en un archivo CSV
    df_metricas = pd.DataFrame(metricas)
    df_transacciones = pd.DataFrame(transacciones)
    print("✅ Archivos CSV generados exitosamente.")

    # Cargar a SQL Server
    tablas = [('monedas', monedas_df), ('transacciones_moneda', df_transacciones), ('metricas_extra', df_metricas)]
    for tabla in tablas:
        cargar_a_sql(tabla[1], tabla[0])
    



