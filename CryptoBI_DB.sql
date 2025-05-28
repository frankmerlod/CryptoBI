CREATE DATABASE CryptoBI;

USE CryptoBI;

CREATE TABLE monedas (
    id INT PRIMARY KEY,
    binance_id NVARCHAR(20) UNIQUE, -- por ej: 'BTCUSDT' 
    nombre NVARCHAR(20),
    simbolo NVARCHAR(20),
    categoria NVARCHAR(20),
    fecha_lanzamiento DATE
)

CREATE TABLE transacciones_moneda (
    id INT IDENTITY(1,1) PRIMARY KEY,
    moneda_id INTEGER REFERENCES monedas(id),
    fecha DATETIME,
    precio NUMERIC(30, 10),
    volumen DECIMAL(30, 10),
    market_cap DECIMAL(30, 10),
    precio_max DECIMAL(30, 10),
    precio_min DECIMAL(30, 10),
    precio_apertura DECIMAL(30, 10),
    precio_cierre DECIMAL(30, 10)
)

CREATE TABLE metricas_extra (
    id INT IDENTITY(1,1) PRIMARY KEY,
    moneda_id INTEGER REFERENCES monedas(id),
    fecha DATETIME,
    total_supply DECIMAL(30, 10),
    circulating_supply DECIMAL(30, 10),
    max_supply DECIMAL(30, 10),
    sentiment_score DECIMAL(10,4),
    cambio_24h DECIMAL(10,4),
    cambio_7d DECIMAL(10,4)
)

SELECT * FROM transacciones_moneda;
DROP DATABASE CryptoBI