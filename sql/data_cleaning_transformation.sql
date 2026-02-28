/* =========================================================
   PROYECTO: RETAIL E-COMMERCE ANALYTICS
   OBJETIVO:
   Preparar y limpiar datos transaccionales para análisis
   en Power BI o herramientas de BI.

   El proceso incluye:
   ✔ Importación de datos
   ✔ Eliminación de duplicados
   ✔ Limpieza y normalización
   ✔ Creaciones de métricas clave
   ✔ Clasificación de transacciones
   ✔ Preparación para análisis de pedidos
   ========================================================= */

-- =========================================================
-- PASO 0: Crear y seleccionar la base de datos
-- =========================================================

CREATE DATABASE retail_supplychain_db;
USE retail_supplychain_db;

-- =========================================================
-- PASO 1: Crear tabla staging (datos crudos)
-- Esta tabla almacena los datos exactamente como vienen del archivo.
-- =========================================================

CREATE TABLE online_retail_staging (
    InvoiceNo VARCHAR(20),
    StockCode VARCHAR(20),
    Description TEXT,
    Quantity INT,
    InvoiceDate VARCHAR(30),
    UnitPrice DECIMAL(10,4),
    CustomerID VARCHAR(20),
    Country VARCHAR(50)
);

-- =========================================================
-- PASO 2: Importar datos desde archivo CSV
-- =========================================================

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/OnlineRetail.csv'
INTO TABLE online_retail_staging
CHARACTER SET latin1
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- =========================================================
-- PASO 3: Detección y eliminación de duplicados
-- =========================================================

-- 3.1 Verificar duplicados exactos
SELECT 
    InvoiceNo, StockCode, Description, Quantity,
    InvoiceDate, UnitPrice, CustomerID, Country,
    COUNT(*) AS duplicate_count
FROM online_retail_staging
GROUP BY 
    InvoiceNo, StockCode, Description, Quantity,
    InvoiceDate, UnitPrice, CustomerID, Country
HAVING COUNT(*) > 1;

-- 3.2 Conteo total de duplicados
SELECT COUNT(*) 
FROM (
    SELECT COUNT(*) 
    FROM online_retail_staging
    GROUP BY 
        InvoiceNo, StockCode, Description, Quantity,
        InvoiceDate, UnitPrice, CustomerID, Country
    HAVING COUNT(*) > 1
) t;

-- 3.3 Crear tabla limpia sin duplicados
CREATE TABLE online_retail_clean AS
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   InvoiceNo, StockCode, Description, Quantity,
                   InvoiceDate, UnitPrice, CustomerID, Country
               ORDER BY InvoiceNo
           ) AS rn
    FROM online_retail_staging
) t
WHERE rn = 1;

-- =========================================================
-- PASO 4: Limpieza y normalización de datos
-- =========================================================

-- 4.1 Detectar valores vacíos
SELECT COUNT(*) AS blank_description
FROM online_retail_clean
WHERE TRIM(Description) = '' OR Description IS NULL;

SELECT COUNT(*) AS null_customer
FROM online_retail_clean
WHERE CustomerID IS NULL;

-- 4.2 Normalizar texto (consistencia para análisis)
UPDATE online_retail_clean
SET 
    Description = UPPER(TRIM(Description)),
    StockCode  = UPPER(TRIM(StockCode));

-- =========================================================
-- PASO 5: Crear métrica de ingresos (Revenue)
-- =========================================================

ALTER TABLE online_retail_clean
ADD COLUMN revenue DECIMAL(12,2);

UPDATE online_retail_clean
SET revenue = ROUND(Quantity * UnitPrice, 2);

-- Validación rápida
SELECT 
    MIN(revenue) AS min_revenue,
    MAX(revenue) AS max_revenue,
    SUM(revenue) AS total_revenue
FROM online_retail_clean;

-- =========================================================
-- PASO 6: Clasificación del tipo de transacción
-- Permite separar ventas reales de ajustes, pérdidas y devoluciones
-- =========================================================

ALTER TABLE online_retail_clean
ADD COLUMN transaction_type VARCHAR(30);

-- Devoluciones
UPDATE online_retail_clean
SET transaction_type = 'RETURN'
WHERE InvoiceNo LIKE 'C%';

-- Cargos operativos o financieros
UPDATE online_retail_clean
SET transaction_type = 'CHARGE'
WHERE transaction_type IS NULL
AND StockCode IN ('POST','DOT','M','S','D','C2','BANK CHARGES',
                  'AMAZON FEE','CRUK','PACKING CHARGE');

-- Pérdidas operativas
UPDATE online_retail_clean
SET transaction_type = 'OPERATIONAL_LOSS'
WHERE transaction_type IS NULL
AND (
    Description LIKE '%DAMAGED%' OR
    Description LIKE '%BROKEN%' OR
    Description LIKE '%LOST%' OR
    Description LIKE '%SMASHED%' OR
    Description LIKE '%CRUSHED%' OR
    Description LIKE '%RUST%' OR
    Description LIKE '%MISSING%'
);

-- Ajustes internos
UPDATE online_retail_clean
SET transaction_type = 'ADJUSTMENT'
WHERE transaction_type IS NULL
AND (
    Description LIKE '%ADJUST%' OR
    Description LIKE '%ERROR%' OR
    Description LIKE '%WRONG%' OR
    Description LIKE '%STOCK%' OR
    Description LIKE '%TEST%' OR
    Description LIKE '%CHECK%'
);

-- Ventas reales
UPDATE online_retail_clean
SET transaction_type = 'SALE'
WHERE transaction_type IS NULL;

-- Validación
SELECT transaction_type, COUNT(*) 
FROM online_retail_clean
GROUP BY transaction_type;

-- =========================================================
-- PASO 7: Limpieza final de columnas técnicas
-- =========================================================

ALTER TABLE online_retail_clean
DROP COLUMN rn;

-- =========================================================
-- PASO 8: Limpieza de valores faltantes en descripción
-- =========================================================

UPDATE online_retail_clean
SET Description = 'UNKNOWN'
WHERE Description IS NULL OR TRIM(Description) = '';

-- =========================================================
-- PASO 9: Convertir fechas a formato DATETIME
-- =========================================================

UPDATE online_retail_clean
SET InvoiceDate = STR_TO_DATE(InvoiceDate, '%m/%d/%Y %H:%i');

ALTER TABLE online_retail_clean
MODIFY COLUMN InvoiceDate DATETIME;

-- =========================================================
-- PASO 10: Crear identificador limpio de pedido
-- Permite analizar órdenes correctamente incluso con devoluciones
-- =========================================================

ALTER TABLE online_retail_clean
ADD COLUMN order_id_clean VARCHAR(20);

UPDATE online_retail_clean
SET order_id_clean = 
    CASE 
        WHEN InvoiceNo LIKE 'C%' THEN SUBSTRING(InvoiceNo, 2)
        ELSE InvoiceNo
    END;
