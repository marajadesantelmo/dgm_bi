# -*- coding: utf-8 -*-
"""
Script de diagnóstico para ejecutar en el servidor (donde hay acceso MySQL).
Guarda resultados como CSVs para commitear y revisar localmente.

Objetivo: investigar los tres problemas de la reunión 2026-06-19:
  1. Erogaciones de capital faltantes (maquinaria, vehículos, obras en curso)
  2. Movimientos en moneda extranjera (Incol Corp ~$158K + $113K USD)
  3. Tres empresas sin movimientos: Incol, AUTOMOTORES EL TRIANGULO, ISUZU
"""

import mysql.connector
import pandas as pd
from tokens import host, user, database

connection = mysql.connector.connect(
    host=host,
    user=user,
    database=database,
    charset='utf8'
)
cursor = connection.cursor()

def run_query(sql, filename, description):
    print(f"\n=== {description} ===")
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        cols = [c[0] for c in cursor.description]
        df = pd.DataFrame(data, columns=cols)
        df.to_csv(f"diagnostico/{filename}", index=False, encoding='utf-8-sig')
        print(f"  → {len(df)} filas guardadas en diagnostico/{filename}")
        if len(df) <= 20:
            print(df.to_string())
    except Exception as e:
        print(f"  ERROR: {e}")

import os
os.makedirs("diagnostico", exist_ok=True)

# ─── 1. Schema discovery ─────────────────────────────────────────────────────

for tabla in ['compras', 'asientos', 'asientositems', 'monedacotizaciones']:
    run_query(
        f"DESCRIBE {tabla};",
        f"schema_{tabla}.csv",
        f"Schema de tabla: {tabla}"
    )

# ─── 2. Monedas disponibles ───────────────────────────────────────────────────

run_query("""
SELECT RecID, CotMoneda2, FechaVigencia, IDMoneda
FROM monedacotizaciones
ORDER BY FechaVigencia DESC
LIMIT 30;
""", "monedas_cotizaciones_sample.csv", "Sample de monedacotizaciones")

# ─── 3. Movimientos Incol Corp ────────────────────────────────────────────────

run_query("""
SELECT
    a.FechaCreacion, a.TipoOrigen,
    ai.Importe1, ai.TipoSaldo,
    cc.Numero, cc.Descripcion AS CuentaDesc,
    f.RazonSocial,
    a.Descripcion AS Detalle
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
LEFT JOIN compras comp ON a.IDOrigen = comp.RecID
LEFT JOIN fiscal f ON comp.IDFiscal = f.RecID
WHERE (f.RazonSocial LIKE '%INCOL%' OR f.RazonSocial LIKE '%Incol%')
AND a.FechaCreacion >= '2024-01-01'
ORDER BY a.FechaCreacion;
""", "incol_movimientos.csv", "Movimientos Incol Corp")

# ─── 4. Movimientos AUTOMOTORES EL TRIANGULO y ISUZU ─────────────────────────

run_query("""
SELECT
    a.FechaCreacion, a.TipoOrigen,
    ai.Importe1, ai.TipoSaldo,
    cc.Numero, cc.Descripcion AS CuentaDesc,
    f.RazonSocial,
    a.Descripcion AS Detalle
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
LEFT JOIN compras comp ON a.IDOrigen = comp.RecID
LEFT JOIN fiscal f ON comp.IDFiscal = f.RecID
WHERE (f.RazonSocial LIKE '%TRIANGULO%' OR f.RazonSocial LIKE '%ISUZU%' OR f.RazonSocial LIKE '%Isuzu%')
AND a.FechaCreacion >= '2024-01-01'
ORDER BY a.FechaCreacion;
""", "automotores_isuzu_movimientos.csv", "Movimientos AUTOMOTORES EL TRIANGULO e ISUZU")

# ─── 5. Cuentas patrimoniales (1.x.x.x) con movimientos ─────────────────────

run_query("""
SELECT
    cc.Numero, cc.Descripcion,
    a.TipoOrigen,
    COUNT(*) AS Cant_Asientos,
    SUM(ai.Importe1) AS Total_Importe1
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE cc.Numero LIKE '1%'
AND a.FechaCreacion >= '2024-01-01'
AND ai.TipoSaldo = 0
GROUP BY cc.Numero, cc.Descripcion, a.TipoOrigen
ORDER BY cc.Numero, a.TipoOrigen;
""", "cuentas_patrimoniales_movimientos.csv", "Cuentas patrimoniales (1.x.x.x) con movimientos desde 2024")

# ─── 6. Si compras tiene IDCotizacionMoneda ───────────────────────────────────
# Este bloque intenta el join; si falla es porque el campo no existe

run_query("""
SELECT
    comp.RecID,
    f.RazonSocial,
    comp.FechaCreacion,
    mc.CotMoneda2,
    mc.IDMoneda
FROM compras comp
LEFT JOIN monedacotizaciones mc ON comp.IDCotizacionMoneda = mc.RecID
LEFT JOIN fiscal f ON comp.IDFiscal = f.RecID
WHERE (f.RazonSocial LIKE '%INCOL%' OR f.RazonSocial LIKE '%Incol%')
AND comp.FechaCreacion >= '2024-01-01'
ORDER BY comp.FechaCreacion;
""", "incol_compras_moneda.csv", "Compras Incol con cotización de moneda (si el campo existe)")

# ─── 7. Verificar cuentas de maquinaria y obras en curso ─────────────────────

run_query("""
SELECT cc.Numero, cc.Descripcion, cc.Nivel, cc.Imputable
FROM cuentascontables cc
WHERE cc.Numero LIKE '1.2%' OR cc.Descripcion LIKE '%aquinaria%'
   OR cc.Descripcion LIKE '%bra%' OR cc.Descripcion LIKE '%ehiculo%'
   OR cc.Descripcion LIKE '%orno%'
ORDER BY cc.Numero;
""", "cuentas_capital_descripcion.csv", "Cuentas de maquinaria, obras, vehículos en el plan de cuentas")

# ─── 8. Detalle mensual de cuentas patrimoniales clave ───────────────────────

run_query("""
SELECT
    DATE_FORMAT(a.FechaCreacion, '%Y-%m') AS Mes,
    cc.Numero, cc.Descripcion,
    a.TipoOrigen,
    SUM(ai.Importe1) AS Total,
    COUNT(*) AS Cant
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE cc.Numero LIKE '1%'
AND a.FechaCreacion BETWEEN '2026-01-01' AND '2026-12-31'
AND ai.TipoSaldo = 0
GROUP BY Mes, cc.Numero, cc.Descripcion, a.TipoOrigen
ORDER BY Mes, cc.Numero;
""", "patrimoniales_2026_mensual.csv", "Detalle mensual 2026 de cuentas patrimoniales")

# ─── 9. Buscar Incol en fiscal sin filtro de fecha ───────────────────────────

run_query("""
SELECT DISTINCT f.RazonSocial, f.RecID
FROM fiscal f
WHERE f.RazonSocial LIKE '%INCOL%' OR f.RazonSocial LIKE '%Incol%'
   OR f.RazonSocial LIKE '%TRIANGULO%' OR f.RazonSocial LIKE '%ISUZU%';
""", "fiscal_empresas_testigo.csv", "Razones sociales de Incol, Triangulo, ISUZU en tabla fiscal")

# ─── 10. Sueldos Salta julio 2025: ¿hay datos? ¿qué Detalle tienen? ──────────

run_query("""
SELECT
    a.FechaCreacion,
    ai.TipoSaldo,
    ai.Importe1,
    cc.Numero,
    cc.Descripcion AS Concepto,
    a.Descripcion   AS Detalle,
    a.TipoOrigen
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE cc.Numero IN ('21301001', '21301002')
AND a.FechaCreacion >= '2025-06-01'
AND a.FechaCreacion <  '2025-09-01'
ORDER BY a.FechaCreacion, cc.Numero;
""", "sueldos_salta_jun_ago_2025.csv",
    "Sueldos cuentas 21301001/002 - jun/jul/ago 2025 (todos TipoSaldo, todos Detalle)")

# ─── 11. Participación mensual de sueldos (resumen anual para detectar huecos) ──

run_query("""
SELECT
    DATE_FORMAT(a.FechaCreacion, '%Y-%m') AS Mes,
    cc.Numero,
    cc.Descripcion AS Concepto,
    ai.TipoSaldo,
    COUNT(*)            AS Cant_Asientos,
    SUM(ai.Importe1)    AS Total
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE cc.Numero IN ('21301001', '21301002')
AND a.FechaCreacion >= '2024-01-01'
GROUP BY Mes, cc.Numero, cc.Descripcion, ai.TipoSaldo
ORDER BY Mes, cc.Numero, ai.TipoSaldo;
""", "sueldos_mensual_historico.csv",
    "Resumen mensual histórico sueldos 21301001/002 desde 2024")

cursor.close()
connection.close()
print("\n=== Diagnóstico completo. Revisar archivos en carpeta diagnostico/ ===")
