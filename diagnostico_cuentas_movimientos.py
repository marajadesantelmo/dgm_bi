# -*- coding: utf-8 -*-
"""
Diagnóstico: inventario de TODAS las cuentas contables con movimientos en Táctica.

Objetivo: alimentar el informe (informe_cuentas_contables.pdf) con una tabla, por
unidad de negocio, de las cuentas levantadas del plan de cuentas que efectivamente
tienen movimientos, con la cantidad de movimientos y otros datos de interés
(total imputado, primer/último movimiento).

Correr en el servidor (con acceso MySQL):  python diagnostico_cuentas_movimientos.py
Genera diagnostico/cuentas_movimientos.csv → commitear y hacer pull en local.
"""

import os
import mysql.connector
import pandas as pd
from tokens import host, user, database

connection = mysql.connector.connect(host=host, user=user, database=database, charset='utf8')
cursor = connection.cursor()
os.makedirs("diagnostico", exist_ok=True)

# Una fila por cuenta contable que tenga al menos un movimiento en el libro diario.
sql = """
SELECT
    cc.Numero,
    cc.Descripcion,
    COUNT(*)                                               AS Movs_Total,
    SUM(a.FechaCreacion >= '2024-01-01')                  AS Movs_2024,
    COUNT(DISTINCT a.RecID)                                AS Asientos_Total,
    MIN(a.FechaCreacion)                                  AS Primer_Mov,
    MAX(a.FechaCreacion)                                  AS Ultimo_Mov,
    SUM(CASE WHEN a.FechaCreacion >= '2024-01-01' AND ai.TipoSaldo = 0
             THEN ai.Importe1 ELSE 0 END)                 AS Debe_2024,
    SUM(CASE WHEN a.FechaCreacion >= '2024-01-01' AND ai.TipoSaldo = 1
             THEN ai.Importe1 ELSE 0 END)                 AS Haber_2024
FROM asientositems ai
JOIN asientos a          ON a.RecID  = ai.IDAsiento
JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
GROUP BY cc.Numero, cc.Descripcion
ORDER BY cc.Numero;
"""

print("=== Inventario de cuentas con movimientos ===")
cursor.execute(sql)
data = cursor.fetchall()
cols = [c[0] for c in cursor.description]
df = pd.DataFrame(data, columns=cols)
df.to_csv("diagnostico/cuentas_movimientos.csv", index=False, encoding='utf-8-sig')
print(f"  -> {len(df)} cuentas con movimientos guardadas en diagnostico/cuentas_movimientos.csv")

cursor.close()
connection.close()
print("\n=== Listo. Commitear el CSV y hacer pull en local. ===")
