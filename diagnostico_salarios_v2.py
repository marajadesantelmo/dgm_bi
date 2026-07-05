# -*- coding: utf-8 -*-
"""
Diagnóstico de SALARIOS v2 — separa el "ruido" de las cuentas de resultado 4.2.x.

Motivo: el v1 mostró que las cuentas de egreso RRHH traen, además del sueldo
devengado, otros movimientos que hay que excluir para comparar contra el enfoque
patrimonial:
  - Asientos de CIERRE de resultado (TipoOrigen 10 y 11).
  - Ajustes por INFLACION ("AJ X INFL ... CTAS DE RESULTADO") y sus reversiones.
  - PROVISIONES ("Prov ...") y sus "REVERSION PROVISIONES".
OJO: el sueldo real dice "ASIENTO DE SUELDOS ..." / "ASTO SUELDOS ...", así que NO
se puede filtrar por la palabra 'Asiento' (a diferencia de las cuentas patrimoniales).

Este script clasifica cada movimiento por Detalle y lo agrega por MES CONTABLE
(a.Fecha, el devengamiento) para poder calcular el sueldo "base" limpio.

Correr en el servidor:  python diagnostico_salarios_v2.py
Genera diagnostico/sal_egresos_categorizado.csv
"""

import os
import mysql.connector
import pandas as pd
from tokens import host, user, database

connection = mysql.connector.connect(host=host, user=user, database=database, charset='utf8')
cursor = connection.cursor()
os.makedirs("diagnostico", exist_ok=True)

egresos_rrhh = [
    '42101029', '42102023', '42101009', '42102005', '42101040', '42102030', '42103002',
    '42201029', '42202023', '42201009', '42202005', '42201040', '42202030', '42203002', '4220120',
    '42301029', '42302023', '42301009', '42302005', '42301040', '42302030', '42303002',
]
in_egresos = "', '".join(egresos_rrhh)

sql = f"""
SELECT
    DATE_FORMAT(a.Fecha, '%Y-%m') AS MesContable,
    cc.Numero,
    cc.Descripcion AS Cuenta,
    ai.TipoSaldo,
    CASE
        WHEN a.TipoOrigen IN (10, 11)                         THEN '4_cierre'
        WHEN UPPER(a.Descripcion) LIKE '%INFL%'               THEN '2_infl'
        WHEN UPPER(a.Descripcion) LIKE '%REVERS%'             THEN '3_reversion'
        WHEN UPPER(a.Descripcion) LIKE '%PROV%'               THEN '3_provision'
        ELSE '1_base'
    END AS Categoria,
    COUNT(*)         AS Cant,
    SUM(ai.Importe1) AS Total_Importe1
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE cc.Numero IN ('{in_egresos}')
AND a.FechaCreacion >= '2024-01-01'
GROUP BY MesContable, cc.Numero, cc.Descripcion, ai.TipoSaldo, Categoria
ORDER BY cc.Numero, MesContable, Categoria, ai.TipoSaldo;
"""

print("=== Categorizando egresos RRHH por Detalle ===")
cursor.execute(sql)
data = cursor.fetchall()
cols = [c[0] for c in cursor.description]
df = pd.DataFrame(data, columns=cols)
df.to_csv("diagnostico/sal_egresos_categorizado.csv", index=False, encoding='utf-8-sig')
print(f"  -> {len(df)} filas en diagnostico/sal_egresos_categorizado.csv")

# Resumen rápido en consola: total por categoria y TipoSaldo
resumen = df.groupby(['Categoria', 'TipoSaldo'])['Total_Importe1'].sum().reset_index()
print(resumen.to_string(index=False))

cursor.close()
connection.close()
print("\n=== v2 completo. Commitear el CSV y hacer pull en local. ===")
