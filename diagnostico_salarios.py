# -*- coding: utf-8 -*-
"""
Diagnóstico de SALARIOS desde cuentas de EGRESO (4.2.x) vs. PATRIMONIO (2.1.3).

Contexto (pedido del cliente):
  Hoy `margen_2026.py` estima el gasto salarial desde cuentas PATRIMONIALES
  ("...A PAGAR", grupo 2.1.3), que son de pasivo, no de resultado. El cliente
  pide tomarlo desde cuentas de EGRESO (resultado), p.ej. 4.2.1.01.029
  (BSAS SUELDOS). Los totales deberían ser parecidos; la diferencia principal
  sería la FECHA DE IMPUTACIÓN (devengamiento vs. pago del pasivo).

Objetivo de este script:
  1. Confirmar que las cuentas de egreso RRHH (4.2.x) EXISTEN y tienen movimientos.
  2. Traer totales mensuales para compararlos contra el enfoque patrimonial actual.
  3. Revelar el TipoOrigen con que se imputan (¿Compra? ¿Asiento?) -> define el filtro
     y evita doble conteo con el bloque de egresos genéricos de margen_2026.py.
  4. Comparar Fecha (contable/imputación) vs FechaCreacion (carga) para dimensionar
     el corrimiento de fecha.

Cómo usar (en el servidor de la empresa, con acceso MySQL):
    python diagnostico_salarios.py
Genera CSVs en la carpeta diagnostico/. Commitear esos CSVs y hacer pull en local
para el análisis.
"""

import os
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

os.makedirs("diagnostico", exist_ok=True)


def run_query(sql, filename, description):
    print(f"\n=== {description} ===")
    try:
        cursor.execute(sql)
        data = cursor.fetchall()
        cols = [c[0] for c in cursor.description]
        df = pd.DataFrame(data, columns=cols)
        df.to_csv(f"diagnostico/{filename}", index=False, encoding='utf-8-sig')
        print(f"  -> {len(df)} filas guardadas en diagnostico/{filename}")
        if len(df) <= 25:
            print(df.to_string())
    except Exception as e:
        print(f"  ERROR: {e}")


# ── Listas de cuentas ────────────────────────────────────────────────────────
# EGRESOS RRHH (propuesto). Regional ya viene en la cuenta: BSAS / PAT / BIO.
egresos_rrhh = [
    # Bs.As. (4.2.1)
    '42101029',  # BSAS SUELDOS (costo del servicio)   <-- 4.2.1.01.029 (la que citó el cliente)
    '42102023',  # BSAS SUELDOS (administración)
    '42101009',  # BSAS CARGAS SOCIALES (costo servicio)
    '42102005',  # BSAS CARGAS SOCIALES (administración)
    '42101040',  # BSAS OTROS GASTOS PERSONAL (costo servicio)
    '42102030',  # BSAS OTROS GASTOS PERSONAL (administración)
    '42103002',  # BSAS CAPACITACION DEL PERSONAL
    # Salta / PAT (4.2.2)
    '42201029',  # PAT SUELDOS (costo servicio)
    '42202023',  # PAT SUELDOS (administración)
    '42201009',  # PAT CARGAS SOCIALES (costo servicio)
    '42202005',  # PAT CARGAS SOCIALES (administración)
    '42201040',  # PAT OTROS GASTOS PERSONAL (costo servicio)
    '42202030',  # PAT OTROS GASTOS PERSONAL (administración)
    '42203002',  # PAT CAPACITACION DEL PERSONAL
    '4220120',   # PAT INDEMNIZACIONES JUDICIALES (4.2.2.01.20)
    # BIO (4.2.3) - informativo, hoy no se reporta como unidad
    '42301029', '42302023', '42301009', '42302005',
    '42301040', '42302030', '42303002',
]

# PATRIMONIALES (enfoque actual). Grupo 2.1.3.
patrimonio_rrhh = [
    '21301001', '21301002', '21301003', '21301030',            # remuneraciones a pagar
    '21302001', '21302002', '21302003', '21302004', '21302005',
    '21302006', '21302007', '21302008', '21302009', '21302010',
    '21302011', '21302012', '21302013',                        # cargas sociales / sindicato
]

in_egresos = "', '".join(egresos_rrhh)
in_patrim = "', '".join(patrimonio_rrhh)


# ── 1. EGRESOS RRHH: totales mensuales por cuenta, TipoSaldo, TipoOrigen ──────
# Se agrupa por MES CONTABLE (a.Fecha) y MES DE CARGA (a.FechaCreacion) a la vez:
# cuando difieren, la fila se parte y eso mismo muestra el corrimiento de fecha.
run_query(f"""
SELECT
    DATE_FORMAT(a.Fecha, '%Y-%m')         AS MesContable,
    DATE_FORMAT(a.FechaCreacion, '%Y-%m') AS MesCreacion,
    cc.Numero,
    cc.Descripcion                        AS Cuenta,
    ai.TipoSaldo,
    a.TipoOrigen,
    COUNT(*)                              AS Cant,
    SUM(ai.Importe1)                      AS Total_Importe1
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE cc.Numero IN ('{in_egresos}')
AND a.FechaCreacion >= '2024-01-01'
GROUP BY MesContable, MesCreacion, cc.Numero, cc.Descripcion, ai.TipoSaldo, a.TipoOrigen
ORDER BY cc.Numero, MesCreacion, ai.TipoSaldo;
""", "sal_egresos_mensual.csv",
    "EGRESOS RRHH (4.2.x): total mensual por cuenta / TipoSaldo / TipoOrigen")


# ── 2. PATRIMONIO RRHH: totales mensuales, mismo formato para comparar ────────
# Se separa la parte "operativa" de los asientos de cierre (Detalle con
# 'Asiento'/'ASTO DE'), que es exactamente el filtro que aplica margen_2026.py.
run_query(f"""
SELECT
    DATE_FORMAT(a.Fecha, '%Y-%m')         AS MesContable,
    DATE_FORMAT(a.FechaCreacion, '%Y-%m') AS MesCreacion,
    cc.Numero,
    cc.Descripcion                        AS Cuenta,
    ai.TipoSaldo,
    a.TipoOrigen,
    COUNT(*)                              AS Cant,
    SUM(ai.Importe1)                      AS Total_Importe1,
    SUM(CASE WHEN (a.Descripcion LIKE '%Asiento%' OR a.Descripcion LIKE '%ASTO DE%')
             THEN ai.Importe1 ELSE 0 END) AS Importe_cierre,
    SUM(CASE WHEN NOT (a.Descripcion LIKE '%Asiento%' OR a.Descripcion LIKE '%ASTO DE%')
             THEN ai.Importe1 ELSE 0 END) AS Importe_operativo
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE cc.Numero IN ('{in_patrim}')
AND a.FechaCreacion >= '2024-01-01'
GROUP BY MesContable, MesCreacion, cc.Numero, cc.Descripcion, ai.TipoSaldo, a.TipoOrigen
ORDER BY cc.Numero, MesCreacion, ai.TipoSaldo;
""", "sal_patrimonio_mensual.csv",
    "PATRIMONIO RRHH (2.1.3): total mensual por cuenta / TipoSaldo / TipoOrigen")


# ── 3. Resumen compacto: ¿con qué TipoOrigen se imputan los egresos RRHH? ─────
# Define el filtro de extracción y si hay riesgo de doble conteo con egresos.
run_query(f"""
SELECT
    a.TipoOrigen,
    ai.TipoSaldo,
    COUNT(*)          AS Cant,
    COUNT(DISTINCT a.RecID) AS Asientos,
    SUM(ai.Importe1)  AS Total_Importe1
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE cc.Numero IN ('{in_egresos}')
AND a.FechaCreacion >= '2024-01-01'
GROUP BY a.TipoOrigen, ai.TipoSaldo
ORDER BY a.TipoOrigen, ai.TipoSaldo;
""", "sal_egresos_tipoorigen.csv",
    "EGRESOS RRHH: distribución por TipoOrigen y TipoSaldo (¿Compra? ¿Asiento?)")


# ── 4. Muestra de asientos crudos de las cuentas de SUELDOS de egreso ─────────
# Para ver el texto de Detalle (patrones de cierre/anulación) y si trae proveedor.
run_query("""
SELECT
    a.Fecha, a.FechaCreacion, a.TipoOrigen,
    ai.TipoSaldo, ai.Importe1,
    cc.Numero, cc.Descripcion AS Cuenta,
    a.Descripcion AS Detalle,
    f.RazonSocial
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
LEFT JOIN compras comp ON a.IDOrigen = comp.RecID
LEFT JOIN fiscal f ON comp.IDFiscal = f.RecID
WHERE cc.Numero IN ('42101029', '42102023', '42201029', '42202023')
AND a.FechaCreacion >= '2025-01-01'
ORDER BY a.FechaCreacion
LIMIT 120;
""", "sal_egresos_muestra.csv",
    "Muestra cruda de asientos de SUELDOS de egreso (42x01029 / 42x02023) desde 2025")


# ── 5. Comparación directa mensual Sueldos: EGRESO vs PATRIMONIO (por unidad) ──
# Egreso: suma de cuentas de sueldo por unidad (BSAS/PAT), TipoSaldo=0 (Debe).
# Patrimonio: 21301001/002, Debe, excluyendo asientos de cierre (= lógica actual).
run_query("""
SELECT
    DATE_FORMAT(a.Fecha, '%Y-%m')         AS MesContable,
    DATE_FORMAT(a.FechaCreacion, '%Y-%m') AS MesCreacion,
    CASE
        WHEN cc.Numero IN ('42101029','42102023') THEN 'Bs.As.'
        WHEN cc.Numero IN ('42201029','42202023') THEN 'Salta'
    END AS Unidad,
    SUM(CASE WHEN ai.TipoSaldo = 0 THEN ai.Importe1 ELSE 0 END) AS Sueldos_Debe,
    SUM(CASE WHEN ai.TipoSaldo = 1 THEN ai.Importe1 ELSE 0 END) AS Sueldos_Haber
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE cc.Numero IN ('42101029','42102023','42201029','42202023')
AND a.FechaCreacion >= '2024-01-01'
GROUP BY MesContable, MesCreacion, Unidad
ORDER BY MesCreacion, Unidad;
""", "sal_egresos_sueldos_por_unidad.csv",
    "Sueldos de EGRESO por mes y unidad (Debe/Haber) - para comparar con patrimonio")


cursor.close()
connection.close()
print("\n=== Diagnóstico de salarios completo. Revisar CSVs en diagnostico/ ===")
