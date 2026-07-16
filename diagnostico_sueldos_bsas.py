# -*- coding: utf-8 -*-
"""
DIAGNOSTICO: desagregacion de sueldos/cargas de Bs.As. en
"Consultoria y operaciones" (cuentas 42101) vs "Administracion" (cuentas 42102).

Este script NO modifica nada (solo SELECT). Correr en el server (tiene acceso al DB)
y traer el archivo 'diagnostico_sueldos_bsas.xlsx' a la PC local para analizarlo.

Objetivo: decidir como resolver los meses que caen en el fallback patrimonial
(cuentas 2.1.3 'a pagar', que NO distinguen admin/operativo):
  (A) prorratear el total patrimonial de Bs.As. por el ratio historico admin/operaciones, o
  (B) dejar esos meses sin clasificar.

Replica exactamente la logica de egreso de margen_2026.py (categoria base/provision,
exclusion de inflacion/cierre/reversion, cutoff dinamico) para que los numeros coincidan.
"""

import mysql.connector
import pandas as pd
from tokens import host, user, database

connection = mysql.connector.connect(host=host, user=user, database=database, charset='utf8')
cursor = connection.cursor()

# ── Cuentas RRHH (mismas que margen_2026.py) ─────────────────────────────────
sueldos_eg_ctas = ['42101029', '42102023', '42201029', '42202023']   # SUELDOS (op + admin), BSAS/PAT
cargas_eg_ctas  = ['42101009', '42102005', '42201009', '42202005']   # CARGAS SOCIALES (op + admin)
rrhh_eg_ctas = sueldos_eg_ctas + cargas_eg_ctas
patr_ctas = ['21301001', '21301002']                                  # a pagar: BS AS / PATOGENICOS
cs_ctas = ['21302001', '21302002', '21302004', '21302005', '21302006',
           '21302007', '21302008', '21302009', '21302010']

todas = rrhh_eg_ctas + patr_ctas + cs_ctas
in_clause = "','".join(todas)

# Mismo SELECT que margen_2026.py, restringido a las cuentas relevantes (mas liviano)
cursor.execute(f"""
SELECT a.FechaCreacion, a.Fecha AS FechaContable, ai.Importe1, cc.Descripcion AS Concepto,
       cc.Numero, ai.TipoSaldo, a.Descripcion AS Detalle, f.RazonSocial, a.TipoOrigen
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
LEFT JOIN compras comp ON a.IDOrigen = comp.RecID
LEFT JOIN fiscal f ON comp.IDFiscal = f.RecID
WHERE a.FechaCreacion >= '2024-01-01'
AND cc.Numero IN ('{in_clause}')
""")
data = cursor.fetchall()
columns = [c[0] for c in cursor.description]
gastos = pd.DataFrame(data, columns=columns)
gastos['FechaCreacion'] = pd.to_datetime(gastos['FechaCreacion'])
gastos['FechaContable'] = pd.to_datetime(gastos['FechaContable'])
gastos['Numero'] = gastos['Numero'].astype(str)

diccionario_tipos = {
    1: "Recibo", 2: "ComprobanteFondo", 3: "Deposito", 4: "Pago", 5: "ChequePropio",
    6: "ChequeTercero", 7: "Factura", 8: "Compra", 9: "Asiento Resumen",
    10: "AsientoCierreResultado", 11: "AsientoCierrePatrimonio", 12: "Ajuste"}
gastos['TipoOrigen'] = gastos['TipoOrigen'].map(diccionario_tipos)


# ══════════════════════════════════════════════════════════════════════════════
# 1) RRHH por metodo EGRESO, desagregado por 42101 (operaciones) vs 42102 (admin)
# ══════════════════════════════════════════════════════════════════════════════
def _categoria_rrhh(row):
    if row['TipoOrigen'] in ('AsientoCierreResultado', 'AsientoCierrePatrimonio'):
        return 'cierre'
    det = str(row['Detalle']).upper()
    if 'INFL' in det:   return 'infl'
    if 'REVERS' in det: return 'reversion'
    if 'PROV' in det:   return 'provision'
    return 'base'

rrhh_eg = gastos[gastos['Numero'].isin(rrhh_eg_ctas)].copy()
rrhh_eg = rrhh_eg[rrhh_eg['FechaContable'].notna()].copy()
rrhh_eg['Categoria'] = rrhh_eg.apply(_categoria_rrhh, axis=1)
rrhh_eg['Firma'] = rrhh_eg['Importe1'] * rrhh_eg['TipoSaldo'].map({0: 1, 1: -1})
rrhh_eg['Mes'] = rrhh_eg['FechaContable'].dt.to_period('M')
rrhh_eg['Unidad de Negocios'] = rrhh_eg['Numero'].apply(lambda n: 'Bs.As.' if n.startswith('421') else 'Salta')
rrhh_eg['Tipo'] = rrhh_eg['Numero'].apply(lambda n: 'Sueldos' if n in sueldos_eg_ctas else 'Cargas Sociales')
# Sub-clasificacion: 42101/42201 = costo servicio (operaciones), 42102/42202 = administracion
SUBCLASIF = {'42101': 'Consultoria y operaciones', '42102': 'Administracion',
             '42201': 'Consultoria y operaciones', '42202': 'Administracion'}
rrhh_eg['SubClasif'] = rrhh_eg['Numero'].str[:5].map(SUBCLASIF).fillna('')

# Regla base/provision por (cuenta, mes) — identica a margen_2026.py
rrhh_keep = rrhh_eg[~rrhh_eg['Categoria'].isin(['infl', 'cierre', 'reversion'])].copy()
_base_sum = (rrhh_keep[rrhh_keep['Categoria'] == 'base'].groupby(['Numero', 'Mes'])['Firma'].sum())
_grupos_con_base = set(_base_sum[_base_sum != 0].index)

def _usar_movimiento(row):
    clave = (row['Numero'], row['Mes'])
    if row['Categoria'] == 'base':
        return clave in _grupos_con_base
    return clave not in _grupos_con_base

rrhh_keep = rrhh_keep[rrhh_keep.apply(_usar_movimiento, axis=1)].copy()

# Cutoff dinamico (identico a margen_2026.py)
cutoff = rrhh_keep.loc[rrhh_keep['Tipo'] == 'Sueldos', 'Mes'].max()
_mes_min = pd.Period('2024-01', freq='M')
rrhh_eg_cerr = rrhh_keep[(rrhh_keep['Mes'] <= cutoff) & (rrhh_keep['Mes'] >= _mes_min)].copy()

# ── Hoja resumen_mensual: Bs.As. desagregado + Salta agregado ────────────────
resumen = (rrhh_eg_cerr
           .groupby(['Mes', 'Unidad de Negocios', 'Tipo', 'SubClasif'])['Firma']
           .sum().reset_index().rename(columns={'Firma': 'Importe'}))
# Etiqueta legible por columna
def _etq(r):
    base = r['Tipo']
    if r['Unidad de Negocios'] == 'Bs.As.':
        return f"{base} {r['SubClasif']} - Bs.As."
    return f"{base} - Salta"
resumen['Concepto'] = resumen.apply(_etq, axis=1)
resumen_pivot = (resumen.pivot_table(index='Mes', columns='Concepto', values='Importe', aggfunc='sum')
                 .sort_index())
resumen_pivot = resumen_pivot.round(0)

# ── Hoja ratio_bsas: participacion operaciones vs admin (Bs.As.) por mes ──────
bsas = rrhh_eg_cerr[rrhh_eg_cerr['Unidad de Negocios'] == 'Bs.As.'].copy()
ratio = (bsas.groupby(['Mes', 'Tipo', 'SubClasif'])['Firma'].sum()
         .unstack('SubClasif').fillna(0).reset_index())
# Aseguro ambas columnas
for col in ['Consultoria y operaciones', 'Administracion']:
    if col not in ratio.columns:
        ratio[col] = 0.0
ratio['Total'] = ratio['Consultoria y operaciones'] + ratio['Administracion']
ratio['% Operaciones'] = (ratio['Consultoria y operaciones'] / ratio['Total'] * 100).round(1)
ratio['% Administracion'] = (ratio['Administracion'] / ratio['Total'] * 100).round(1)
ratio = ratio.sort_values(['Tipo', 'Mes'])

# ── Hoja cutoff_y_fallback: totales patrimoniales Bs.As. y flag de fallback ───
patr = gastos[gastos['Numero'].isin(patr_ctas)].copy()
patr = patr[(patr['TipoSaldo'] == 0) & (~patr['Detalle'].astype(str).str.contains('Asiento|ASTO DE', na=False))].copy()
patr['Mes'] = patr['FechaCreacion'].dt.to_period('M')
patr['Unidad de Negocios'] = patr['Numero'].map({'21301001': 'Bs.As.', '21301002': 'Salta'})
patr_bsas = (patr[patr['Unidad de Negocios'] == 'Bs.As.']
             .groupby('Mes')['Importe1'].sum().reset_index()
             .rename(columns={'Importe1': 'Patrimonial_BsAs_Sueldos'}))

# Total egreso sueldos Bs.As. por mes (para comparar magnitudes)
egr_bsas_sueldos = (rrhh_eg_cerr[(rrhh_eg_cerr['Unidad de Negocios'] == 'Bs.As.') &
                                 (rrhh_eg_cerr['Tipo'] == 'Sueldos')]
                    .groupby('Mes')['Firma'].sum().reset_index()
                    .rename(columns={'Firma': 'Egreso_BsAs_Sueldos'}))

fallback = pd.merge(patr_bsas, egr_bsas_sueldos, on='Mes', how='outer').sort_values('Mes')
fallback['es_fallback (Mes > cutoff)'] = fallback['Mes'] > cutoff
fallback = fallback.round(0)

# ══════════════════════════════════════════════════════════════════════════════
# Salida a Excel + resumen por consola
# ══════════════════════════════════════════════════════════════════════════════
out = 'diagnostico_sueldos_bsas.xlsx'
with pd.ExcelWriter(out, engine='openpyxl') as writer:
    resumen_pivot.to_excel(writer, sheet_name='resumen_mensual')
    ratio.to_excel(writer, sheet_name='ratio_bsas', index=False)
    fallback.to_excel(writer, sheet_name='cutoff_y_fallback', index=False)

print('=' * 70)
print('DIAGNOSTICO SUELDOS BS.AS.')
print('=' * 70)
print(f'Cutoff (ultimo mes con sueldos de egreso cargados): {cutoff}')
n_fb = int((fallback['es_fallback (Mes > cutoff)'] == True).sum())
print(f'Meses en fallback patrimonial (Mes > cutoff): {n_fb}')
print(f'  -> {sorted(fallback.loc[fallback["es_fallback (Mes > cutoff)"], "Mes"].astype(str).tolist())}')
print('\n--- Ratio operaciones/admin Bs.As. (ultimos meses, para ver estabilidad) ---')
_r = ratio[ratio['Tipo'] == 'Sueldos'][['Mes', '% Operaciones', '% Administracion']].tail(12)
print(_r.to_string(index=False))
print(f'\nArchivo generado: {out}')
print('Traelo a la PC local para decidir estrategia (A) prorratear vs (B) sin clasificar.')

cursor.close()
connection.close()
