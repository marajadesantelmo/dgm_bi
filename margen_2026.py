# -*- coding: utf-8 -*-
"""
Generacion de margen operativo en excel
"""

import mysql.connector
import pandas as pd
from tokens import host, user, database

# Cargar cuentas a considerar desde el plan de cuentas
cuentas_contables = pd.read_excel('DGM - Cuentas Contables.xlsx')
cuentas_contables.columns = ['Numero', 'Nivel', 'Tipo', 'Descripcion', 'FechaCreacion', 'FechaModificacion', 'TipoSaldo', 'Imputable', '_', 'Considerar']
cuentas_si = cuentas_contables[cuentas_contables['Considerar'].str.upper().str.strip() == 'SI'].copy()
cuentas_si['Numero'] = cuentas_si['Numero'].apply(lambda x: str(int(float(x))) if pd.notna(x) else '').str.strip()
numeros_si = cuentas_si['Numero'].tolist()
bsas_numeros = cuentas_si[cuentas_si['Descripcion'].str.contains('BSAS|BS AS', na=False)]['Numero'].tolist()
# Salta: cuentas con etiqueta regional PAT o SALTA, EXCLUYENDO las que llevan el prefijo
# BSAS. Así "BSAS PATENTES" (que contiene el substring 'PAT') queda en Bs.As. y no en Salta,
# mientras "PAT ..." y las de PATOGENICOS/PATOLOGICOS siguen en Salta.
salta_numeros = cuentas_si[
    cuentas_si['Descripcion'].str.contains('PAT|SALTA', na=False) &
    ~cuentas_si['Descripcion'].str.contains('BSAS|BS AS', na=False)
]['Numero'].tolist()

connection = mysql.connector.connect(
    host=host, 
    user=user,
    database=database,
    charset='utf8'
)
cursor = connection.cursor()

#Ventas por mes
cursor.execute("""
SELECT 
    fi.ImportePrecio2, 
    f.FechaEmision,
    f.Tipo,
    mc.CotMoneda2,
    (1 - (f.Descuento / 100)) AS DescuentoFactura, 
    CASE 
        WHEN f.Tipo = 1 THEN - (fi.ImportePrecio2 * mc.CotMoneda2 * (1 - (f.Descuento / 100)))
        ELSE (fi.ImportePrecio2 * mc.CotMoneda2 * (1 - (f.Descuento / 100)))
    END AS `Importe`,
    CASE 
        WHEN t.NroSucursal IN (3, 8) THEN 'Bs.As.'
        WHEN t.NroSucursal = 4 THEN 'Salta'
        ELSE 'Otros'
    END AS `Unidad de Negocios`,
    e.Empresa AS Cliente,
    CONCAT(
        CASE f.Tipo
            WHEN 0 THEN 'Factura'
            WHEN 1 THEN 'Nota de Crédito'
            WHEN 2 THEN 'Nota de Débito'
            ELSE 'Comprobante'
        END,
        CASE WHEN f.TipoMultitipo IS NOT NULL
             THEN CONCAT(' (',
                 CASE f.TipoMultitipo
                     WHEN 1 THEN 'A' WHEN 2 THEN 'B' WHEN 3 THEN 'C'
                     WHEN 4 THEN 'E' WHEN 5 THEN 'M' ELSE ''
                 END, ')')
             ELSE '' END,
        ' ',
        LPAD(COALESCE(t.NroSucursal, 0), 4, '0'),
        '-',
        LPAD(COALESCE(f.Numero, 0), 8, '0')
    ) AS Detalle
FROM facturasitems fi
JOIN facturas f ON fi.IdFactura = f.RecID
LEFT JOIN monedacotizaciones mc ON f.IDCotizacionMoneda = mc.RecID
JOIN productos p ON fi.IDProducto = p.RecID
LEFT JOIN talonarios t ON f.IDTalonario = t.RecID
LEFT JOIN contactos c ON f.IDRef = c.IDContacto
LEFT JOIN empresas e ON c.IDEmpresa = e.IDEmpresa
WHERE f.Estado <> 6
AND p.Codigo NOT IN ('4.VEH.10.5 BSAS', '4.VEH.10.5')
AND f.FechaEmision >= '2024-01-01';
""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
df = pd.DataFrame(data, columns=columns)
df['Detalle'] = df['Detalle'].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)
#df.loc[df['Cliente'].str.contains('Towards', na=False), 'Unidad de Negocios'] = 'Otros'
df['Mes'] = df['FechaEmision'].dt.to_period('M')
ventas_detalle = df[['FechaEmision', 'Mes', 'Unidad de Negocios', 'Cliente', 'Importe', 'Detalle']].copy()
ventas_detalle['Concepto'] = 'Ventas netas - ' + ventas_detalle['Unidad de Negocios']
ventas_detalle['Numero'] = ''
ventas_detalle['RazonSocial'] = ventas_detalle['Cliente']
ventas_detalle['Origen'] = 'Ventas'
ventas_detalle.rename(columns={'FechaEmision': 'Fecha'}, inplace=True)
ventas_detalle = ventas_detalle[['Unidad de Negocios', 'Fecha', 'Mes', 'Concepto', 'Numero', 'Importe', 'Detalle', 'RazonSocial', 'Origen']]

#Para resumen en excel
ventas_mensual = df.groupby(['Unidad de Negocios', 'Mes'])['Importe'].sum().reset_index()
ventas_mensual['Concepto'] = 'Ventas netas - ' + ventas_mensual['Unidad de Negocios']
ventas_mensual = ventas_mensual[['Unidad de Negocios', 'Mes', 'Concepto', 'Importe']]
ventas_mensual['Numero'] = '     '

#Gastos
# Importe1 en asientositems ya está en ARS: el sistema convierte al registrar la compra.
cursor.execute("""
SELECT a.FechaCreacion, a.Fecha AS FechaContable, ai.Importe1, cc.Descripcion AS Concepto, cc.Numero, ai.TipoSaldo, a.Descripcion AS Detalle, f.RazonSocial, a.TipoOrigen
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
LEFT JOIN compras comp ON a.IDOrigen = comp.RecID
LEFT JOIN fiscal f ON comp.IDFiscal = f.RecID
WHERE a.FechaCreacion >= '2024-01-01'
""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
gastos = pd.DataFrame(data, columns=columns)
gastos['FechaCreacion'] = pd.to_datetime(gastos['FechaCreacion'])
gastos['Concepto'] = gastos['Concepto'].str.strip().str.title()

diccionario_tipos = {
    1: "Recibo",
    2: "ComprobanteFondo",
    3: "Deposito",
    4: "Pago",
    5: "ChequePropio",
    6: "ChequeTercero",
    7: "Factura",
    8: "Compra",
    9: "Asiento Resumen",
    10: "AsientoCierreResultado",
    11: "AsientoCierrePatrimonio",
    12: "Ajuste"}

# Transformar la columna completa
gastos['TipoOrigen'] = gastos['TipoOrigen'].map(diccionario_tipos)

# ══════════════════════════════════════════════════════════════════════════════
# RRHH (Sueldos + Cargas Sociales) — enfoque HÍBRIDO
#   • Meses cerrados: cuentas de EGRESO 4.2.x (cuentas de resultado, devengado por
#     a.Fecha), tomando el movimiento "base" y excluyendo ajustes por inflación,
#     provisiones que luego se reversan, y asientos de cierre de resultado.
#   • Meses recientes aún no cargados en las cuentas de resultado: fallback al
#     método patrimonial (cuentas 2.1.3 "a pagar"), como se venía haciendo.
#   El SINDICATO se fusiona dentro de Cargas Sociales (el modelo de egreso no tiene
#   una cuenta de sindicato separada).
# ══════════════════════════════════════════════════════════════════════════════
gastos['FechaContable'] = pd.to_datetime(gastos['FechaContable'])

# Cuentas de egreso RRHH (regional ya incorporado: 421=Bs.As., 422=Salta)
sueldos_eg_ctas = ['42101029', '42102023', '42201029', '42202023']   # BSAS/PAT SUELDOS (costo svc + admin)
cargas_eg_ctas  = ['42101009', '42102005', '42201009', '42202005']   # BSAS/PAT CARGAS SOCIALES (costo svc + admin)
rrhh_eg_ctas = sueldos_eg_ctas + cargas_eg_ctas

# Para las filas de RRHH la columna RazonSocial no aplica (no hay cliente/proveedor):
# se usa para indicar el ORIGEN del dato (cuenta de resultado vs. fallback patrimonial).
ORIGEN_EGRESO      = 'Cuenta de egreso (devengado)'
ORIGEN_PATRIMONIAL = 'Cuenta patrimonial (fallback)'

def _categoria_rrhh(row):
    # Clasifica cada movimiento para separar el sueldo real del "ruido" contable.
    if row['TipoOrigen'] in ('AsientoCierreResultado', 'AsientoCierrePatrimonio'):
        return 'cierre'
    det = str(row['Detalle']).upper()
    if 'INFL' in det:   return 'infl'        # ajuste por inflación
    if 'REVERS' in det: return 'reversion'   # reversión de provisión/ajuste
    if 'PROV' in det:   return 'provision'   # provisión (a veces es el sueldo real, a veces duplicado)
    return 'base'

rrhh_eg = gastos[gastos['Numero'].astype(str).isin(rrhh_eg_ctas)].copy()
rrhh_eg['Numero'] = rrhh_eg['Numero'].astype(str)
rrhh_eg = rrhh_eg[rrhh_eg['FechaContable'].notna()].copy()
rrhh_eg['Categoria'] = rrhh_eg.apply(_categoria_rrhh, axis=1)
rrhh_eg['Firma'] = rrhh_eg['Importe1'] * rrhh_eg['TipoSaldo'].map({0: 1, 1: -1})
rrhh_eg['Mes'] = rrhh_eg['FechaContable'].dt.to_period('M')
rrhh_eg['Unidad de Negocios'] = rrhh_eg['Numero'].apply(lambda n: 'Bs.As.' if n.startswith('421') else 'Salta')
rrhh_eg['Tipo'] = rrhh_eg['Numero'].apply(lambda n: 'Sueldos' if n in sueldos_eg_ctas else 'Cargas Sociales')

# Nos quedamos con 'base' y 'provision' (descartamos inflación, cierre y reversión).
# Regla por (cuenta, mes): usar la suma 'base'; si esa suma es 0, usar la 'provision'
# (mes en que la provisión ES el devengamiento real y no fue reversada).
rrhh_keep = rrhh_eg[~rrhh_eg['Categoria'].isin(['infl', 'cierre', 'reversion'])].copy()
_base_sum = (rrhh_keep[rrhh_keep['Categoria'] == 'base']
             .groupby(['Numero', 'Mes'])['Firma'].sum())
_grupos_con_base = set(_base_sum[_base_sum != 0].index)   # claves (Numero, Mes) con base real

def _usar_movimiento(row):
    clave = (row['Numero'], row['Mes'])
    if row['Categoria'] == 'base':
        return clave in _grupos_con_base
    return clave not in _grupos_con_base   # provisión: sólo si no hay base ese mes

rrhh_keep = rrhh_keep[rrhh_keep.apply(_usar_movimiento, axis=1)].copy()

# Cutoff dinámico: último mes con sueldos de egreso efectivamente cargados
cutoff = rrhh_keep.loc[rrhh_keep['Tipo'] == 'Sueldos', 'Mes'].max()

# ── Resumen y detalle EGRESO (meses entre 2024-01 y el cutoff) ───────────────
_mes_min = pd.Period('2024-01', freq='M')
rrhh_eg_cerr = rrhh_keep[(rrhh_keep['Mes'] <= cutoff) & (rrhh_keep['Mes'] >= _mes_min)].copy()
rrhh_eg_cerr['Concepto'] = rrhh_eg_cerr['Tipo'] + ' - ' + rrhh_eg_cerr['Unidad de Negocios']

rrhh_eg_mes = (rrhh_eg_cerr.groupby(['Unidad de Negocios', 'Tipo', 'Concepto', 'Mes'])['Firma']
               .sum().reset_index().rename(columns={'Firma': 'Importe'}))

rrhh_eg_det = rrhh_eg_cerr[['Unidad de Negocios', 'FechaContable', 'Mes', 'Concepto', 'Numero',
                            'Firma', 'Detalle', 'TipoOrigen']].copy()
rrhh_eg_det.rename(columns={'FechaContable': 'Fecha', 'Firma': 'Importe'}, inplace=True)
rrhh_eg_det['Origen'] = 'Sueldos'
rrhh_eg_det['RazonSocial'] = ORIGEN_EGRESO

# ── Fallback PATRIMONIAL (meses > cutoff) ────────────────────────────────────
patr = gastos[gastos['Numero'].astype(str).isin(['21301001', '21301002'])].copy()
patr['Numero'] = patr['Numero'].astype(str)
patr = patr[(patr['TipoSaldo'] == 0) & (~patr['Detalle'].str.contains('Asiento|ASTO DE', na=False))].copy()
patr['Mes'] = patr['FechaCreacion'].dt.to_period('M')
patr = patr[patr['Mes'] > cutoff].copy()
patr['Unidad de Negocios'] = patr['Numero'].map({'21301001': 'Bs.As.', '21301002': 'Salta'})

# Sueldos patrimoniales (fallback)
patr_sueldos = patr.groupby(['Unidad de Negocios', 'Mes'])['Importe1'].sum().reset_index()
patr_sueldos.rename(columns={'Importe1': 'Importe'}, inplace=True)
patr_sueldos['Tipo'] = 'Sueldos'

# Participación de sueldos por unidad para prorratear cargas + sindicato
_tot_mes = patr_sueldos.groupby('Mes')['Importe'].sum().rename('TotMes').reset_index()
_part = patr_sueldos.merge(_tot_mes, on='Mes', how='left')
_part['Participacion'] = _part['Importe'] / _part['TotMes']

# Cargas sociales + sindicato patrimoniales (fallback), prorrateados por participación
cs_ctas = ['21302001', '21302002', '21302004', '21302005', '21302006',
           '21302007', '21302008', '21302009', '21302010']
cs = gastos[gastos['Numero'].astype(str).isin(cs_ctas) & (gastos['TipoSaldo'] == 0)].copy()
cs['Mes'] = cs['FechaCreacion'].dt.to_period('M')
cs = cs[cs['Mes'] > cutoff]
cs_tot = cs.groupby('Mes')['Importe1'].sum().rename('CargasTot').reset_index()
patr_cargas = _part.merge(cs_tot, on='Mes', how='left')
patr_cargas['Importe'] = patr_cargas['CargasTot'].fillna(0) * patr_cargas['Participacion']
patr_cargas['Tipo'] = 'Cargas Sociales'
patr_cargas['Concepto'] = 'Cargas Sociales - ' + patr_cargas['Unidad de Negocios']

patr_mes = pd.concat([
    patr_sueldos[['Unidad de Negocios', 'Mes', 'Importe', 'Tipo']],
    patr_cargas[['Unidad de Negocios', 'Mes', 'Importe', 'Tipo']],
], ignore_index=True)
patr_mes['Concepto'] = patr_mes['Tipo'] + ' - ' + patr_mes['Unidad de Negocios']

# Detalle patrimonial (fallback)
patr_sueldos_det = patr[['Unidad de Negocios', 'FechaCreacion', 'Mes', 'Numero', 'Detalle', 'Importe1', 'TipoOrigen']].copy()
patr_sueldos_det.rename(columns={'FechaCreacion': 'Fecha', 'Importe1': 'Importe'}, inplace=True)
patr_sueldos_det['Concepto'] = 'Sueldos - ' + patr_sueldos_det['Unidad de Negocios']
patr_sueldos_det['Origen'] = 'Sueldos'
patr_sueldos_det['RazonSocial'] = ORIGEN_PATRIMONIAL
patr_cargas_det = patr_cargas[['Unidad de Negocios', 'Mes', 'Importe', 'Concepto']].copy()
patr_cargas_det['RazonSocial'] = ORIGEN_PATRIMONIAL

# ── Combinación EGRESO (cerrado) + PATRIMONIO (fallback) ─────────────────────
sueldos_mensual = pd.concat([
    rrhh_eg_mes[rrhh_eg_mes['Tipo'] == 'Sueldos'][['Unidad de Negocios', 'Concepto', 'Mes', 'Importe']],
    patr_mes[patr_mes['Tipo'] == 'Sueldos'][['Unidad de Negocios', 'Concepto', 'Mes', 'Importe']],
], ignore_index=True)
sueldos_mensual['Numero'] = '                 '

cargas_sociales_final = pd.concat([
    rrhh_eg_mes[rrhh_eg_mes['Tipo'] == 'Cargas Sociales'][['Unidad de Negocios', 'Concepto', 'Mes', 'Importe']],
    patr_mes[patr_mes['Tipo'] == 'Cargas Sociales'][['Unidad de Negocios', 'Concepto', 'Mes', 'Importe']],
], ignore_index=True)
cargas_sociales_final['Numero'] = '                 '

sueldos_detalle = pd.concat([
    rrhh_eg_det[rrhh_eg_det['Concepto'].str.startswith('Sueldos')],
    patr_sueldos_det,
], ignore_index=True)
sueldos_detalle = sueldos_detalle[['Unidad de Negocios', 'Fecha', 'Mes', 'Concepto', 'Numero', 'Importe', 'Detalle', 'RazonSocial', 'Origen', 'TipoOrigen']]

cargas_sociales_detalle = pd.concat([
    rrhh_eg_det[rrhh_eg_det['Concepto'].str.startswith('Cargas Sociales')][['Mes', 'Unidad de Negocios', 'Importe', 'Concepto', 'RazonSocial']],
    patr_cargas_det,
], ignore_index=True)


egresos1 = gastos[gastos['Numero'].astype(str).isin(numeros_si)]
egresos2 = gastos[gastos['Concepto'].str.contains('Mantenimiento|mantenimiento')]
egresos = pd.concat([egresos1, egresos2])
# Excluir las cuentas de sueldos/cargas de egreso: ya se computan en el bloque RRHH
# (evita doble conteo, ya que estas cuentas están marcadas 'Si' en el plan de cuentas)
egresos = egresos[~egresos['Numero'].astype(str).isin(rrhh_eg_ctas)]

egresos = egresos[
    (egresos['TipoOrigen'] == "Compra") |                                                         # Se toman los movimientos con TipoOrigen = 8 (Compras)
    ((egresos['Numero'] == '42102025') & (egresos['TipoOrigen'] == 'ComprobanteFondo'))].copy()   # Comprobantes de AySA que tienen otro TipoOrigen

egresos = egresos[~egresos['Detalle'].str.contains('INFL')].copy()
egresos.loc[ egresos['Detalle'].str.contains('Nota de Crédito|Anulación', case=False, na=False),   'Importe1'] *= -1

egresos['Mes'] = egresos["FechaCreacion"].dt.to_period("M")


egresos.loc[egresos["Numero"].astype(str).isin(bsas_numeros), "Unidad de Negocios"] = "Bs.As."
egresos.loc[egresos["Numero"].astype(str).isin(salta_numeros), "Unidad de Negocios"] = "Salta"

# Hardcode: HERNANDEZ GUSTAVO OMAR opera en Salta aunque sus cuentas son BSAS
mask_hernandez = egresos['RazonSocial'].str.upper().str.strip() == 'HERNANDEZ GUSTAVO OMAR'
egresos.loc[mask_hernandez, 'Unidad de Negocios'] = 'Salta'
egresos.loc[mask_hernandez, 'Concepto'] = egresos.loc[mask_hernandez, 'Concepto'].str.replace('Bsas ', 'Pat ', regex=False)

# Hardcode: GOBIERNO DE LA CIUDAD DE BUENOS AIRES → Salta.
# Son multas de patente de un vehículo de Salta labradas en Buenos Aires: el gasto es de
# Salta aunque la cuenta sea BSAS. Se reasigna la unidad y se reetiqueta el concepto Bsas→Pat.
# (El resto de las cuentas "BSAS PATENTES" quedan correctamente en Bs.As.)
mask_gcba = egresos['RazonSocial'].str.upper().str.strip() == 'GOBIERNO DE LA CIUDAD DE BUENOS AIRES'
egresos.loc[mask_gcba, 'Unidad de Negocios'] = 'Salta'
egresos.loc[mask_gcba, 'Concepto'] = egresos.loc[mask_gcba, 'Concepto'].str.replace('Bsas ', 'Pat ', regex=False)

egresos_mensual = egresos.groupby([ "Unidad de Negocios", "Mes", "Numero", "Concepto"])["Importe1"].sum().reset_index()
egresos_mensual.rename(columns={ 'Importe1': 'Importe'}, inplace=True)

egresos_detalle = egresos[['Unidad de Negocios',  'FechaCreacion', 'Mes', 'Numero', 'Concepto', 'Detalle', 'RazonSocial', 'Importe1', 'TipoOrigen']].copy()

egresos_detalle['Importe'] = egresos_detalle['Importe1']
#egresos_detalle['Detalle'] = egresos_detalle['Descripcion']
egresos_detalle['Origen'] = 'Compras'
egresos_detalle.rename(columns={'FechaCreacion': 'Fecha'}, inplace=True)
egresos_detalle = egresos_detalle[['Unidad de Negocios', 'Fecha', 'Mes', 'Concepto', 'Numero', 'Importe', 'Detalle', 'RazonSocial', 'Origen', 'TipoOrigen']]

datos = pd.concat([ventas_mensual, sueldos_mensual, cargas_sociales_final, egresos_mensual])
#Obtenog códigos que luego uso
codigos = datos[['Concepto', 'Numero']].drop_duplicates()

movimientos = pd.concat([
    ventas_detalle,
    sueldos_detalle,
    cargas_sociales_detalle,
    egresos_detalle
], ignore_index=True)

movimientos = movimientos[movimientos['Unidad de Negocios'].isin(['Bs.As.', 'Salta'])].copy()
movimientos.sort_values(['Unidad de Negocios', 'Fecha', 'Concepto', 'Numero'], inplace=True)

#Paso a Salta este caso en particular
movimientos.loc[movimientos['Detalle'] == "PAGO RECIBO DA", ['Unidad de Negocios', 'Concepto']] = ['Salta', '01-Otros DA']


salta = datos[datos['Unidad de Negocios'] == 'Salta']
bsas = datos[datos['Unidad de Negocios'] == 'Bs.As.']

import re

def add_prefix(concepto):
    if pd.isna(concepto):
        return concepto
    
    concepto_str = str(concepto)
    
    if re.match(r'^\d{2}-', concepto_str):  # Ya tiene prefijo
        return concepto_str
    elif "Ventas netas" in concepto_str:
        return "00-" + concepto_str
    elif any(x in concepto_str for x in ["Sueldos - ", "Sueldos Y Jornales", "Cargas Sociales", "Sindicato"]):
        return "01-" + concepto_str
    else:
        return "02-" + concepto_str

def get_grupo(concepto):
    if concepto.startswith("00-"):
        return "1.Ventas"
    elif concepto.startswith("01-"):
        return "2.RRHH"
    else:
        return "3.Egresos"

bsas['Concepto'] = bsas['Concepto'].apply(add_prefix)
salta['Concepto'] = salta['Concepto'].apply(add_prefix)

salta_movimientos = movimientos[movimientos['Unidad de Negocios'] == 'Salta'].copy()
bsas_movimientos = movimientos[movimientos['Unidad de Negocios'] == 'Bs.As.'].copy()

bsas_movimientos['Concepto'] = bsas_movimientos['Concepto'].apply(add_prefix)
salta_movimientos['Concepto'] = salta_movimientos['Concepto'].apply(add_prefix)

bsas_movimientos['Grupo'] = bsas_movimientos['Concepto'].apply(get_grupo)
salta_movimientos['Grupo'] = salta_movimientos['Concepto'].apply(get_grupo)

# Create the pivot table for P&L format
salta_cash_flow = salta.pivot_table(
    index= "Concepto", 
    columns="Mes", 
    values="Importe", 
    aggfunc="sum"
).sort_index().reset_index()

bsas_cash_flow= bsas.pivot_table(
    index= "Concepto", 
    columns="Mes", 
    values="Importe", 
    aggfunc="sum"
).sort_index().reset_index()
    
salta_cash_flow['Grupo'] = salta_cash_flow['Concepto'].apply(get_grupo)
bsas_cash_flow['Grupo'] = bsas_cash_flow['Concepto'].apply(get_grupo)


def ordenar(datos): 
    orden_conceptos = [
        "00-Ventas netas - Bs.As.", "00-Ventas netas - Salta", "00-Ventas netas - Otros",
        "01-Sueldos - Salta", "01-Sueldos - Bs.As.",
        "01-Cargas Sociales - Bs.As.", "01-Cargas Sociales - Salta"
    ]
    categorias_unicas = orden_conceptos + sorted(set(datos["Concepto"].unique()) - set(orden_conceptos))
    # Ensure 'Concepto' is a categorical variable with a predefined order
    datos["Concepto"] = pd.Categorical(
        datos["Concepto"],
        categories=categorias_unicas,
        ordered=True)
    return datos.sort_values("Concepto")

salta_cash_flow = ordenar(salta_cash_flow)
bsas_cash_flow = ordenar(bsas_cash_flow)
#Columna grupo va primero
cols = ['Grupo'] + [col for col in bsas_cash_flow.columns if col != 'Grupo']
bsas_cash_flow = bsas_cash_flow[cols]
cols = ['Grupo'] + [col for col in salta_cash_flow.columns if col != 'Grupo']
salta_cash_flow = salta_cash_flow[cols]

cols = ['Grupo'] + [col for col in bsas_movimientos.columns if col != 'Grupo']
bsas_movimientos = bsas_movimientos[cols]
cols = ['Grupo'] + [col for col in salta_movimientos.columns if col != 'Grupo']
salta_movimientos = salta_movimientos[cols]


with pd.ExcelWriter("margen_2026.xlsx", engine="openpyxl") as writer:
    bsas_cash_flow.to_excel(writer, sheet_name="Bs.As.", index=False)
    salta_cash_flow.to_excel(writer, sheet_name="Salta", index=False)
    bsas_movimientos.to_excel(writer, sheet_name="Bs.As. - movimientos", index=False)
    salta_movimientos.to_excel(writer, sheet_name="Salta - movimientos", index=False)
