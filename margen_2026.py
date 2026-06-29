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
salta_numeros = cuentas_si[
    (cuentas_si['Descripcion'].str.contains('PAT', na=False)) |
    (cuentas_si['Descripcion'].str.contains('SALTA', na=False) & ~cuentas_si['Descripcion'].str.contains('BSAS|BS AS', na=False))
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
SELECT a.FechaCreacion, ai.Importe1, cc.Descripcion AS Concepto, cc.Numero, ai.TipoSaldo, a.Descripcion AS Detalle, f.RazonSocial, a.TipoOrigen
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

#Filtro segun cuentas (dejo comentadas anteriores en la que busqué)
sueldos = gastos[gastos['Numero'].isin(['21301001', '21301002'])].copy()    # Esto es modulo de gastos
#sueldos = gastos[gastos['Numero'].isin(['42101029', '42101023', '42201029', '42201023', '42301029', '42301023'])].copy()    ## Esto es modulo de egresos
#sueldos = gastos[gastos['Numero'].isin(['42101029','42102023', '42201029', '42202023'])].copy()

#Tomo solo el debe
sueldos = sueldos[sueldos['TipoSaldo'] == 0].copy()
#Descarto asientos de cierre de cuentas patrimoniales (no son el devengamiento real del sueldo)
sueldos = sueldos[~sueldos['Detalle'].str.contains('Asiento|ASTO DE', na=False)].copy()

#Formato
sueldos.loc[:, 'Mes'] = sueldos['FechaCreacion'].dt.to_period('M')
sueldos.loc[sueldos['Concepto'] == 'Sueldos Y Jornales A Pagar Bs As', 'Unidad de Negocios'] = 'Bs.As.'
sueldos.loc[sueldos['Concepto'] == 'Sueldos Y Jornales A Pagar Patogenicos', 'Unidad de Negocios'] = 'Salta'
sueldos_mensual= sueldos.groupby(['Unidad de Negocios', 'Numero', 'Concepto', 'Mes'])['Importe1'].sum().reset_index()
sueldos_mensual.rename(columns={'Importe1': 'Importe'}, inplace=True)
sueldos_mensual['Numero'] = '                 '
sueldos_detalle = sueldos[['FechaCreacion', 'Unidad de Negocios', 'Mes', 'Numero', 'Concepto',  'Detalle', 'Importe1', 'TipoOrigen']].copy()
sueldos_detalle['Importe'] = sueldos_detalle['Importe1']
sueldos_detalle['Origen'] = 'Sueldos'
sueldos_detalle.rename(columns={'FechaCreacion': 'Fecha'}, inplace=True)
sueldos_detalle = sueldos_detalle[['Unidad de Negocios', 'Fecha', 'Mes', 'Concepto', 'Numero', 'Importe', 'Detalle', 'Origen', 'TipoOrigen']]

#Calculo participacion de salarios sobre total mensual por unidad de denogcio
total_mensual_sueldos = sueldos.groupby('Mes')['Importe1'].sum().reset_index()
total_mensual_sueldos.rename(columns={'Importe1': 'Total Mensual'}, inplace=True)
sueldos_mensual_unegocio = sueldos_mensual.merge(total_mensual_sueldos, on='Mes', how='left')
sueldos_mensual_unegocio.rename(columns={'Importe1': 'Importe'}, inplace=True)
sueldos_mensual_unegocio['Participacion'] = sueldos_mensual_unegocio['Importe'] / sueldos_mensual_unegocio['Total Mensual']
sueldos_mensual_unegocio = sueldos_mensual_unegocio[['Concepto', 'Mes', 'Importe', 'Participacion']]
sueldos_mensual_unegocio.loc[sueldos_mensual_unegocio['Concepto'] == 'Sueldos Y Jornales A Pagar Bs As', 'Unidad de Negocios'] = 'Bs.As.'
sueldos_mensual_unegocio.loc[sueldos_mensual_unegocio['Concepto'] == 'Sueldos Y Jornales A Pagar Patogenicos', 'Unidad de Negocios'] = 'Salta'
sueldos_mensual_unegocio = sueldos_mensual_unegocio[['Mes', 'Unidad de Negocios', 'Participacion']]

cargas_sociales = gastos[gastos['Numero'].isin(['21302001', '21302002', '21302004', '21302005', '21302006'])].copy()
cargas_sociales = cargas_sociales[cargas_sociales['TipoSaldo'] == 0]
cargas_sociales['Mes'] = cargas_sociales["FechaCreacion"].dt.to_period("M")
cargas_sociales_mensual = cargas_sociales.groupby(cargas_sociales["Mes"])["Importe1"].sum().reset_index()
cargas_sociales_mensual.columns = ['Mes', 'Cargas Sociales Total']
cargas_sociales_mensual_unegocio = sueldos_mensual_unegocio.merge(cargas_sociales_mensual, on='Mes', how='left')
cargas_sociales_mensual_unegocio['Importe'] = (cargas_sociales_mensual_unegocio['Cargas Sociales Total'] * cargas_sociales_mensual_unegocio['Participacion'])
cargas_sociales_mensual_unegocio['Concepto'] = 'Cargas Sociales - ' + cargas_sociales_mensual_unegocio['Unidad de Negocios']
cargas_sociales_final = cargas_sociales_mensual_unegocio[['Unidad de Negocios', 'Mes', 'Concepto', 'Importe']]
cargas_sociales_final['Numero'] = '                 '
cargas_sociales_detalle = cargas_sociales_final[['Mes', 'Unidad de Negocios', 'Importe', 'Concepto']]

#cargas_sociales_detalle = cargas_sociales.merge(sueldos_mensual_unegocio, on='Mes', how='left')
#cargas_sociales.rename(columns={'FechaCreacion': 'Fecha', 
#                                'Importe1': 'Importe', 
#                                'Descripcion': 'Concepto'}, inplace=True)
#cargas_sociales_detalle = cargas_sociales[['Fecha', 'Mes', 'Concepto', 'Numero', 'Importe']].copy()
#cargas_sociales_detalle['Unidad de Negocios'] = "-"#

sindicato = gastos[gastos['Numero'].isin(['21302007', '21302008', '21302009', '21302010'])].copy()
sindicato['Mes'] = sindicato["FechaCreacion"].dt.to_period("M")
sindicato_mensual = sindicato.groupby(sindicato["Mes"])["Importe1"].sum().reset_index()
sindicato_mensual.columns = ['Mes', 'Sindicato Total']
sindicato_mensual_unegocio = sueldos_mensual_unegocio.merge(sindicato_mensual, on='Mes', how='left')
sindicato_mensual_unegocio['Importe'] = (sindicato_mensual_unegocio['Sindicato Total'] * sindicato_mensual_unegocio['Participacion'])
sindicato_mensual_unegocio['Concepto'] = 'Sindicato - ' + sindicato_mensual_unegocio['Unidad de Negocios']
sindicato_final = sindicato_mensual_unegocio[['Unidad de Negocios', 'Mes', 'Concepto', 'Importe']]
sindicato_final['Numero'] = '                 '
sindicato_detalle = sindicato_final[['Mes', 'Unidad de Negocios', 'Importe', 'Concepto']]


egresos1 = gastos[gastos['Numero'].astype(str).isin(numeros_si)]
egresos2 = gastos[gastos['Concepto'].str.contains('Mantenimiento|mantenimiento')]
egresos = pd.concat([egresos1, egresos2])

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
egresos.loc[mask_hernandez, 'Concepto'] = egresos.loc[mask_hernandez, 'Concepto'].str.replace('Pat ', 'Bsas ', regex=False)

egresos_mensual = egresos.groupby([ "Unidad de Negocios", "Mes", "Numero", "Concepto"])["Importe1"].sum().reset_index()
egresos_mensual.rename(columns={ 'Importe1': 'Importe'}, inplace=True)

egresos_detalle = egresos[['Unidad de Negocios',  'FechaCreacion', 'Mes', 'Numero', 'Concepto', 'Detalle', 'RazonSocial', 'Importe1', 'TipoOrigen']].copy()

egresos_detalle['Importe'] = egresos_detalle['Importe1']
#egresos_detalle['Detalle'] = egresos_detalle['Descripcion']
egresos_detalle['Origen'] = 'Compras'
egresos_detalle.rename(columns={'FechaCreacion': 'Fecha'}, inplace=True)
egresos_detalle = egresos_detalle[['Unidad de Negocios', 'Fecha', 'Mes', 'Concepto', 'Numero', 'Importe', 'Detalle', 'RazonSocial', 'Origen', 'TipoOrigen']]

datos = pd.concat([ventas_mensual, sueldos_mensual, cargas_sociales_final, sindicato_final, egresos_mensual])
#Obtenog códigos que luego uso
codigos = datos[['Concepto', 'Numero']].drop_duplicates()

movimientos = pd.concat([
    ventas_detalle,
    sueldos_detalle,
    cargas_sociales_detalle,
    sindicato_detalle,
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
    elif any(x in concepto_str for x in ["Sueldos Y Jornales", "Cargas Sociales", "Sindicato"]):
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
        "01-Sueldos Y Jornales A Pagar Patogenicos", "01-Sueldos Y Jornales A Pagar Bs As", 
        "01-Sindicato - Bs.As.", "01-Sindicato - Salta", 
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
