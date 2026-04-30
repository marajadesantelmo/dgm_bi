# -*- coding: utf-8 -*-
"""
Generacion de cashflow en excel
"""

import mysql.connector
import pandas as pd
from tokens import host, user, database

'C:\\Users\\ceo\\Documents\\Tableros Power BI'

#dolar_cotizacion = pd.read_excel('cotizacion_dolar_procesada.xlsx') #Ver que hay que mejorar esto

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
    e.Empresa AS Cliente
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
df.loc[df['Cliente'].str.contains('Towards', na=False), 'Unidad de Negocios'] = 'Otros'
df['Mes'] = df['FechaEmision'].dt.to_period('M')
ventas_detalle = df[['FechaEmision', 'Mes', 'Unidad de Negocios', 'Cliente', 'Importe']].copy()
ventas_detalle['Concepto'] = 'Ventas netas - ' + ventas_detalle['Unidad de Negocios']
ventas_detalle['Numero'] = ''
ventas_detalle['Detalle'] = ventas_detalle['Cliente']
ventas_detalle['Origen'] = 'Ventas'
ventas_detalle.rename(columns={'FechaEmision': 'Fecha'}, inplace=True)
ventas_detalle = ventas_detalle[['Unidad de Negocios', 'Fecha', 'Mes', 'Concepto', 'Numero', 'Importe', 'Detalle', 'Origen']]
ventas_mensual = df.groupby(['Unidad de Negocios', 'Mes'])['Importe'].sum().reset_index()
ventas_mensual['Concepto'] = 'Ventas netas - ' + ventas_mensual['Unidad de Negocios']
ventas_mensual = ventas_mensual[['Unidad de Negocios', 'Mes', 'Concepto', 'Importe']]
ventas_mensual['Numero'] = '     '

#Gastos
cursor.execute("""
SELECT a.FechaCreacion, ai.Importe1, cc.Descripcion, cc.Numero
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE a.FechaCreacion >= '2024-01-01'
""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
gastos = pd.DataFrame(data, columns=columns)
gastos['FechaCreacion'] = pd.to_datetime(gastos['FechaCreacion'])
gastos['Descripcion'] = gastos['Descripcion'].str.strip().str.title()

sueldos = gastos[gastos['Numero'].isin(['21301001', '21301002'])].copy()
sueldos.loc[:, 'Mes'] = sueldos['FechaCreacion'].dt.to_period('M')
sueldos_mensual= sueldos.groupby(['Numero', 'Descripcion', 'Mes'])['Importe1'].sum().reset_index()
sueldos_mensual.columns = ['Numero', 'Concepto', 'Mes', 'Importe']
sueldos_mensual.loc[sueldos_mensual['Concepto'] == 'Sueldos Y Jornales A Pagar Bs As', 'Unidad de Negocios'] = 'Bs.As.'
sueldos_mensual.loc[sueldos_mensual['Concepto'] == 'Sueldos Y Jornales A Pagar Patogenicos', 'Unidad de Negocios'] = 'Salta'
sueldos_detalle = sueldos[['FechaCreacion', 'Mes', 'Numero', 'Descripcion', 'Importe1']].copy()
sueldos_detalle.loc[sueldos_detalle['Descripcion'] == 'Sueldos Y Jornales A Pagar Bs As', 'Unidad de Negocios'] = 'Bs.As.'
sueldos_detalle.loc[sueldos_detalle['Descripcion'] == 'Sueldos Y Jornales A Pagar Patogenicos', 'Unidad de Negocios'] = 'Salta'
sueldos_detalle['Concepto'] = sueldos_detalle['Descripcion']
sueldos_detalle['Importe'] = sueldos_detalle['Importe1']
sueldos_detalle['Detalle'] = sueldos_detalle['Descripcion']
sueldos_detalle['Origen'] = 'Sueldos'
sueldos_detalle.rename(columns={'FechaCreacion': 'Fecha'}, inplace=True)
sueldos_detalle = sueldos_detalle[['Unidad de Negocios', 'Fecha', 'Mes', 'Concepto', 'Numero', 'Importe', 'Detalle', 'Origen']]

#Calculo participacion de salarios sobre total mensual por unidad de denogcio
total_mensual_sueldos = sueldos.groupby('Mes')['Importe1'].sum().reset_index()
total_mensual_sueldos.rename(columns={'Importe1': 'Total Mensual'}, inplace=True)
sueldos_mensual_unegocio = sueldos_mensual.merge(total_mensual_sueldos, on='Mes', how='left')
sueldos_mensual_unegocio['Participacion'] = sueldos_mensual_unegocio['Importe'] / sueldos_mensual_unegocio['Total Mensual']
sueldos_mensual_unegocio = sueldos_mensual_unegocio[['Concepto', 'Mes', 'Importe', 'Participacion']]
sueldos_mensual_unegocio.loc[sueldos_mensual_unegocio['Concepto'] == 'Sueldos Y Jornales A Pagar Bs As', 'Unidad de Negocios'] = 'Bs.As.'
sueldos_mensual_unegocio.loc[sueldos_mensual_unegocio['Concepto'] == 'Sueldos Y Jornales A Pagar Patogenicos', 'Unidad de Negocios'] = 'Salta'
sueldos_mensual_unegocio = sueldos_mensual_unegocio[['Mes', 'Unidad de Negocios', 'Participacion']]

cargas_sociales = gastos[gastos['Numero'].isin(['21302001', '21302002', '21302004', '21302005', '21302006'])].copy()
cargas_sociales['Mes'] = cargas_sociales["FechaCreacion"].dt.to_period("M")
cargas_sociales_mensual = cargas_sociales.groupby(cargas_sociales["Mes"])["Importe1"].sum().reset_index()
cargas_sociales_mensual.columns = ['Mes', 'Cargas Sociales Total']
cargas_sociales_mensual_unegocio = sueldos_mensual_unegocio.merge(cargas_sociales_mensual, on='Mes', how='left')
cargas_sociales_mensual_unegocio['Importe'] = (cargas_sociales_mensual_unegocio['Cargas Sociales Total'] * cargas_sociales_mensual_unegocio['Participacion'])
cargas_sociales_mensual_unegocio['Concepto'] = 'Cargas Sociales - ' + cargas_sociales_mensual_unegocio['Unidad de Negocios']
cargas_sociales_final = cargas_sociales_mensual_unegocio[['Unidad de Negocios', 'Mes', 'Concepto', 'Importe']]
cargas_sociales_final['Numero'] = '                 '
cargas_sociales_detalle = cargas_sociales.merge(sueldos_mensual_unegocio, on='Mes', how='left')
cargas_sociales_detalle['Concepto'] = 'Cargas Sociales - ' + cargas_sociales_detalle['Unidad de Negocios']
cargas_sociales_detalle['Importe'] = cargas_sociales_detalle['Importe1'] * cargas_sociales_detalle['Participacion']
cargas_sociales_detalle['Detalle'] = cargas_sociales_detalle['Descripcion']
cargas_sociales_detalle['Origen'] = 'Cargas Sociales'
cargas_sociales_detalle.rename(columns={'FechaCreacion': 'Fecha'}, inplace=True)
cargas_sociales_detalle = cargas_sociales_detalle[['Unidad de Negocios', 'Fecha', 'Mes', 'Concepto', 'Numero', 'Importe', 'Detalle', 'Origen']]

sindicato = gastos[gastos['Numero'].isin(['21302007', '21302008', '21302009', '21302010'])].copy()
sindicato['Mes'] = sindicato["FechaCreacion"].dt.to_period("M")
sindicato_mensual = sindicato.groupby(sindicato["Mes"])["Importe1"].sum().reset_index()
sindicato_mensual.columns = ['Mes', 'Sindicato Total']

sindicato_mensual_unegocio = sueldos_mensual_unegocio.merge(sindicato_mensual, on='Mes', how='left')
sindicato_mensual_unegocio['Importe'] = (sindicato_mensual_unegocio['Sindicato Total'] * sindicato_mensual_unegocio['Participacion'])
sindicato_mensual_unegocio['Concepto'] = 'Sindicato - ' + sindicato_mensual_unegocio['Unidad de Negocios']
sindicato_final = sindicato_mensual_unegocio[['Unidad de Negocios', 'Mes', 'Concepto', 'Importe']]
sindicato_final['Numero'] = '                 '
sindicato_detalle = sindicato.merge(sueldos_mensual_unegocio, on='Mes', how='left')
sindicato_detalle['Concepto'] = 'Sindicato - ' + sindicato_detalle['Unidad de Negocios']
sindicato_detalle['Importe'] = sindicato_detalle['Importe1'] * sindicato_detalle['Participacion']
sindicato_detalle['Detalle'] = sindicato_detalle['Descripcion']
sindicato_detalle['Origen'] = 'Sindicato'
sindicato_detalle.rename(columns={'FechaCreacion': 'Fecha'}, inplace=True)
sindicato_detalle = sindicato_detalle[['Unidad de Negocios', 'Fecha', 'Mes', 'Concepto', 'Numero', 'Importe', 'Detalle', 'Origen']]



egresos1 = gastos[gastos['Numero'].isin(['42201010', '42101010', '42101056', '42201031', '42101001', '42201001', '11504001', '11501001', '42201041' ])]
egresos2 = gastos[gastos['Descripcion'].str.contains('Mantenimiento|mantenimiento')]
egresos = pd.concat([egresos1, egresos2])

egresos['Mes'] = egresos["FechaCreacion"].dt.to_period("M")
egresos_mensual = egresos.groupby(["Mes", "Numero", "Descripcion"])["Importe1"].sum().reset_index()
bsas_numeros = ["11501001", "42101001", "42101010", "42101056", "42101036", "42103011", "11504001"]
salta_numeros = ["42201031", "42201001", "42201041", "42201010", "42201056", "42201036"]
egresos_mensual.loc[egresos_mensual["Numero"].astype(str).isin(bsas_numeros), "Unidad de Negocios"] = "Bs.As."
egresos_mensual.loc[egresos_mensual["Numero"].astype(str).isin(salta_numeros), "Unidad de Negocios"] = "Salta"

egresos_mensual.columns = ['Mes', 'Numero', 'Concepto', 'Importe', 'Unidad de Negocios']
egresos_detalle = egresos[['FechaCreacion', 'Numero', 'Descripcion', 'Importe1']].copy()
egresos_detalle['Mes'] = egresos['Mes']
egresos_detalle.loc[egresos_detalle['Numero'].astype(str).isin(bsas_numeros), 'Unidad de Negocios'] = 'Bs.As.'
egresos_detalle.loc[egresos_detalle['Numero'].astype(str).isin(salta_numeros), 'Unidad de Negocios'] = 'Salta'
egresos_detalle['Concepto'] = egresos_detalle['Descripcion']
egresos_detalle['Importe'] = egresos_detalle['Importe1']
egresos_detalle['Detalle'] = egresos_detalle['Descripcion']
egresos_detalle['Origen'] = 'Compras'
egresos_detalle.rename(columns={'FechaCreacion': 'Fecha'}, inplace=True)
egresos_detalle = egresos_detalle[['Unidad de Negocios', 'Fecha', 'Mes', 'Concepto', 'Numero', 'Importe', 'Detalle', 'Origen']]

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



salta = datos[datos['Unidad de Negocios'] == 'Salta']
bsas = datos[datos['Unidad de Negocios'] == 'Bs.As.']
salta_movimientos = movimientos[movimientos['Unidad de Negocios'] == 'Salta'].copy()
bsas_movimientos = movimientos[movimientos['Unidad de Negocios'] == 'Bs.As.'].copy()

# Create the pivot table for P&L format
salta_cash_flow = salta.pivot_table(
    index= "Concepto", 
    columns="Mes", 
    values="Importe", 
    aggfunc="sum"
).sort_index().reset_index().merge(codigos, on= "Concepto")

bsas_cash_flow= bsas.pivot_table(
    index= "Concepto", 
    columns="Mes", 
    values="Importe", 
    aggfunc="sum"
).sort_index().reset_index().merge(codigos, on= "Concepto")

salta_cash_flow = salta_cash_flow[['Numero'] + [col for col in salta_cash_flow.columns if col != 'Numero']]
bsas_cash_flow = bsas_cash_flow[['Numero'] + [col for col in bsas_cash_flow.columns if col != 'Numero']]


def ordenar(datos): 
    orden_conceptos = [
        "Ventas netas - Bs.As.", "Ventas netas - Salta", "Ventas netas - Otros",
        "Sueldos Y Jornales A Pagar Patogenicos", "Sueldos Y Jornales A Pagar Bs As", 
        "Sindicato - Bs.As.", "Sindicato - Salta", 
        "Cargas Sociales - Bs.As.", "Cargas Sociales - Salta"
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

with pd.ExcelWriter("cash_flow2026.xlsx", engine="openpyxl") as writer:
    bsas_cash_flow.to_excel(writer, sheet_name="Bs.As.", index=False)
    salta_cash_flow.to_excel(writer, sheet_name="Salta", index=False)
    bsas_movimientos.to_excel(writer, sheet_name="Bs.As. - movimientos", index=False)
    salta_movimientos.to_excel(writer, sheet_name="Salta - movimientos", index=False)
