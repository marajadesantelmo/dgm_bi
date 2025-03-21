# -*- coding: utf-8 -*-
"""
Generacion de cashflow en excel
"""

import mysql.connector
import pandas as pd
from tokens import host, user, database

'C:\\Users\\ceo\\Documents\\Tableros Power BI'

dolar_cotizacion = pd.read_excel('cotizacion_dolar_procesada.xlsx') #Ver que hay que mejorar esto

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
gastos['Descripcion'] = gastos['Descripcion'].str.strip().str.title()

sueldos = gastos[gastos['Numero'].isin(['21301001', '21301002'])]
sueldos.loc[:, 'FechaCreacion'] = pd.to_datetime(sueldos['FechaCreacion'])
sueldos.loc[:, 'Mes'] = sueldos['FechaCreacion'].dt.to_period('M')
sueldos_mensual= sueldos.groupby(['Numero', 'Descripcion', 'Mes'])['Importe1'].sum().reset_index()
sueldos_mensual.columns = ['Numero', 'Concepto', 'Mes', 'Importe']
sueldos_mensual.loc[sueldos_mensual['Concepto'] == 'Sueldos Y Jornales A Pagar Bs As', 'Unidad de Negocios'] = 'Bs.As.'
sueldos_mensual.loc[sueldos_mensual['Concepto'] == 'Sueldos Y Jornales A Pagar Patogenicos', 'Unidad de Negocios'] = 'Salta'

#Calculo participacion de salarios sobre total mensual por unidad de denogcio
total_mensual_sueldos = sueldos.groupby('Mes')['Importe1'].sum().reset_index()
total_mensual_sueldos.rename(columns={'Importe1': 'Total Mensual'}, inplace=True)
sueldos_mensual_unegocio = sueldos_mensual.merge(total_mensual_sueldos, on='Mes', how='left')
sueldos_mensual_unegocio['Participacion'] = sueldos_mensual_unegocio['Importe'] / sueldos_mensual_unegocio['Total Mensual']
sueldos_mensual_unegocio = sueldos_mensual_unegocio[['Concepto', 'Mes', 'Importe', 'Participacion']]
sueldos_mensual_unegocio.loc[sueldos_mensual_unegocio['Concepto'] == 'Sueldos Y Jornales A Pagar Bs As', 'Unidad de Negocios'] = 'Bs.As.'
sueldos_mensual_unegocio.loc[sueldos_mensual_unegocio['Concepto'] == 'Sueldos Y Jornales A Pagar Patogenicos', 'Unidad de Negocios'] = 'Salta'
sueldos_mensual_unegocio = sueldos_mensual_unegocio[['Mes', 'Unidad de Negocios', 'Participacion']]

cargas_sociales = gastos[gastos['Numero'].isin(['21302001', '21302002', '21302004', '21302005', '21302006'])]
cargas_sociales['Mes'] = cargas_sociales["FechaCreacion"].dt.to_period("M")
cargas_sociales_mensual = cargas_sociales.groupby(cargas_sociales["Mes"])["Importe1"].sum().reset_index()
cargas_sociales_mensual.columns = ['Mes', 'Cargas Sociales Total']
cargas_sociales_mensual_unegocio = sueldos_mensual_unegocio.merge(cargas_sociales_mensual, on='Mes', how='left')
cargas_sociales_mensual_unegocio['Importe'] = (cargas_sociales_mensual_unegocio['Cargas Sociales Total'] * cargas_sociales_mensual_unegocio['Participacion'])
cargas_sociales_mensual_unegocio['Concepto'] = 'Cargas Sociales - ' + cargas_sociales_mensual_unegocio['Unidad de Negocios']
cargas_sociales_final = cargas_sociales_mensual_unegocio[['Unidad de Negocios', 'Mes', 'Concepto', 'Importe']]
cargas_sociales_final['Numero'] = '                 '

sindicato = gastos[gastos['Numero'].isin(['21302007', '21302008', '21302009', '21302010'])]
sindicato['Mes'] = sindicato["FechaCreacion"].dt.to_period("M")
sindicato_mensual = sindicato.groupby(sindicato["Mes"])["Importe1"].sum().reset_index()
sindicato_mensual.columns = ['Mes', 'Sindicato Total']

sindicato_mensual_unegocio = sueldos_mensual_unegocio.merge(sindicato_mensual, on='Mes', how='left')
sindicato_mensual_unegocio['Importe'] = (sindicato_mensual_unegocio['Sindicato Total'] * sindicato_mensual_unegocio['Participacion'])
sindicato_mensual_unegocio['Concepto'] = 'Sindicato - ' + sindicato_mensual_unegocio['Unidad de Negocios']
sindicato_final = sindicato_mensual_unegocio[['Unidad de Negocios', 'Mes', 'Concepto', 'Importe']]
sindicato_final['Numero'] = '                 '



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

datos = pd.concat([ventas_mensual, sueldos_mensual, cargas_sociales_final, sindicato_final, egresos_mensual])
#Obtenog códigos que luego uso
codigos = datos[['Concepto', 'Numero']].drop_duplicates()

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
    ordered=True
)

# Create the pivot table for P&L format
cash_flow = datos.pivot_table(
    index= "Concepto", 
    columns="Mes", 
    values="Importe", 
    aggfunc="sum"
).sort_index()

# Convertir el índice en columna y resetearlo
cash_flow = cash_flow.reset_index()

cash_flow = cash_flow.merge(codigos, on= "Concepto")

cash_flow = cash_flow[['Numero'] + [col for col in cash_flow.columns if col != 'Numero']]

cash_flow.to_excel('ver_cashflow.xlsx')