# -*- coding: utf-8 -*-
"""
Generacion de cashflow en excel
"""

import mysql.connector
import pandas as pd
from tokens import host, user, database

#'C:\\Users\\ceo\\Documents\\Tableros Power BI'

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
AND f.FechaEmision >= '2025-01-01';
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

ventas_mensual = (
    ventas_mensual.pivot_table(
        index=["Numero", "Concepto"], 
        columns="Mes",
        values="Importe",
        aggfunc="sum"
    )
    .reset_index()
)


#### Ejemplo con detalle para Mayo 2025
cursor.execute("""
SELECT 
    cc.Numero,
    cc.Descripcion AS DescripcionCtaCtble,
    a.Descripcion AS DescripcionAsiento,
    ai.Tipo,
    ai.Importe1,
    a.Fecha
FROM asientositems ai
LEFT JOIN asientos a 
    ON ai.IDAsiento = a.RecID
LEFT JOIN cuentascontables cc 
    ON ai.IDCuentaContable = cc.RecID
WHERE a.Fecha >= '2025-01-01'
  AND cc.Numero > 42100000;
""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
gastos = pd.DataFrame(data, columns=columns)
gastos['DescripcionCtaCtble'] = gastos['DescripcionCtaCtble'].str.title()
gastos = gastos.loc[:, ~gastos.columns.duplicated()]
gastos['Fecha']= pd.to_datetime(gastos['Fecha'])
gastos.loc[:, 'Mes'] = gastos['Fecha'].dt.to_period('M')
gastos['Fecha'] = gastos['Fecha'].dt.strftime("%d/%m/%Y")
gastos = gastos[~gastos['DescripcionAsiento'].str.contains('Asiento de cierre')]
gastos = gastos[~gastos['DescripcionCtaCtble'].str.contains('Amortizaciones')]
gastos = gastos[~gastos['DescripcionCtaCtble'].str.contains('Armotizaciones')]
gastos = gastos[gastos['Numero'] != "42105001"] #Bsas Diferencia De Cambio
gastos = gastos[gastos['Numero'] != "42301016"] #Bio

#Detalle mayo
mayo_2025= gastos[gastos['Mes'] == '2025-05']
mayo_2025['DescripcionCtaCtble'] = mayo_2025['DescripcionCtaCtble'].str.title()
mayo_2025_bsas= mayo_2025[mayo_2025['DescripcionCtaCtble'].str.contains('Bs.As.|Bs As|Bsas')]
mayo_2025_bsas_42101030 = mayo_2025_bsas[mayo_2025_bsas['Numero'] == '42101030']

cash_flow_gral = gastos.pivot_table(
    index= ["Numero", "DescripcionCtaCtble"],
    columns="Mes", 
    values="Importe1", 
    aggfunc="sum"
).sort_index().reset_index()

cash_flow_gral = cash_flow_gral[['Numero'] + [col for col in cash_flow_gral.columns if col != 'Numero']]
cash_flow_gral = cash_flow_gral.rename(columns={"DescripcionCtaCtble": "Concepto"})

cash_flow_gral = pd.concat([ventas_mensual, cash_flow_gral], ignore_index=True)

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

cash_flow_gral = ordenar(cash_flow_gral)
cash_flow_bsas = cash_flow_gral[cash_flow_gral['Concepto'].str.contains('Bs.As.|Bs As|Bsas|Ventas netas - Otros')]
cash_flow_salta = cash_flow_gral[~cash_flow_gral['Concepto'].str.contains('Bs.As.|Bs As|Bsas|Ventas netas - Otros')]

#Calculo resultado

month_cols = cash_flow_gral.columns[2:]
resultado = cash_flow_gral.iloc[:2][month_cols].sum() - cash_flow_gral.iloc[2:][month_cols].sum()
resultado_row = pd.DataFrame([["", "Resultado", *resultado.values]], columns=["Numero", "Concepto", *month_cols])
cash_flow_gral = pd.concat([cash_flow_gral, resultado_row], ignore_index=True)

month_cols = cash_flow_bsas.columns[2:]
resultado = cash_flow_bsas.iloc[:2][month_cols].sum() - cash_flow_bsas.iloc[2:][month_cols].sum()
resultado_row = pd.DataFrame([["", "Resultado", *resultado.values]], columns=["Numero", "Concepto", *month_cols])
cash_flow_bsas = pd.concat([cash_flow_bsas, resultado_row], ignore_index=True)

month_cols = cash_flow_salta.columns[2:]
resultado = cash_flow_salta.iloc[:2][month_cols].sum() - cash_flow_salta.iloc[2:][month_cols].sum()
resultado_row = pd.DataFrame([["", "Resultado", *resultado.values]], columns=["Numero", "Concepto", *month_cols])
cash_flow_salta = pd.concat([cash_flow_salta, resultado_row], ignore_index=True)

with pd.ExcelWriter("Margen Operativo DGM2025-2.xlsx", engine="openpyxl") as writer:
    cash_flow_bsas.to_excel(writer, sheet_name="Bs.As.", index=False)
    cash_flow_salta.to_excel(writer, sheet_name="Salta", index=False)
    mayo_2025_bsas.to_excel(writer, sheet_name="Mayo 2025 - BsAs", index=False)
    mayo_2025_bsas_42101030.to_excel(writer, sheet_name="Mayo 2025 - BsAs 42101030", index=False)
    cash_flow_gral.to_excel(writer, sheet_name="Gral.", index=False)
