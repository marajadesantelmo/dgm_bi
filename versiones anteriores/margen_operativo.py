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
        WHEN t.NroSucursal IN (3, 8) THEN 'Buenos Aires'
        WHEN t.NroSucursal = 4 THEN 'Salta'
        ELSE 'Buenos Aires'
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
# df.loc[df['Cliente'].str.contains('Towards', na=False), 'Unidad de Negocios'] = 'Otros'   #Acá y en la query de arriba puse Otros en Buenos Aires
df['Mes'] = df['FechaEmision'].dt.to_period('M')
ventas_mensual = df.groupby(['Unidad de Negocios', 'Mes'])['Importe'].sum().reset_index()
ventas_mensual['Concepto'] = 'Ventas netas - ' + ventas_mensual['Unidad de Negocios']
ventas_mensual = ventas_mensual[['Unidad de Negocios', 'Mes', 'Concepto', 'Importe']]
ventas_mensual['Numero'] = '     '
ventas_mensual['Saldo'] = ventas_mensual['Importe']

cursor.execute("""
SELECT 
    cc.Numero,
    cc.Descripcion AS Concepto,
    a.Descripcion AS Comprobante,
    ai.Importe1 AS Importe,
    a.Fecha
FROM asientositems ai
LEFT JOIN asientos a 
    ON ai.IDAsiento = a.RecID
LEFT JOIN cuentascontables cc 
    ON ai.IDCuentaContable = cc.RecID
LEFT JOIN movimientos m
    ON ai.IDMovimiento = m.RecID
WHERE a.Fecha >= '2025-01-01'
  AND cc.Numero  IN (
      42101009,
      42101029,
      42201009,
      42201029,
      42102005,
      42102023
  );
""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
gastos = pd.DataFrame(data, columns=columns)
gastos['Concepto'] = gastos['Concepto'].str.title()
gastos = gastos.loc[:, ~gastos.columns.duplicated()]
gastos['Fecha']= pd.to_datetime(gastos['Fecha'])
gastos.loc[:, 'Mes'] = gastos['Fecha'].dt.to_period('M')
gastos['Fecha'] = gastos['Fecha'].dt.strftime("%d/%m/%Y")
gastos = gastos[~gastos['Comprobante'].str.contains('Asiento de cierre')]
gastos = gastos[~gastos['Concepto'].str.contains('Amortizaciones')]
gastos = gastos[~gastos['Concepto'].str.contains('Armotizaciones')]
gastos = gastos[gastos['Numero'] != "42105001"] #Bsas Diferencia De Cambio
gastos = gastos[gastos['Numero'] != "42301016"] #Bio
gastos['Saldo'] = - gastos['Importe']

gastos['Unidad de Negocios'] = 'Salta'
gastos.loc[
    gastos['Concepto'].str.contains(r'Bs\.As\.|Bs As|Bsas|Ventas netas - Otros', regex=True, case=False, na=False),
    'Unidad de Negocios'
] = 'Buenos Aires'


margen_operativo = pd.concat([ventas_mensual, gastos])

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


margen_operativo= ordenar(margen_operativo)

categorias_ctas = pd.read_excel('categorias_ctas_contables.xlsx')

margen_operativo['Numero'] = margen_operativo['Numero'].astype(str)
categorias_ctas['Numero'] = categorias_ctas['Numero'].astype(str)

margen_operativo = margen_operativo.merge(
    categorias_ctas,
    on="Numero",
    how="left"
)

margen_operativo['Categoria'] = margen_operativo['Categoria'].fillna("1.VENTAS")

margen_operativo.to_excel('data/margen_operativo.xlsx', sheet_name='Margen Operativo')





##### VER ESTO CON DATA DEL MAYOR


cursor.execute("""
SELECT 
    cc.Numero,
    cc.Descripcion AS Concepto,
    a.Descripcion AS Comprobante,
    ai.Importe1 AS Importe,
    a.Fecha,
    f.RazonSocial
FROM asientositems ai
LEFT JOIN asientos a 
    ON ai.IDAsiento = a.RecID
LEFT JOIN cuentascontables cc 
    ON ai.IDCuentaContable = cc.RecID
LEFT JOIN movimientos m
    ON ai.IDMovimiento = m.RecID
LEFT JOIN fiscal f
    ON m.IDFiscalCliente = f.RecID
WHERE a.Fecha >= '2025-01-01'
  AND cc.Numero > 42100000;
""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
gastos = pd.DataFrame(data, columns=columns)


cursor.execute("""
SELECT a.Fecha, f.RazonSocial, a.Descripcion AS Comprobante, cc.Numero,  cc.Descripcion AS Concepto, ai.Importe1 AS Importe
FROM movimientos m
LEFT JOIN asientositems ai
    ON ai.IDMovimiento = m.RecID
LEFT JOIN asientos a 
    ON ai.IDAsiento = a.RecID
LEFT JOIN cuentascontables cc 
    ON ai.IDCuentaContable = cc.RecID
LEFT JOIN fiscal f
    ON m.IDFiscalCliente = f.RecID
WHERE a.Fecha >= '2025-01-01'
  AND cc.Numero > 42100000;
""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
df = pd.DataFrame(data, columns=columns)
df
