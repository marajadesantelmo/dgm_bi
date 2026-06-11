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
df['Mes'] = df['FechaEmision'].dt.to_period('M')
ventas_mensual = df.groupby(['Unidad de Negocios', 'Mes'])['Importe'].sum().reset_index()
ventas_mensual['Concepto'] = 'Ventas netas - ' + ventas_mensual['Unidad de Negocios']
ventas_mensual = ventas_mensual[['Unidad de Negocios', 'Mes', 'Concepto', 'Importe']]
ventas_mensual['Numero'] = '     '
ventas_mensual['Saldo'] = ventas_mensual['Importe']
ventas_mensual['CodigoProducto'] = '1.VENTAS'

#Gastos
cursor.execute("""SELECT
    p.Descripcion AS Producto,
    p.TipoProducto AS Concepto,
    p.Codigo AS CodigoProducto,
    ci.ImportePrecio1 AS Importe,
    c.FechaCreacion AS Fecha,
    f.RazonSocial AS Proveedor,
    c.Numero,
    c.NroSucursal,
    CASE 
        WHEN c.Estado = 0 THEN 'Pendiente'
        WHEN c.Estado = 1 THEN 'Pagado'
        WHEN c.Estado = 2 THEN 'Anulado'
    ELSE 'Otro'
    END AS Estado,
    CASE 
        WHEN c.TipoComprobante = 0 THEN 'Factura'
        WHEN c.TipoComprobante = 1 THEN 'Nota de Credito'
        WHEN c.TipoComprobante = 2 THEN 'Nota de Debito'
        ELSE 'Otro'
    END AS TipoComprobante,
    c.Numero
FROM comprasitems ci
LEFT JOIN compras c
    ON ci.IDCompra = c.RecID
LEFT JOIN fiscal f
    ON c.IDFiscal = f.RecID
LEFT JOIN productos p
    ON ci.IDProducto = p.RecID
LEFT JOIN arbolcarpetas ac
    ON p.IDCarpeta = ac.RecID
WHERE c.FechaCreacion >= '2025-01-01';""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
gastos= pd.DataFrame(data, columns=columns)

gastos = pd.DataFrame(data, columns=columns)
gastos['Fecha']= pd.to_datetime(gastos['Fecha'])
gastos.loc[:, 'Mes'] = gastos['Fecha'].dt.to_period('M')
gastos['Fecha'] = gastos['Fecha'].dt.strftime("%d/%m/%Y")
gastos['Saldo'] = gastos.apply(lambda row: -row['Importe'] if row['TipoComprobante'] != 'Nota de Credito' else row['Importe'], axis=1)
gastos['Concepto'] = gastos['Concepto'].str.title()
gastos = gastos[gastos['Estado']== 'Pagado']
#### VER: se puede asignar unidad de negocios desde compras?

#Sueldos
cursor.execute("""
SELECT 
    cc.Numero,
    cc.Descripcion AS Concepto,
    a.Descripcion AS Producto,
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
sueldos = pd.DataFrame(data, columns=columns)
sueldos['Concepto'] = sueldos['Concepto'].str.title()
sueldos['Fecha']= pd.to_datetime(sueldos['Fecha'])
sueldos.loc[:, 'Mes'] = sueldos['Fecha'].dt.to_period('M')
sueldos['Fecha'] = sueldos['Fecha'].dt.strftime("%d/%m/%Y")
sueldos['Saldo'] = - sueldos['Importe']
sueldos['Unidad de Negocios'] = 'Salta'
sueldos.loc[
    sueldos['Concepto'].str.contains(r'Bs\.As\.|Bs As|Bsas|Ventas netas - Otros', regex=True, case=False, na=False),
    'Unidad de Negocios'
] = 'Buenos Aires'
sueldos['CodigoProducto'] = '2.Sueldos y Cargas Sociales'
sueldos['Producto'] = sueldos['Producto'].str.title()


margen_operativo = pd.concat([ventas_mensual, sueldos, gastos])

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

margen_operativo.to_excel('data/margen_operativo_desde_compras.xlsx', sheet_name='Margen Operativo desde Compras')


