# -*- coding: utf-8 -*-
"""
Análisis de clientes nuevos según facturación
"""

import mysql.connector
import pandas as pd
from tokens import host, user, database
import re

connection = mysql.connector.connect(
    host=host, 
    user=user,
    database=database,
    charset='utf8'
)
cursor = connection.cursor()

cursor.execute("""
SELECT 
    e.Empresa, 
    fisc.NroImpuesto1 AS CUIT,
    ind.Valor AS Segmento, 
    ti.Valor AS Tipo,
    MAX(f.FechaEmision) AS UltimaFactura
FROM contactos c
LEFT JOIN empresas e 
    ON e.IDEmpresa = c.IDEmpresa
LEFT JOIN fiscal fisc 
    ON fisc.IDRef = e.IDEmpresa
LEFT JOIN industriaysub ind 
    ON ind.IDRef = e.IDEmpresa
LEFT JOIN facturas f 
    ON f.IDRef = c.IDContacto
LEFT JOIN tiposysub ti
    ON ti.IDRef = e.IDEmpresa
GROUP BY 
    e.Empresa
""")

data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
clientes = pd.DataFrame(data, columns=columns)
illegal_chars = re.compile(r"[\000-\010]|[\013-\014]|[\016-\037]")
for col in clientes.select_dtypes(include=["object"]).columns:
    clientes[col] = clientes[col].astype(str).apply(lambda x: illegal_chars.sub("", x))

clientes.sort_values(by="Empresa", ascending=True, inplace=True)
clientes= clientes[clientes["UltimaFactura"].notna()].copy()
clientes["UltimaFactura"] = pd.to_datetime(clientes["UltimaFactura"], format='%Y-%m-%d')
clientes = clientes[clientes['UltimaFactura'] >= '2020-01-01']
clientes = clientes.sort_values(by="UltimaFactura", ascending=False)
clientes["UltimaFactura"] = clientes["UltimaFactura"].dt.strftime("%d/%m/%Y")


cursor.execute("""SELECT
    f.RazonSocial AS Proveedor,
    f.NroImpuesto1 AS CUIT,
    c.FechaCreacion AS Fecha,
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
FROM compras c
LEFT JOIN fiscal f
    ON c.IDFiscal = f.RecID
WHERE c.FechaCreacion >= '2020-01-01';""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
proveedores= pd.DataFrame(data, columns=columns)
proveedores = proveedores[proveedores['Estado']== 'Pagado']
proveedores['Fecha']= pd.to_datetime(proveedores['Fecha'], format='%Y-%m-%d')
proveedores = (proveedores.groupby(['Proveedor', 'CUIT'], as_index=False)['Fecha'].max())
proveedores.rename(columns={'Fecha': 'Ultima Compra'}, inplace=True)
proveedores = proveedores.sort_values(by='Ultima Compra', ascending=False)
proveedores["Ultima Compra"] = proveedores["Ultima Compra"].dt.strftime("%d/%m/%Y")

for col in proveedores.select_dtypes(include=["object"]).columns:
    proveedores[col] = proveedores[col].astype(str).apply(lambda x: illegal_chars.sub("", x))

with pd.ExcelWriter("Informe Clientes y Proveedores Oct2025-v2.xlsx", engine="openpyxl") as writer:
    clientes.to_excel(writer, sheet_name="Clientes 2020 o mas reciente", index=False)
    proveedores.to_excel(writer, sheet_name='Proveedores 2020 o mas reciente', index=False)

