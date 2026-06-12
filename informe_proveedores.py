# -*- coding: utf-8 -*-
"""
Análisis de proveedores según facturación
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
    c.Nombre,
    c.Apellido,
    c.Correo,
    e.Empresa,
    ti.Valor AS Tipo,
    MAX(c.FechaCreacion) AS FechaCreacion,
    MAX(c.FechaModificacion) AS FechaModificacion,
    MAX(comps.FechaCreacion) AS UltimaCompra
FROM contactos c
LEFT JOIN empresas e
    ON e.IDEmpresa = c.IDEmpresa
LEFT JOIN industriaysub ind
    ON ind.IDRef = e.IDEmpresa
LEFT JOIN compras comps
    ON comps.IDRef = c.IDContacto
LEFT JOIN tiposysub ti
    ON ti.IDRef = e.IDEmpresa
GROUP BY
    e.Empresa
""")

data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
contactos = pd.DataFrame(data, columns=columns)

illegal_chars = re.compile(r"[\000-\010]|[\013-\014]|[\016-\037]")
for col in contactos.select_dtypes(include=["object"]).columns:
    contactos[col] = contactos[col].astype(str).apply(lambda x: illegal_chars.sub("", x))

contactos = contactos[contactos['Tipo']=='PROVEEDOR']
contactos.drop(columns=['Apellido', 'Correo', 'Nombre'], inplace=True)
contactos.sort_values(by="Empresa", ascending=True, inplace=True)
contactos.sort_values(by="FechaCreacion", ascending=False, inplace=True)

# Compras del último año por proveedor (facturas no anuladas)
cursor.execute("""
SELECT
    e.Empresa,
    COUNT(DISTINCT comps.RecID) AS CantidadFacturas,
    SUM(ci.ImportePrecio1) AS ImporteUltimoAnio
FROM empresas e
JOIN contactos c ON c.IDEmpresa = e.IDEmpresa
JOIN compras comps ON comps.IDRef = c.IDContacto
JOIN comprasitems ci ON ci.IDCompra = comps.RecID
WHERE comps.FechaCreacion >= DATE_SUB(NOW(), INTERVAL 1 YEAR)
  AND comps.Estado != 2
  AND comps.TipoComprobante = 0
GROUP BY e.Empresa
""")

data_compras = cursor.fetchall()
columns_compras = [column[0] for column in cursor.description]
compras_anio = pd.DataFrame(data_compras, columns=columns_compras)

contactos = contactos.merge(compras_anio, on='Empresa', how='left')
contactos['CantidadFacturas'] = contactos['CantidadFacturas'].fillna(0).astype(int)
contactos['ImporteUltimoAnio'] = contactos['ImporteUltimoAnio'].fillna(0).round(2)

contactos_2025_creacion = contactos[contactos["FechaCreacion"] >= "2025-10-01"]
contacto_2025_ultima_compra = contactos[contactos["UltimaCompra"] >= "2025-10-01"]
proveedores_activos = contactos[contactos["CantidadFacturas"] > 0].sort_values("ImporteUltimoAnio", ascending=False)

with pd.ExcelWriter("Informe proveedores 2026-06.xlsx", engine="openpyxl") as writer:
    contactos_2025_creacion.to_excel(writer, sheet_name="Proveedores nuevos segun alta", index=False)
    contacto_2025_ultima_compra.to_excel(writer, sheet_name="Proveedores activos segun ultima compra", index=False)
    proveedores_activos.to_excel(writer, sheet_name="Compras ultimo anio", index=False)
    contactos.to_excel(writer, sheet_name="Todos los proveedores", index=False)
