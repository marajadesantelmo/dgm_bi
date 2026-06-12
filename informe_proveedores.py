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
    c.Nombre,
    c.Apellido, 
    c.Correo, 
    e.Empresa, 
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
contactos.sort_values(by="Empresa", ascending=True, inplace=True)
contactos.sort_values(by="FechaCreacion", ascending=False, inplace=True)

contactos_2025_creacion = contactos[contactos["FechaCreacion"] >= "2025-10-01"]
contacto_2025_ultima_compra = contactos[contactos["UltimaCompra"] >= "2025-10-01"]

with pd.ExcelWriter("Informe proveedores 2026-06.xlsx", engine="openpyxl") as writer:
    contactos_2025_creacion.to_excel(writer, sheet_name="Proveedores nuevos segun alta", index=False)
    contacto_2025_ultima_compra.to_excel(writer, sheet_name="Proveedores activos segun ultima compra", index=False)
