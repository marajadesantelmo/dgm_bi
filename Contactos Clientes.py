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
    ind.Valor AS Segmento, 
    ti.Valor AS Tipo,
    MAX(c.FechaCreacion) AS FechaCreacion,
    MAX(c.FechaModificacion) AS FechaModificacion, 
    MAX(f.FechaEmision) AS UltimaFactura 
FROM contactos c
LEFT JOIN empresas e 
    ON e.IDEmpresa = c.IDEmpresa
LEFT JOIN industriaysub ind 
    ON ind.IDRef = e.IDEmpresa
LEFT JOIN facturas f 
    ON f.IDRef = c.IDContacto
LEFT JOIN tiposysub ti
    ON ti.IDRef = e.IDEmpresa
GROUP BY 
    c.Apellido, 
    c.Correo, 
    e.Empresa
""")

data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
contactos = pd.DataFrame(data, columns=columns)

illegal_chars = re.compile(r"[\000-\010]|[\013-\014]|[\016-\037]")
for col in contactos.select_dtypes(include=["object"]).columns:
    contactos[col] = contactos[col].astype(str).apply(lambda x: illegal_chars.sub("", x))

contactos.sort_values(by="Empresa", ascending=True, inplace=True)
contactos.sort_values(by="FechaCreacion", ascending=False, inplace=True)
contactos_clientes = contactos[contactos["UltimaFactura"].notna()].copy()

with pd.ExcelWriter("Contactos Clientes.xlsx", engine="openpyxl") as writer:
    contactos.to_excel(writer, sheet_name="Todos los Contactos", index=False)
    contactos_clientes.to_excel(writer, sheet_name="Contactos con Factura", index=False)

