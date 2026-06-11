# -*- coding: utf-8 -*-
"""
Análisis de clientes nuevos según facturación
"""

import mysql.connector
import pandas as pd
from tokens import host, user, database

connection = mysql.connector.connect(
    host=host, 
    user=user,
    database=database,
    charset='utf8'
)
cursor = connection.cursor()

cursor.execute("""
SELECT   e.Empresa, f.FechaEmision,
    CASE 
        WHEN t.NroSucursal IN (3, 8) THEN 'Buenos Aires'
        WHEN t.NroSucursal = 4 THEN 'Salta'
        ELSE 'Otros'
    END AS `Unidad de Negocios`
FROM facturas f
LEFT JOIN contactos c ON f.IDRef = c.IDContacto
LEFT JOIN empresas e ON c.IDEmpresa = e.IDEmpresa
LEFT JOIN talonarios t ON f.IDTalonario = t.RecID
""")

data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
facturas = pd.DataFrame(data, columns=columns)

# Cerrar conexión
cursor.close()
connection.close()

facturas['FechaEmision'] = pd.to_datetime(facturas['FechaEmision'])
facturas['AñoMes'] = facturas['FechaEmision'].dt.to_period('M')
idx = facturas.groupby('Empresa')['FechaEmision'].idxmin()
primeras_facturas = facturas.loc[idx].reset_index(drop=True)
primeras_facturas['AñoMes'] = primeras_facturas['FechaEmision'].dt.to_period('M')
nuevos_clientes = primeras_facturas.groupby(['AñoMes', 'Unidad de Negocios']).size().reset_index(name='ClientesNuevos')
nuevos_clientes = nuevos_clientes.sort_values('AñoMes')
nuevos_clientes['Mes'] = nuevos_clientes['AñoMes'].astype(str)
nuevos_clientes = nuevos_clientes[['Mes', 'Unidad de Negocios', 'ClientesNuevos']]
#Guarda datos procedos para powerbi
nuevos_clientes.to_excel('data/nuevos_clientes.xlsx')


#Informe con nombre de cliente
clientes_primer_factura = primeras_facturas[['Empresa', 'AñoMes', 'Unidad de Negocios']]
clientes_primer_factura['Mes'] = clientes_primer_factura['AñoMes'].astype(str)
clientes_primer_factura = clientes_primer_factura[['Empresa', 'Mes', 'Unidad de Negocios']]
clientes_primer_factura = clientes_primer_factura.sort_values('Mes')

# Save to Excel
clientes_primer_factura.to_excel('data/clientes_primer_factura.xlsx', index=False)