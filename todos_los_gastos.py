# -*- coding: utf-8 -*-
"""
Generacion de cashflow en excel
"""

import mysql.connector
import pandas as pd
from tokens import host, user, database

dolar_cotizacion = pd.read_excel('cotizacion_dolar_procesada.xlsx') #Ver que hay que mejorar esto

connection = mysql.connector.connect(
    host='190.12.103.138',
    user='root',
    database='dgm',
    charset='utf8'
)
cursor = connection.cursor()

#Gastos
cursor.execute("""
SELECT a.FechaCreacion, ai.Importe1, cc.Descripcion, cc.Numero
FROM asientos a
LEFT JOIN asientositems ai ON a.RecID = ai.IDAsiento
LEFT JOIN cuentascontables cc ON cc.RecID = ai.IDCuentaContable
WHERE a.FechaCreacion >= '2025-01-01'
""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
gastos = pd.DataFrame(data, columns=columns)
gastos['Descripcion'] = gastos['Descripcion'].str.strip().str.title()

gastos.loc[:, 'FechaCreacion'] = pd.to_datetime(gastos['FechaCreacion'])
gastos.loc[:, 'Mes'] = gastos['FechaCreacion'].dt.to_period('M')
gastos_mensual= gastos.groupby(['Numero', 'Descripcion', 'Mes'])['Importe1'].sum().reset_index()

pnl_format = gastos_mensual.pivot_table(index=['Numero', 'Descripcion'], columns='Mes', values='Importe1', aggfunc='sum')

# Reset column names for better readability
pnl_format.columns.name = None  # Remove the columns name
pnl_format = pnl_format.reset_index()

print(pnl_format)

pnl_format.to_excel('ver_todos_gastos.xlsx')