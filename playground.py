import mysql.connector
import pandas as pd
from tokens import host, user, database

'C:\\Users\\ceo\\Documents\\Tableros Power BI'

#dolar_cotizacion = pd.read_excel('cotizacion_dolar_procesada.xlsx') #Ver que hay que mejorar esto

connection = mysql.connector.connect(
    host=host, 
    user=user,
    database=database,
    charset='utf8'
)
cursor = connection.cursor()


cursor.execute("""
SELECT *
FROM cuentascontables
""")
data = cursor.fetchall()
columns = [column[0] for column in cursor.description]
df = pd.DataFrame(data, columns=columns)