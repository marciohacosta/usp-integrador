import os
import pandas as pd
import pyodbc
from prophet import Prophet
from dotenv import load_dotenv

load_dotenv("predicao-dev.env")

# Parâmetros de ambiente
SERVER = os.environ["SERVER"]
DATABASE = os.environ["DATABASE"]
USERNAME = os.environ["USER"]
PASSWORD = os.environ["PASSWORD"]
REGID = int(os.environ["REGID"])
PERIOD = int(os.environ["PERIOD"])

# String de conexão para o SQL Server On Premise
connString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'

# Criação do objeto de conexão
conn = pyodbc.connect(connString)

# Criação do objeto cursor para execução das procedures
cursor = conn.cursor()

# Limpeza da predição existente
sqlClean = f"{{call usp_Predicoes_Clean({REGID})}}"
cursor.execute(sqlClean)
conn.commit()

# Obtenção dos dados carregados em refined (on premise)
sqlQuery = f"{{call usp_Temperaturas_SelectForPrediction({REGID})}}"
cursor.execute(sqlQuery)
records = cursor.fetchall()

# Transformação para data frame adequado ao Prophet
refinedData = pd.DataFrame.from_records(records, columns=["ds", "y"])
refinedData = refinedData.sort_values(by='ds')
refinedData = refinedData.reset_index(drop=True)

# Modelagem
m = Prophet()
m.fit(refinedData)

# Treinamento
future = m.make_future_dataframe(periods=PERIOD)

# Predição
forecast = m.predict(future)

# Inserção na tabela de predições
for index, row in forecast.iterrows():
    sqlStmt = f"{{call usp_Predicoes_Insert({REGID}, '{row['ds']}', {row['yhat']})}}"
    print(sqlStmt)
    cursor.execute(sqlStmt)

# Efetivação das transações
conn.commit()

# Fechamento do cursor e conexão
cursor.close()
conn.close()
