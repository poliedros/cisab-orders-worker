
# -*- coding: utf-8 -*-

import os
import json
import pika
import logging
import pymongo
import pandas as pd
from itertools import chain
from datetime import datetime, timedelta
from dotenv import load_dotenv
from azure.storage.blob import BlobClient
from reportlab.lib.pagesizes import letter
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph
from reportlab.lib.enums import TA_CENTER
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

load_dotenv()


# Realizando conexão com o banco
if os.getenv("MONGO_CONN_STR") is None:
    logging.error("Unable to get environment variables.")
    exit()

client = pymongo.MongoClient(
    os.getenv("MONGO_CONN_STR"), serverSelectionTimeoutMS=5000)

try:
    client.server_info()
except Exception:
    logging.error("Unable to connect to Mongo server.")
    exit()

database = client.get_database()


# Extraindo coleções
cart_collection = database.get_collection("carts")
demand_collection = database.get_collection("demands")


# Extraindo demandas que fecham na data de hoje
today = datetime.today()
tomorrow = today + timedelta(days=1)

today = today.replace(hour=0, minute=0, second=0, microsecond=0)
tomorrow = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)

demands = demand_collection.find({
    "end_date": {"$gte": today, "$lt": tomorrow}
})

closed_demands = [str(demand["_id"]) for demand in demands]
if len(closed_demands) == 0:
    logging.info("There's no demand closing today!")
    exit()


# Extraindo carrinhos fechados das demandas fechadas
carts = cart_collection.find({
    "state": {"$eq": "closed"},
    "demand_id": {"$in": closed_demands}
})


# Transformando as coleções em dataframes
df_carts = pd.DataFrame(list(carts))

logging.info(str(len(df_carts)) + " orders for today.")


# Pré-processamento
df_carts = df_carts.rename(columns={'_id': 'cart_id'})


# Expandindo a coluna de produtos
df_exploded = df_carts.explode(column="products")


# Extraindo atributos dos produtos e colocando-os em outras colunas
def getAtrr(row, atrribute):
    rowDict = dict(row)
    return rowDict[atrribute]


df_exploded["product_id"] = df_exploded["products"].apply(
    lambda x: getAtrr(x, "_id"))
df_exploded["quantity"] = df_exploded["products"].apply(
    lambda x: getAtrr(x, "quantity"))


# Montando o nome final do produto e adicionando-o na coluna descrição
def getProductDesc(row):
    measurementsList = list(row["products"]["measurements"])
    newList = []
    for m in measurementsList:
        newList.append(" ".join(list(m.values())))

    normsList = list(row["products"]["norms"])

    return row["products"]["name"] + " " + " ".join(newList) + " " + " ".join(normsList)


df_exploded["description"] = df_exploded.apply(
    lambda x: getProductDesc(x), axis=1)


# Exportando o dataframe final para planilha em Excel na Azure
def uploadToStorage(file_name):
    try:
        blob = BlobClient.from_connection_string(conn_str=os.getenv(
            "AZURE_CONN_STR"), container_name="cisab-consolidados", blob_name=file_name)
    except:
        logging.error("Unable to connect with Blob Storage.")
        exit()

    try:
        with open(file_name, "rb") as data:
            blob.upload_blob(data)
    except:
        logging.error("Unable to upload the file to Blob Storage.")
        exit()


# Enviando evento para o RabbitMQ mandar o email
def createAndSendEvent(connection, spreadsheet, pdf):
    channel = connection.channel()

    to = os.getenv("RABBITMQ_TO")
    event = {
        "pattern": "send_email",
        "data": {
        "message": {
            "to": to,
            "subject": "Consolidado de pedidos",
            "body": "A demanda fechou e você pode baixar o consolidado de pedidos pelo link: <a href='" + os.getenv("AZURE_BLOB_STORAGE") + spreadsheet + "'>clique aqui</a>. Você também pode baixar o relatório de pedidos pelo link: <a href='" + os.getenv("AZURE_BLOB_STORAGE") + pdf + "'>clique aqui</a>"
        }
        }
    }

    channel.queue_declare(queue='notifier')
    channel.basic_publish(exchange='',
                        routing_key='notifier',
                        body=json.dumps(event, ensure_ascii=False))

    logging.info(f"Email has been sent to {to}")

    channel.close()


def sendEmail(spreadsheet, pdf):
    try:
        credentials = pika.PlainCredentials(os.getenv("RABBITMQ_USER"),
                                            os.getenv("RABBITMQ_PASSWORD"))
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(os.getenv("RABBITMQ_CONN_STR"),
                                    int(os.getenv("RABBITMQ_PORT")),
                                    '/',
                                    credentials))
        
        createAndSendEvent(connection, spreadsheet, pdf)

    except:
        logging.error("Unable to connect with RabbitMQ.")
        exit()

# Gerando relatório em PDF com os pedidos de cada município
def createPDF(df, file_name):
  county_prods = []
  for county in df["Município / Autarquia"].unique():
    prods_list = df[df["Município / Autarquia"] == county][["Produto", "Quantidade"]].values.tolist()
    prods_list.insert(0, [county])
    county_prods.append(prods_list)

  subheaders = []
  unnested_list = list(chain(*county_prods))
  for county in df["Município / Autarquia"].unique():
    subheaders.append(unnested_list.index([county]))

  # Criando PDF
  doc = SimpleDocTemplate(file_name, pagesize=letter)

  # Colocando título
  styles = getSampleStyleSheet()
  styles.add(ParagraphStyle(name='centered', parent=styles['Heading3'], alignment=TA_CENTER))
  title = Paragraph("Relatório de pedidos por município", styles["Title"])
  subtitle = Paragraph(df.loc[0, 'demand_name'], styles["centered"])

  # Criando tabela
  table = Table(unnested_list)

  for i in subheaders:
    # Adicionando estilo para os subtítulos (nome dos municípios)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, i), (-1, i), "gray"),
        ("TEXTCOLOR", (0, i), (-1, i), "white"),
    ]))

  # Gerando PDF
  doc.build([title, subtitle, table])

# Gerando a planilha a partir do dataframe final
def generateFiles(df, demand_id):
    df_demand = df[df.demand_id == demand_id].reset_index()

    if(len(df_demand) > 0):
        df_pivot = df_demand.pivot_table(index="Produto", values="Quantidade", columns="Município / Autarquia")
        df_pivot["Total"] = df_pivot.sum(axis=1)
        df_pivot = df_pivot.sort_values(by="Produto")

        format_data = "%Y-%m-%d %H-%M-%S"
        file_name = datetime.strftime(datetime.today(), format_data) + \
            " " + df_demand.loc[0, "demand_name"]
        sheet_name = f"{file_name}.xlsx"
        pdf_name = f"{file_name}.pdf"

        df_pivot.to_excel(sheet_name)
        createPDF(df_demand, pdf_name)

        uploadToStorage(sheet_name)
        uploadToStorage(pdf_name)
        sendEmail(sheet_name, pdf_name)
    else:
        logging.warning("There weren't any orders for " + demand_id)

# Pós-processamento
newNames = {"county_name": "Município / Autarquia",
            "description": "Produto", "quantity": "Quantidade"}
df = df_exploded.rename(columns=newNames)


# Executando processos finais para cada demanda fechada no dia
for demand in closed_demands:
    generateFiles(df, demand)
