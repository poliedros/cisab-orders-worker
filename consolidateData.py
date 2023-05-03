
# -*- coding: utf-8 -*-

import os
import json
import pika
import logging
import pymongo
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobClient


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
tomorrow = today.replace(day=today.day + 1, hour=0,
                         minute=0, second=0, microsecond=0)
today = today.replace(hour=0, minute=0, second=0, microsecond=0)

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
def createAndSendEvent(connection, file_name):
    channel = connection.channel()

    to = os.getenv("RABBITMQ_TO")
    event = {
        "pattern": "send_email",
        "data": {
            "message": {
                "to": to,
                "subject": "Consolidado de pedidos",
                "body": "A demanda fechou e você pode baixar o consolidado de pedidos pelo link: <a href='" + os.getenv("AZURE_BLOB_STORAGE") + file_name + "'>clique aqui</a>"
            }
        }
    }

    channel.queue_declare(queue='notifier')
    channel.basic_publish(exchange='',
                          routing_key='notifier',
                          body=json.dumps(event, ensure_ascii=False))

    logging.info(f"Email has been sent to {to}")

    channel.close()


def sendEmail(file_name):
    try:
        credentials = pika.PlainCredentials(os.getenv("RABBITMQ_USER"),
                                            os.getenv("RABBITMQ_PASSWORD"))
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(os.getenv("RABBITMQ_CONN_STR"),
                                      int(os.getenv("RABBITMQ_PORT")),
                                      '/',
                                      credentials))

        createAndSendEvent(connection, file_name)

    except:
        logging.error("Unable to connect with RabbitMQ.")
        exit()


# Gerando a planilha a partir do dataframe final
def generateSheet(df, demand_id):
    df_demand = df[df.demand_id == demand_id].reset_index()

    if (len(df_demand) > 0):
        df_pivot = df_demand.pivot_table(
            index="Produto", values="Quantidade", columns="Município / Autarquia")
        df_pivot["Total"] = df_pivot.sum(axis=1)
        df_pivot = df_pivot.sort_values(by="Produto")

        format_data = "%Y-%m-%d %H-%M-%S"
        file_name = datetime.strftime(datetime.today(), format_data) + \
            " " + df_demand.loc[0, "demand_name"] + ".xlsx"
        df_pivot.to_excel(file_name)

        uploadToStorage(file_name)
        sendEmail(file_name)
    else:
        logging.warning("There weren't any orders for " + demand_id)


# Pós-processamento
newNames = {"county_name": "Município / Autarquia",
            "description": "Produto", "quantity": "Quantidade"}
df = df_exploded.rename(columns=newNames)


# Executando processos finais para cada demanda fechada no dia
for demand in closed_demands:
    generateSheet(df, demand)
