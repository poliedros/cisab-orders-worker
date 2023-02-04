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
product_collection = database.get_collection("products")
county_collection = database.get_collection("counties")

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

# Extraindo todos os produtos, municípios e autarquias
products = product_collection.find()
counties = county_collection.find()

# Transformando as coleções em dataframes
df_carts = pd.DataFrame(carts)
df_products = pd.DataFrame(products)
df_counties = pd.DataFrame(counties)

logging.info(str(len(df_carts)) + " orders for today.")

# Pre-processando os dataframes
df_carts = df_carts.rename(columns={'_id': 'cart_id'})
df_products = df_products.rename(columns={'_id': 'product_id'})

df_carts.county_id = df_carts.county_id.astype(str)
df_counties._id = df_counties._id.astype(str)

# Juntando carrinhos com municípios e autarquias para pegar seus nomes através de seus ids
df_carts_with_counties = df_carts.merge(
    df_counties[["_id", "name"]], left_on="county_id", right_on="_id")[["name", "products"]]

# Expandindo a coluna de produtos
df_exploded = df_carts_with_counties.explode(column="products")

# Extraindo atributos dos produtos e colocando-os em outras colunas


def getAtrr(row, atrribute):
    rowDict = dict(row)
    return rowDict[atrribute]


df_exploded["_id"] = df_exploded["products"].apply(lambda x: getAtrr(x, "_id"))
df_exploded["quantity"] = df_exploded["products"].apply(
    lambda x: getAtrr(x, "quantity"))

# Uma vez extraídos os produtos, pode-se excluir a coluna produtos
df_exploded.drop(columns=["products"], inplace=True)

# Montando o nome final do produto e adicionando-o na coluna descrição


def getProductDesc(row):
    measurementsList = list(row["measurements"])
    newList = []
    for m in measurementsList:
        newList.append(" ".join(list(m.values())))

    normsList = list(row["norms"])

    return row["name"] + " " + " ".join(newList) + " " + " ".join(normsList)


df_products["description"] = df_products.apply(
    lambda x: getProductDesc(x), axis=1)

# Processamento dos dataframes para unir o df expandido com o df de produtos
newNames = {"name": "Município / Autarquia",
            "description": "Produto", "quantity": "Quantidade"}

df_products.product_id = df_products.product_id.astype(str)
df_exploded._id = df_exploded._id.astype(str)

df = df_exploded.merge(df_products[["product_id", "description"]], left_on="_id",
                       right_on="product_id").drop(columns="_id").rename(columns=newNames)

# Reorganizando o dataframe final e calculando o total
df_pivot = df.pivot_table(
    index="Produto", values="Quantidade", columns="Município / Autarquia")
df_pivot["Total"] = df_pivot.sum(axis=1)

# Exportando o dataframe final para planilha em Excel na Azure
format_data = "%Y-%m-%d_%H-%M-%S"
file_name = "Consolidado-de-Pedidos_" + \
    datetime.strftime(datetime.today(), format_data) + ".xlsx"
df_pivot.to_excel(file_name)

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
try:
    credentials = pika.PlainCredentials(os.getenv("RABBITMQ_USER"),
                                        os.getenv("RABBITMQ_PASSWORD"))
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(os.getenv("RABBITMQ_CONN_STR"),
                                  int(os.getenv("RABBITMQ_PORT")),
                                  '/',
                                  credentials))
except:
    logging.error("Unable to connect with RabbitMQ.")
    exit()

channel = connection.channel()

event = {
    "message": {
        "to": os.getenv("RABBITMQ_TO"),
        "subject": "Consolidado de pedidos",
        "body": "A demanda fechou e você pode baixar o consolidado de pedidos pelo link: " + os.getenv("AZURE_BLOB_STORAGE") + file_name
    }
}

channel.queue_declare(queue='send_email')
channel.basic_publish(exchange='',
                      routing_key='send_email',
                      body=json.dumps(event, ensure_ascii=False))
