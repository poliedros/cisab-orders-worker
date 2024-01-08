# Consolidado de demandas

Para executar esta aplicação:

1. Primeiro configure as variáveis de ambiente em um arquivo <em>.env</em>.

  ```
  MONGO_CONN_STR
  AZURE_CONN_STR
  AZURE_BLOB_STORAGE
  RABBITMQ_CONN_STR
  RABBITMQ_USER
  RABBITMQ_PASSWORD
  RABBITMQ_PORT
  RABBITMQ_TO
  ```
 
2. Instale os requerimentos:

   `pip install -r requirements.txt`

3. Uma vez configuradas as variáveis, é possível executar o notebook célula a célula ou executar o script em Python: 

   `python consolidateData.py`

