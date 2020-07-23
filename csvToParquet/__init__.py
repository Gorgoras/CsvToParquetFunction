import logging
import os
import json
import pandas
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO
from io import BytesIO
from datetime import datetime
from azure.keyvault.secrets import SecretClient
from azure.identity import ManagedIdentityCredential
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Empezando conversion a parquet de un csv.')

    # tomo parametro con nombre de archivo csv
    csv_name = req.headers.get('csv_name')

    # creo path al archivo a leer
    csv_path = "PI_Genelba_DA/{}.csv".format(csv_name)

    # nombre de la tabla para buscar las urls
    fileSystem = "rawdata"

    # traer connection string como secreto del key vault
    retrieved_secret = getConnectionString()

    # creamos el objeto para conectarnos al archivo
    blob = BlobClient.from_connection_string(conn_str=retrieved_secret.value, 
                                         container_name=fileSystem, 
                                         blob_name=csv_path)

    # descargamos el csv y lo guardamos como string en una variable
    contenido = blob.download_blob().readall().decode('utf-8')

    # armo dataframe a partir del string
    df = pandas.read_csv(StringIO(contenido))

    # cliente para data lake
    blob_service_client = BlobServiceClient.from_connection_string(
        retrieved_secret.value)

    # dividir dataset por año y mes
    df['time'] = pandas.to_datetime(df['time'])
    df['year'] = df['time'].dt.year
    df['month'] = df['time'].dt.month
    years = df.year.unique()
    months = df.month.unique()
    blob_service_client = BlobServiceClient.from_connection_string(retrieved_secret.value)
    try:
        # para cada año y mes, creamos un parquet 
        for y in years:
            for m in months:
                df_loop = df.loc[(df['year']==y) & (df['month']==m)]
                df_upload = df_loop.drop(axis=1, columns=['year', 'month'])
                nombre_archivo = "strutured/Parquet/PI_Genelba_DA/{}/{}/PI_Genelba_DA_timestamp/{}.snappy.parquet".format(y, m, csv_name)
                blob_client = blob_service_client.get_blob_client(container=fileSystem, blob=nombre_archivo)
                subirDataframe(df_upload, blob_client)


        ret = dict()
        ret['result'] = "Success"
        return func.HttpResponse(
            json.dumps(ret),
            status_code=200
        )
    except Exception as ex:
        ret = dict()
        ret['result'] = ex
        return func.HttpResponse(
            json.dumps(ret),
            status_code=400
        )


def getConnectionString():
    """Retrieves the connection string using either Managed Identity
    or Service Principal"""

    KeyVault_DNS = os.environ["KeyVault_DNS"]
    SecretName = os.environ["SecretName"]

    try:
        creds = ManagedIdentityCredential()
        client = SecretClient(vault_url=KeyVault_DNS, credential=creds)
        retrieved_secret = client.get_secret(SecretName)
    except BaseException:
        creds = ClientSecretCredential(client_id=os.environ["SP_ID"],
                                       client_secret=os.environ["SP_SECRET"],
                                       tenant_id=os.environ["TENANT_ID"])
        client = SecretClient(vault_url=KeyVault_DNS, credential=creds)
        retrieved_secret = client.get_secret(SecretName)

    return retrieved_secret


def subirDataframe(df_to_upload, blob_client):
    """Sube un dataframe al archivo pasado como parametro"""

    # escribe tabla en un buffer de memoria
    table = pa.Table.from_pandas(df_to_upload)
    buf = BytesIO()
    pq.write_table(table, buf, compression='snappy')

    # subida de informacion
    logging.info('Subiendo informacion al lake')
    blob_client.upload_blob(buf.getvalue())