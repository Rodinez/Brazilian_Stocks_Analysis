import requests
from datetime import datetime
import json
import os
import zipfile
import tempfile
import boto3
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

datasets = {
    "dfp": "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/",
    "itr": "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/",
    "fre": "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FRE/DADOS/",
    "fca": "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/"
}

METADATA_BUCKET = "metadata"
METADATA_KEY = "cvm/hist_cmv.json"

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
    )

def get_links():
    zips = []

    for dataset in datasets.values():
        html = requests.get(dataset).text
        soup = BeautifulSoup(html, "html.parser")

        for link in soup.find_all("a"):
            href = link.get("href")
            dados = str(link.next_sibling).strip().split()

            if href and dados and href.endswith(".zip"):
                dados.insert(0, dataset + href)
                dados.insert(1, href)
                zips.append(dados)

    return zips

def create_dict(data):
    data_dict = {
        row[0]: {
            "name": row[1],
            "data": row[2],
            "hora": row[3],
            "tam": row[4]
        }
        for row in data
    }

    return data_dict

def get_last_catalog(client):
    try:
        response = client.get_object(
            Bucket=METADATA_BUCKET,
            Key=METADATA_KEY
        )

        content = response["Body"].read()
        return json.loads(content)

    except client.exceptions.NoSuchKey:
        return {}

def check_file_version(data, last_data):
    updates = []

    for key, value in data.items():
        last_value = last_data.get(key)

        if value != last_value:
            updates.append((key, value["name"]))

    return updates

def download_new_files(links, client):
    for link, name in links:
        if "dfp" in link:
            dataset_type = "dfp"
        elif "itr" in link:
            dataset_type = "itr"
        elif "fre" in link:
            dataset_type = "fre"
        else:
            dataset_type = "fca"

        with tempfile.NamedTemporaryFile(suffix=".zip") as temp:
            response = requests.get(link, stream=True)
            response.raise_for_status()

            for chunk in response.iter_content(chunk_size=8192):
                temp.write(chunk)

            temp.flush()

            with zipfile.ZipFile(temp.name, "r") as z:
                erro = z.testzip()

                if erro is not None:
                    raise ValueError(f"Arquivo corrompido: {erro}")

            client.upload_file(
                temp.name,
                "bronze",
                f"cvm/{dataset_type}/{name}"
            )

def save_catalog(client, data):
    client.put_object(
        Bucket=METADATA_BUCKET,
        Key=METADATA_KEY,
        Body=json.dumps(
            data,
            ensure_ascii=False,
            indent=4
        ).encode("utf-8"),
        ContentType="application/json"
    )

def check_catalog():
    client = get_minio_client()
    zips = get_links()
    data = create_dict(zips)
    last_data = get_last_catalog(client)
    links = check_file_version(data, last_data)

    if links:
        download_new_files(links, client)
        save_catalog(client, data)

with DAG(
    dag_id="CMV_catalog",
    start_date=datetime(2026, 8, 30),
    schedule=None,
    catchup=False,
    tags=["bronze", "CMV"]
) as dag:

    CMV_catalog_task = PythonOperator(
        task_id="CMV_catalog",
        python_callable=check_catalog
    )