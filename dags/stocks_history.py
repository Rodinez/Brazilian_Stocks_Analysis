import os
import io
import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import boto3
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv

from airflow import DAG
from airflow.operators.python import PythonOperator

load_dotenv("/opt/airflow/.env")

TOKEN = os.getenv("BRAPI_TOKEN")
URL = "https://brapi.dev/api/v2/tickers"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}"
}

METADATA_PATH = "/opt/airflow/data/state/yfinance/stocks_metadata.json"

MINIO_BUCKET = "bronze"

STOCKS_PREFIX = "yfinance/stocks"
SHARES_PREFIX = "yfinance/shares"
DIVIDENDS_PREFIX = "yfinance/dividends"

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ROOT_USER"],
        aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"]
    )

def get_stocks():
    stocks = []
    page = 1

    while True:
        params = {
            "type": "stock",
            "sortBy": "symbol",
            "sortOrder": "asc",
            "page": page,
            "limit": 100
        }

        response = requests.get(URL, headers=HEADERS, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        for stock in data["results"]:
            if stock["isActive"]:
                stocks.append(stock["symbol"])

        if not data["pagination"]["hasNextPage"]:
            break

        page += 1

    stocks = [stock for stock in stocks if not stock.endswith("F")]

    return sorted(set(stocks))

def should_update():
    tz_sp = ZoneInfo("America/Sao_Paulo")
    now = datetime.now(tz_sp)

    if not os.path.exists(METADATA_PATH):
        return True, "2010-01-01"

    with open(METADATA_PATH, encoding="utf-8") as f:
        metadata = json.load(f)

    last_update = datetime.fromisoformat(metadata["last_update"])

    if last_update.tzinfo is None:
        last_update = last_update.replace(tzinfo=tz_sp)

    update = (
        last_update.date() < now.date() or (last_update.date() == now.date() and 10 <= now.hour <= 17)
    )

    start_date = last_update.strftime("%Y-%m-%d")

    return update, start_date

def update_metadata():
    tz_sp = ZoneInfo("America/Sao_Paulo")

    os.makedirs(os.path.dirname(METADATA_PATH), exist_ok=True)

    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {"last_update": datetime.now(tz_sp).isoformat()},
            f,
            indent=4
        )

def get_previous_csv(client, key):
    try:
        response = client.get_object(Bucket=MINIO_BUCKET, Key=key)
        content = response["Body"].read()

        return pd.read_csv(io.BytesIO(content))

    except client.exceptions.NoSuchKey:
        return None

    except Exception as e:
        if "NoSuchKey" in str(e) or "specified key does not exist" in str(e):
            return None
        raise

def upload_csv(client, data, key):
    buffer = io.BytesIO()
    data.to_csv(buffer, index=False)
    buffer.seek(0)

    client.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=buffer.getvalue(),
        ContentType="text/csv"
    )

def download_stocks(stocks, start_date):
    client = get_minio_client()
    failed = []

    for stock in stocks:
        ticker = f"{stock}.SA"
        key = f"{STOCKS_PREFIX}/{stock}.csv"

        for attempt in range(1, 6):
            print(f"Baixando {ticker}... tentativa {attempt}/5")

            try:
                data = yf.download(
                    ticker,
                    start=start_date,
                    auto_adjust=False,
                    progress=False
                )

                if data.empty:
                    raise ValueError("Sem dados")

                if len(data) < 100:
                    print(f"{ticker}: apenas {len(data)} registros")

                data = data.reset_index()
                data.columns = [
                    "Date",
                    "Adj Close",
                    "Close",
                    "High",
                    "Low",
                    "Open",
                    "Volume"
                ]
                data["Date"] = data["Date"].dt.date

                historical = get_previous_csv(client, key)

                if historical is not None:
                    historical["Date"] = pd.to_datetime(historical["Date"]).dt.date
                    historical = historical.iloc[:-1]

                    data = pd.concat([historical, data], ignore_index=True)

                data = (
                    data
                    .drop_duplicates(subset=["Date"], keep="last")
                    .sort_values("Date")
                    .reset_index(drop=True)
                )

                upload_csv(client, data, key)

                print(f"Salvo no Bronze: s3://{MINIO_BUCKET}/{key}")
                break

            except Exception as e:
                print(f"Falha em {ticker}: {e}")

                if attempt == 5:
                    failed.append(ticker)
                    print(f"FALHOU 5 VEZES: {ticker}")

    print("\nTickers que falharam:")

    for ticker in failed:
        print(ticker)

def download_shares(stocks, start_date):
    client = get_minio_client()
    failed = []

    for stock in stocks:
        ticker = f"{stock}.SA"
        key = f"{SHARES_PREFIX}/{stock}.csv"

        for attempt in range(1, 6):
            print(f"Baixando ações {ticker}... tentativa {attempt}/5")

            try:
                shares = yf.Ticker(ticker).get_shares_full(start=start_date)

                if shares is None or shares.empty:
                    print(f"{ticker}: nenhuma informação de ações")
                    break

                data = shares.reset_index()
                data.columns = ["Date", "Shares"]
                data["Date"] = pd.to_datetime(data["Date"]).dt.date

                historical = get_previous_csv(client, key)

                if historical is not None:
                    historical["Date"] = pd.to_datetime(historical["Date"]).dt.date

                    data = pd.concat([historical, data], ignore_index=True)

                    data = (
                        data
                        .drop_duplicates(subset=["Date"], keep="last")
                        .sort_values("Date")
                        .reset_index(drop=True)
                    )

                upload_csv(client, data, key)

                print(f"Ações salvas no Bronze: s3://{MINIO_BUCKET}/{key}")
                break

            except Exception as e:
                print(f"Falha em ações {ticker}: {e}")

                if attempt == 5:
                    failed.append(ticker)
                    print(f"FALHOU 5 VEZES: {ticker}")

    print("\nTickers de ações que falharam:")

    for ticker in failed:
        print(ticker)

def download_dividends(stocks, start_date):
    client = get_minio_client()
    failed = []

    for stock in stocks:
        ticker = f"{stock}.SA"
        key = f"{DIVIDENDS_PREFIX}/{stock}.csv"

        for attempt in range(1, 6):
            print(f"Baixando dividendos {ticker}... tentativa {attempt}/5")

            try:
                dividends = yf.Ticker(ticker).dividends

                if dividends.empty:
                    print(f"{ticker}: nenhum dividendo encontrado")
                    break

                dividends.index = dividends.index.tz_localize(None)
                dividends = dividends[dividends.index >= pd.Timestamp(start_date)]

                data = dividends.reset_index()
                data.columns = ["Date", "Dividend"]
                data["Date"] = pd.to_datetime(data["Date"]).dt.date

                historical = get_previous_csv(client, key)

                if historical is not None:
                    historical["Date"] = pd.to_datetime(historical["Date"]).dt.date

                    data = pd.concat([historical, data], ignore_index=True)

                    data = (
                        data
                        .drop_duplicates(subset=["Date"], keep="last")
                        .sort_values("Date")
                        .reset_index(drop=True)
                    )

                upload_csv(client, data, key)

                print(f"Dividendos salvos no Bronze: s3://{MINIO_BUCKET}/{key}")
                break

            except Exception as e:
                print(f"Falha em dividendos {ticker}: {e}")

                if attempt == 5:
                    failed.append(ticker)
                    print(f"FALHOU 5 VEZES: {ticker}")

    print("\nTickers de dividendos que falharam:")
    for ticker in failed:
        print(ticker)


def update_stocks():
    update, start_date = should_update()

    if not update:
        print("Nenhuma atualização necessária.")
        return None

    stocks = get_stocks()
    download_stocks(stocks, start_date)
    download_dividends(stocks, start_date)
    download_shares(stocks, start_date)

    update_metadata()
    print(f"{len(stocks)} ações encontradas.")
    return stocks

with DAG(
    dag_id="stocks_history",
    start_date=datetime(2026, 8, 30),
    schedule=None,
    catchup=False,
    tags=["bronze", "yfinance"]
) as dag:

    update_stocks_task = PythonOperator(
        task_id="update_stocks",
        python_callable=update_stocks
    )