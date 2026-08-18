import os
import json
import requests
from datetime import datetime
import yfinance as yf
from dotenv import load_dotenv

load_dotenv("../.env")

TOKEN = os.getenv("BRAPI_TOKEN")

URL = "https://brapi.dev/api/v2/tickers"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}"
}

METADATA_PATH = "pull_data/yfinance/metadata.json"
YFINANCE_PATH = "data/yfinance" 

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

        response = requests.get(
            URL,
            headers=HEADERS,
            params=params,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        for stock in data["results"]:
            if stock["isActive"]:
                stocks.append(stock["symbol"])

        if not data["pagination"]["hasNextPage"]:
            break

        page += 1

    return sorted(set(stocks))


def should_update():
    if not os.path.exists(METADATA_PATH):
        return True

    with open(METADATA_PATH, encoding="utf-8") as f:
        metadata = json.load(f)

    last_update = datetime.fromisoformat(
        metadata["last_update"]
    )

    return last_update.date() < datetime.now().date()


def update_metadata():
    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "last_update": datetime.now().isoformat()
            },
            f,
            indent=4
        )

def download_yfinance(stocks):
    os.makedirs(YFINANCE_PATH, exist_ok=True)

    failed = []

    for stock in stocks:
        ticker = f"{stock}.SA"

        for attempt in range(1, 6):
            print(f"Baixando {ticker}... tentativa {attempt}/5")

            try:
                data = yf.download(
                    ticker,
                    start="2010-01-01",
                    auto_adjust=False,
                    progress=False
                )

                if data.empty:
                    raise ValueError("Sem dados")

                path = os.path.join(
                    YFINANCE_PATH,
                    f"{stock}.csv"
                )

                data.to_csv(path)

                print(f"Salvo: {path}")
                break

            except Exception as e:
                print(f"Falha em {ticker}: {e}")

                if attempt == 5:
                    failed.append(ticker)
                    print(f"FALHOU 5 VEZES: {ticker}")

    print("\nTickers que falharam:")
    for ticker in failed:
        print(ticker)

def update_stocks():
    if not should_update():
        print("Lista de ações já consultada hoje.")
        return None

    stocks = get_stocks()

    download_yfinance(stocks)

    update_metadata()

    print(f"{len(stocks)} ações encontradas.")

    return stocks


if __name__ == "__main__":
    stocks = update_stocks()