import os
import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import yfinance as yf
import pandas as pd
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

    stocks = [stock for stock in stocks if not stock.endswith("F")]
    return sorted(set(stocks))


def should_update():
    if not os.path.exists(METADATA_PATH):
        return True, "2010-01-01"

    with open(METADATA_PATH, encoding="utf-8") as f:
        metadata = json.load(f)

    last_update = datetime.fromisoformat(
        metadata["last_update"]
    )
    
    tz_sp = ZoneInfo("America/Sao_Paulo")
    time_sp = int(datetime.now(tz_sp).strftime("%H"))
    
    update = True if (last_update.date() < datetime.now().date()) or (last_update.date() == datetime.now().date() and (time_sp >= 10 and time_sp <= 17)) else False
    start_date = str(last_update.strftime("%Y-%m-%d"))
    
    return update, start_date


def update_metadata():
    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "last_update": datetime.now().isoformat()
            },
            f,
            indent=4
        )

def download_yfinance(stocks, start_date):
    os.makedirs(YFINANCE_PATH, exist_ok=True)

    failed = []

    for stock in stocks:
        ticker = f"{stock}.SA"

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
                data.columns = ["Date", "Adj Close", "Close", "High", "Low", "Open", "Volume"]
                data["Date"] = data["Date"].dt.date
                
                path = os.path.join(YFINANCE_PATH, f"{stock}.csv")

                if os.path.exists(path):
                    historical = pd.read_csv(path)
                    historical = historical.iloc[:-1]

                    data = pd.concat([historical, data], ignore_index=True)

                data.to_csv(path, index=False)

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
    update, start_date = should_update()
    
    if not update:
        return None

    stocks = get_stocks()
    download_yfinance(stocks, start_date)
    update_metadata()

    print(f"{len(stocks)} ações encontradas.")
    return stocks

if __name__ == "__main__":
    stocks = update_stocks()