import os
import json
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import yfinance as yf
import pandas as pd
from dotenv import load_dotenv

load_dotenv("../../.env")

TOKEN = os.getenv("BRAPI_TOKEN")

URL = "https://brapi.dev/api/v2/tickers"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}"
}

METADATA_PATH = "pull_data/yfinance/stocks_metadata.json"
STOCKS_PATH = "data/stocks" 
SHARES_PATH = "data/shares" 
DIVIDENDS_PATH = "data/dividends"

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
    tz_sp = ZoneInfo("America/Sao_Paulo")
    now = datetime.now(tz_sp)

    if not os.path.exists(METADATA_PATH):
        return True, "2010-01-01"

    with open(METADATA_PATH, encoding="utf-8") as f:
        metadata = json.load(f)

    last_update = datetime.fromisoformat(
        metadata["last_update"]
    )

    if last_update.tzinfo is None:
        last_update = last_update.replace(tzinfo=tz_sp)

    update = (last_update.date() < now.date() or (last_update.date() == now.date() and 10 <= now.hour <= 17))
    start_date = last_update.strftime("%Y-%m-%d")

    return update, start_date


def update_metadata():
    tz_sp = ZoneInfo("America/Sao_Paulo")

    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(
            {
                "last_update": datetime.now(tz_sp).isoformat()
            },
            f,
            indent=4
        )

def download_stocks(stocks, start_date):
    os.makedirs(STOCKS_PATH, exist_ok=True)

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
                
                path = os.path.join(STOCKS_PATH, f"{stock}.csv")

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
        
def download_shares(stocks, start_date):
    os.makedirs(SHARES_PATH, exist_ok=True)

    failed = []

    for stock in stocks:
        ticker = f"{stock}.SA"

        for attempt in range(1, 6):
            print(f"Baixando ações {ticker}... tentativa {attempt}/5")

            try:
                shares = yf.Ticker(ticker).get_shares_full(
                    start=start_date
                )

                if shares is None or shares.empty:
                    print(f"{ticker}: nenhuma informação de ações")
                    break

                data = shares.reset_index()

                data.columns = ["Date", "Shares"]

                # Normaliza a data
                data["Date"] = pd.to_datetime(
                    data["Date"]
                ).dt.date

                path = os.path.join(
                    SHARES_PATH,
                    f"{stock}.csv"
                )

                if os.path.exists(path):
                    historical = pd.read_csv(path)

                    historical["Date"] = pd.to_datetime(
                        historical["Date"]
                    ).dt.date

                    data = pd.concat(
                        [historical, data],
                        ignore_index=True
                    )

                    data = (
                        data
                        .drop_duplicates(subset=["Date"], keep="last")
                        .sort_values("Date")
                        .reset_index(drop=True)
                    )

                data.to_csv(path, index=False)

                print(f"Ações salvas: {path}")
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
    os.makedirs(DIVIDENDS_PATH, exist_ok=True)

    failed = []

    for stock in stocks:
        ticker = f"{stock}.SA"

        for attempt in range(1, 6):
            print(f"Baixando dividendos {ticker}... tentativa {attempt}/5")

            try:
                dividends = yf.Ticker(ticker).dividends

                if dividends.empty:
                    print(f"{ticker}: nenhum dividendo encontrado")
                    break

                dividends.index = dividends.index.tz_localize(None)

                dividends = dividends[
                    dividends.index >= pd.Timestamp(start_date)
                ]

                data = dividends.reset_index()
                data.columns = ["Date", "Dividend"]

                data["Date"] = pd.to_datetime(data["Date"]).dt.date

                path = os.path.join(
                    DIVIDENDS_PATH,
                    f"{stock}.csv"
                )

                if os.path.exists(path):
                    historical = pd.read_csv(path)

                    historical["Date"] = pd.to_datetime(
                        historical["Date"]
                    ).dt.date

                    data = pd.concat(
                        [historical, data],
                        ignore_index=True
                    )

                    data = (
                        data
                        .drop_duplicates(subset=["Date"], keep="last")
                        .sort_values("Date")
                        .reset_index(drop=True)
                    )

                data.to_csv(path, index=False)

                print(f"Dividendos salvos: {path}")
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
        return None

    stocks = get_stocks()
    download_stocks(stocks, start_date)
    download_dividends(stocks, start_date)
    download_shares(stocks, start_date)
    update_metadata()

    print(f"{len(stocks)} ações encontradas.")
    return stocks

if __name__ == "__main__":
    stocks = update_stocks()
