import requests
import json
import os
import zipfile
import shutil
from bs4 import BeautifulSoup

datasets = {
    "dfp": "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/",
    "itr": "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/ITR/DADOS/"
}

HIST_PATH = "pull_data/cmv/hist_dfp_itr.json"

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
        } for row in data
    }
    
    return data_dict

def check_file_version(data: dict):
    updates = []
    if not os.path.exists(HIST_PATH):
        return [(key, value["name"]) for key, value in data.items()]
    
    with open(HIST_PATH, encoding="utf8") as f:
        last_data = dict(json.load(f))
    
    for key, value in data.items():
        last_value = last_data.get(key)
        if value != last_value:
            updates.append((key, value["name"]))
            
    return updates

def download_new_files(links):
    for link, name in links:
        if "dfp" in link:
            tipo = "dfp"
        else:
            tipo = "itr"
            
        name_no_ext = os.path.splitext(name)[0]
            
        PATH = os.path.join("data", tipo, name_no_ext)
        TEMP_EXTRACT = os.path.join("data", tipo, "temp", name_no_ext)
        TEMP_FILE = os.path.join("data", tipo, "temp", name)
        
        os.makedirs(os.path.join("data", tipo), exist_ok=True)
        os.makedirs(os.path.dirname(TEMP_FILE), exist_ok=True)

        response = requests.get(link, stream=True)
        response.raise_for_status()

        with open(TEMP_FILE, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                
        with zipfile.ZipFile(TEMP_FILE, "r") as z:
            erro = z.testzip()

            if erro is not None:
                raise ValueError(f"Arquivo corrompido: {erro}")
            
        os.makedirs(TEMP_EXTRACT, exist_ok=True)

        with zipfile.ZipFile(TEMP_FILE, "r") as z:
            z.extractall(TEMP_EXTRACT)
            
        if os.path.exists(PATH):
            shutil.rmtree(PATH)

        shutil.move(TEMP_EXTRACT, PATH)

        shutil.rmtree(os.path.dirname(TEMP_FILE))
        
def check_catalog():
    zips = get_links()            
    data = create_dict(zips)
    links = check_file_version(data)
    if links:
        download_new_files(links)
        
        with open(HIST_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
                
if __name__ == "__main__":
    check_catalog()