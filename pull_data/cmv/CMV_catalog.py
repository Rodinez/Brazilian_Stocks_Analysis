import requests
import json
import os
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
        with open(HIST_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
            return None
    
    with open(HIST_PATH, encoding="utf8") as f:
        last_data = dict(json.load(f))
    
    for key, value in data.items():
        last_value = last_data.get(key)
        if value != last_value:
            updates.append((key, value[0]))
            
    return updates

def download_new_files(links):
    for link, name in links:
        href = link

        if "dfp" in link:
            PATH = f"data/dfp/{name}"
            TEMP_PATH = "data/dfp/temp"
        else:
            PATH = f"data/itr/{name}"
            TEMP_PATH = "data/itr/temp"

        response = requests.get(link, stream=True)
        response.raise_for_status()

        with open(TEMP_PATH, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    
def check_catalog():
    zips = get_links()            
    data = create_dict(zips)
    links = check_file_version(data)
    #if links:
        #download_new_files(links)
                
if __name__ == "__main__":
    check_catalog()