import requests
import pandas as pd
from sqlalchemy import create_engine

# função extraction
def extract()-> dict:
    API_URL = "http://www.universidades.com.br/brasil.htm"
    data = requests.get(API_URL).json()
    return data


# função transformação
def transform(data:dict) -> pd.DataFrame:
    df = pd.DataFrame(data)
    print(f"Número total de Universidade no API {len(data)}")
    df = df[df["name"].str.contains("Universidade Federal")]
    print(f"Numero de universidades federais {len(df)}")
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains","country","web_pages","name"]]

# função carregar
def load(df:pd.DataFrame)-> None:
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('UF_uni', disk_engine, if_exists='replace')


data = extract()
df = transform(data)
load(df)