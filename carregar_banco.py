import os
import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine
from tqdm import tqdm
import chardet

engine = create_engine(
    f"postgresql+psycopg2://usuario:senha@localhost:5433/geodata"
)

base_path = "dados"

def salvar_csv(path):
    nome_tabela = os.path.splitext(os.path.basename(path))[0].lower()
    print(f"📄 Importando CSV: {nome_tabela}")
    with open(path, 'rb') as f:
        enc = chardet.detect(f.read(50000))['encoding']
    df = pd.read_csv(path, encoding=enc)
    df.to_sql(nome_tabela, engine, if_exists="replace", index=False)
    print(f"✅ Tabela '{nome_tabela}' criada ({len(df)} linhas)")

def salvar_shapefile(path):
    nome_tabela = os.path.splitext(os.path.basename(path))[0].lower()
    print(f"🗺️ Importando Shapefile: {nome_tabela}")
    gdf = gpd.read_file(path)
    gdf.to_postgis(nome_tabela, engine, if_exists="replace", index=False)
    print(f"✅ Tabela '{nome_tabela}' criada ({len(gdf)} geometrias)")

def varrer_pasta(base_path):
    for root, _, files in os.walk(base_path):
        for file in tqdm(files, desc=f"Varredura em {root}"):
            path = os.path.join(root, file)
            if file.lower().endswith(".csv"):
                salvar_csv(path)
            elif file.lower().endswith(".shp"):
                salvar_shapefile(path)

if __name__ == "__main__":
    print("🚀 Iniciando importação de dados...")
    varrer_pasta(base_path)
    print("🎉 Importação concluída!")
