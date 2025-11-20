import pandas as pd
import numpy as np
from config import FILES, PROCESSED_FILES

def process_voting_data():
    print("Processando dados de Votação...")
    df = pd.read_csv(FILES["votacao"], sep=";", encoding="latin-1")
    
    # Filtros
    df = df[df["DS_CARGO"] == "DEPUTADO ESTADUAL"]
    
    columns = [
        "CD_MUNICIPIO", "NM_MUNICIPIO", "NR_ZONA", "NR_SECAO",
        "NR_VOTAVEL", "NM_VOTAVEL", "QT_VOTOS", "SQ_CANDIDATO",
        "NR_LOCAL_VOTACAO", "NM_LOCAL_VOTACAO", "DS_LOCAL_VOTACAO_ENDERECO"
    ]
    df = df[columns]
    
    # Limpeza de valores numéricos inválidos (menor que 0)
    numeric_cols = ["CD_MUNICIPIO", "NR_ZONA", "NR_SECAO", "NR_VOTAVEL", 
                    "QT_VOTOS", "SQ_CANDIDATO", "NR_LOCAL_VOTACAO"]
    
    # Remove linhas onde qualquer coluna numérica seja negativa
    df = df[(df[numeric_cols] >= 0).all(axis=1)]
    
    # Salvar
    df.to_csv(PROCESSED_FILES["votacao"], index=False, header=False, sep=";")
    print(f"Votação salva em: {PROCESSED_FILES['votacao']}")

def process_census_municipio():
    print("Processando Censo Município...")
    df = pd.read_csv(FILES["censo_mun"], sep=",", encoding="utf-8")
    
    df = df[df["sigla_uf"] == "PR"]
    
    columns = [
        "id_municipio", "domicilios", "populacao", "area",
        "taxa_alfabetizacao", "idade_mediana", "razao_sexo",
        "indice_envelhecimento"
    ]
    df = df[columns]
    
    df.to_csv(PROCESSED_FILES["censo_mun"], index=False, header=False, sep=";")

def process_census_sector():
    print("Processando Censo Setor...")
    df = pd.read_csv(FILES["censo_sec"], sep=",", encoding="utf-8")
    
    columns = [
        "id_municipio", "id_setor_censitario", "pessoas", "domicilios",
        "media_moradores_domicilios", "area", "geometria"
    ]
    df = df[columns]
    
    df.to_csv(PROCESSED_FILES["censo_sec"], index=False, header=False, sep=";")

def process_voting_places():
    print("Processando Locais de Votação...")
    df = pd.read_csv(FILES["locais_votacao"], sep=",", encoding="utf-8")
    
    columns = ["id_municipio", "zona", "secao", "melhor_urbano"]
    df = df[columns].dropna()
    
    df.to_csv(PROCESSED_FILES["locais_votacao"], index=False, header=False, sep=";")

def process_rais():
    print("Processando RAIS...")
    df = pd.read_csv(FILES["rais"], sep=",", encoding="utf-8")
    # O notebook original não aplicava filtros específicos na RAIS além de ler/salvar
    df.to_csv(PROCESSED_FILES["rais"], index=False, header=False, sep=";")

def process_extra():
    print("Processando Dados Extras (Conectividade)...")
    df = pd.read_csv(FILES["extra"], sep=",", encoding="utf-8")
    df.to_csv(PROCESSED_FILES["extra"], index=False, header=False, sep=";")

def run_all_processing():
    process_voting_data()
    process_census_municipio()
    process_census_sector()
    #process_voting_places()
    process_rais()
    process_extra()