import pandas as pd
import numpy as np
from config import FILES, PROCESSED_FILES

def process_voting_data():
    try:
        df_votacao = pd.read_csv(
            FILES["votacao"],
            sep=";",
            encoding="latin1",
            usecols=["CD_MUNICIPIO", "DS_CARGO", "NM_VOTAVEL", "QT_VOTOS"]
        )

        # Filtra apenas para o cargo de Deputado Estadual
        df_dep = df_votacao[df_votacao["DS_CARGO"] == "DEPUTADO ESTADUAL"].copy()

        # Calcula o total de votos para Deputado Estadual por município
        total_votos_mun = df_dep.groupby("CD_MUNICIPIO")["QT_VOTOS"].sum().reset_index()
        total_votos_mun.rename(columns={"QT_VOTOS": "total_votos_dep_est"}, inplace=True)

        # Calcula os votos para o candidato específico por município
        df_traiano = df_dep[df_dep["NM_VOTAVEL"] == "ADEMAR LUIZ TRAIANO"].copy()
        votos_traiano_mun = df_traiano.groupby("CD_MUNICIPIO")["QT_VOTOS"].sum().reset_index()
        votos_traiano_mun.rename(columns={"QT_VOTOS": "votos_traiano"}, inplace=True)

        # Junta os dados e calcula o percentual
        df_agg = pd.merge(total_votos_mun, votos_traiano_mun, on="CD_MUNICIPIO", how="left")
        df_agg["votos_traiano"].fillna(0, inplace=True) # Municípios onde ele não teve votos
        df_agg["percentual_traiano"] = (df_agg["votos_traiano"] / df_agg["total_votos_dep_est"]) * 100

        # Mapeia o código do TSE para o do IBGE
        df_mapa = pd.read_csv(FILES["mapa_cod"], usecols=["id_municipio_tse", "id_municipio_ibge"])
        df_final = pd.merge(df_agg, df_mapa, left_on="CD_MUNICIPIO", right_on="id_municipio_tse")
        
        # Seleciona e renomeia colunas finais
        df_final = df_final[["id_municipio_ibge", "percentual_traiano"]]
        df_final.rename(columns={"id_municipio_ibge": "id_municipio"}, inplace=True)

        # Salva o arquivo processado
        df_final.to_csv(PROCESSED_FILES["votacao_dep_traiano"], index=False, header=True, sep=";")
        print(f"Arquivo de votação para Dep. Traiano salvo em: {PROCESSED_FILES['votacao_dep_traiano']}")

    except Exception as e:
        print(f"Um erro ocorreu ao processar os dados de votação: {e}")
        # Cria um arquivo vazio com header para o pipeline não quebrar
        pd.DataFrame(columns=["id_municipio", "percentual_traiano"]).to_csv(
            PROCESSED_FILES["votacao_dep_traiano"], index=False, header=True, sep=";"
        )

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

def process_rais():
    print("Processando RAIS (Otimizado)...")
    df = pd.read_csv(FILES["rais"], sep=",", encoding="utf-8")
    
    # Otimização: Pré-agregar os dados da RAIS
    # A análise final só precisa da média da remuneração por município.
    # Vamos calcular isso agora para reduzir drasticamente o tamanho dos dados.
    
    print("  - Agregando dados para calcular a remuneração média por município...")
    rais_agg = df.groupby('id_municipio')['valor_remuneracao_media_sm'].mean().reset_index()
    rais_agg.rename(columns={'valor_remuneracao_media_sm': 'remuneracao_media'}, inplace=True)
    
    # Salva o arquivo agregado, que é muito menor.
    rais_agg.to_csv(PROCESSED_FILES["rais"], index=False, header=True, sep=";")
    print(f"  - Arquivo RAIS agregado e otimizado salvo em: {PROCESSED_FILES['rais']}")

def process_extra():
    print("Processando Dados Extras (Conectividade)...")
    df = pd.read_csv(FILES["extra"], sep=",", encoding="utf-8")
    df.to_csv(PROCESSED_FILES["extra"], index=False, header=False, sep=";")

def run_all_processing():
    process_voting_data()
    process_census_municipio()
    process_census_sector()
    process_rais()
    process_extra()