import os
import unicodedata
import re

# --- Caminhos de Diretórios ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Atualizado para apontar para a raiz do projeto, onde os dados estão agora
DATA_DIR = os.path.join(BASE_DIR, "..") 
PROCESSED_DIR = os.path.join(BASE_DIR, "processed_data")

# Certifique-se de que a pasta de processados existe
os.makedirs(PROCESSED_DIR, exist_ok=True)

# --- Arquivos de Entrada ---
FILES = {
    # Apontando para o arquivo de votação real e descompactado
    "votacao": os.path.join(DATA_DIR, "dados_info", "votacao_secao_2022_PR", "votacao_secao_2022_PR.csv"), 
    "censo_mun": os.path.join(DATA_DIR, "censo-municipio.csv"),
    "censo_sec": os.path.join(DATA_DIR, "censo-setor-censitario.csv"),
    "rais": os.path.join(DATA_DIR, "RAIS-PR-2022.csv"),
    "extra": os.path.join(DATA_DIR, "IndiceBrConectividadePR2022.csv"),
    "shp_mun": os.path.join(DATA_DIR, "dados_info", "PR_Municipios_2022", "PR_Municipios_2022.shp"),
    "mapa_cod": os.path.join(DATA_DIR, "mapa-cod-municipio.csv"),
}

# --- Arquivos de Saída (Processados) ---
PROCESSED_FILES = {
    # Arquivo para a votação do deputado estadual específico (gerado dinamicamente abaixo)
    # A chave "votacao_dep_candidate" será criada a partir de CANDIDATE_NAME
    "censo_mun": os.path.join(PROCESSED_DIR, "censo_mun_processado.csv"),
    "censo_sec": os.path.join(PROCESSED_DIR, "censo_sec_processado.csv"),
    "rais": os.path.join(PROCESSED_DIR, "rais_processado.csv"),
    "extra": os.path.join(PROCESSED_DIR, "extra_processado.csv"),
}

# --- Configuração do candidato alvo (editar conforme necessário) ---
# Nome exato como aparece no arquivo de votação (geralmente em MAIÚSCULAS)
CANDIDATE_NAME = "ALEXANDRE MARANHÃO KHURY"

def _slugify(name: str) -> str:
    # Remove acentos e caracteres não alfanuméricos, converte para minúsculas
    nfkd = unicodedata.normalize('NFKD', name)
    only_ascii = ''.join([c for c in nfkd if not unicodedata.combining(c)])
    slug = re.sub(r'[^0-9a-zA-Z]+', '_', only_ascii).strip('_').lower()
    return slug

CANDIDATE_SLUG = _slugify(CANDIDATE_NAME)

# Arquivo processado para os votos do candidato alvo
PROCESSED_FILES[f"votacao_dep_{CANDIDATE_SLUG}"] = os.path.join(PROCESSED_DIR, f"votacao_dep_{CANDIDATE_SLUG}.csv")

# --- Configurações do Banco de Dados ---
# Altere conforme suas credenciais reais ou use variáveis de ambiente
DB_CONFIG = {
    "user": "usuario",     # Usuário definido no docker-compose.yml
    "password": "senha", # Senha definida no docker-compose.yml
    "host": "localhost",
    "port": "5432",
    "dbname": "geodata", # Nome do banco de dados definido no docker-compose.yml
    "schema": "public"      # Schema padrão
}