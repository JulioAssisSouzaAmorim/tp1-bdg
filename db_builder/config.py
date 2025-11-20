import os

# --- Caminhos de Diretórios ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "../dados")
PROCESSED_DIR = os.path.join(BASE_DIR, "processed_data")

# Certifique-se de que a pasta de processados existe
os.makedirs(PROCESSED_DIR, exist_ok=True)

# --- Arquivos de Entrada ---
FILES = {
    "votacao": os.path.join(DATA_DIR, "votacao_secao_2022_PR.csv"),
    "censo_mun": os.path.join(DATA_DIR, "censo-municipio.csv"),
    "censo_sec": os.path.join(DATA_DIR, "censo-setor-censitario.csv"),
    #"locais_votacao": os.path.join(DATA_DIR, "geometrias_votacao_2022.csv"),
    "rais": os.path.join(DATA_DIR, "RAIS-PR-2022.csv"),
    "extra": os.path.join(DATA_DIR, "IndiceBrConectividadePR2022.csv"),
    "shp_mun": os.path.join(DATA_DIR, "PR_Municipios_2024.shp"),
    "shp_reg_ime": os.path.join(DATA_DIR, "PR_RG_Imediatas_2024.shp"),
    "shp_reg_int": os.path.join(DATA_DIR, "PR_RG_Intermediarias_2024.shp"),
}

# --- Arquivos de Saída (Processados) ---
PROCESSED_FILES = {
    "votacao": os.path.join(PROCESSED_DIR, "resultados_secao.csv"),
    "censo_mun": os.path.join(PROCESSED_DIR, "censo_mun.csv"),
    "censo_sec": os.path.join(PROCESSED_DIR, "censo_sec.csv"),
    #"locais_votacao": os.path.join(PROCESSED_DIR, "mun_zone_section_location.csv"),
    "rais": os.path.join(PROCESSED_DIR, "rais.csv"),
    "extra": os.path.join(PROCESSED_DIR, "extra.csv"),
}

# --- Configurações do Banco de Dados ---
# Altere conforme suas credenciais reais ou use variáveis de ambiente
DB_CONFIG = {
    "user": "usuario",     # Substitua pelo seu usuario
    "password": "senha", # Substitua pela sua senha
    "host": "localhost",
    "port": "5432",
    "dbname": "geodata", # Substitua pelo nome do banco
    "schema": "dados"
}