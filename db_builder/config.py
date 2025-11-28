import os

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
    # Novo arquivo para a votação do deputado estadual específico
    "votacao_dep_traiano": os.path.join(PROCESSED_DIR, "votacao_dep_traiano.csv"),
    "censo_mun": os.path.join(PROCESSED_DIR, "censo_mun_processado.csv"),
    "censo_sec": os.path.join(PROCESSED_DIR, "censo_sec_processado.csv"),
    "rais": os.path.join(PROCESSED_DIR, "rais_processado.csv"),
    "extra": os.path.join(PROCESSED_DIR, "extra_processado.csv"),
}

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