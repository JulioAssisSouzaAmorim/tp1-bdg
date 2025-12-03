import psycopg2
from sqlalchemy import create_engine
import geopandas as gpd
from config import DB_CONFIG, PROCESSED_FILES, FILES
import importlib

class DatabaseManager:
    def __init__(self):
        # Carrega a configuração do arquivo config.py
        self.db_config = DB_CONFIG
        self.schema = self.db_config.get('schema', 'public')
        
        # String de conexão para SQLAlchemy
        self.conn_str = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        
        # Conexão Psycopg2
        self.conn = psycopg2.connect(
            dbname=self.db_config['dbname'],
            user=self.db_config['user'],
            password=self.db_config['password'],
            host=self.db_config['host'],
            port=self.db_config['port'],
            options=f"-c search_path={self.schema},public"
        )
        self.cur = self.conn.cursor()
        self.engine = create_engine(self.conn_str)

    def create_schema(self):
        """Cria o schema se não existir"""
        print(f"Verificando schema '{self.schema}'...")
        self.cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema};")
        self.conn.commit()

    def create_tables(self):
        self.create_schema() # Garante que o schema existe antes de criar tabelas
        print(f"Criando tabelas no schema '{self.schema}'...")
        
        # Como definimos o search_path na conexão, não é estritamente necessário 
        # prefixar com o schema aqui, mas é uma boa prática para DDL.
        # Vamos usar f-strings para injetar o schema.
        
        # Determina a tabela de votação do candidato a partir da config
        config = importlib.import_module('config')
        candidate_slug = getattr(config, 'CANDIDATE_SLUG', 'khury')
        vot_table = f"votacao_dep_{candidate_slug}"

        queries = [
            f"""
            DROP TABLE IF EXISTS {self.schema}.{vot_table};
            CREATE TABLE {self.schema}.{vot_table} (
                id_municipio int,
                percentual_candidato float
            );
            """,
            f"""
            DROP TABLE IF EXISTS {self.schema}.censo_mun;
            CREATE TABLE {self.schema}.censo_mun (
                id_municipio int, domicilios int, populacao int, area int,
                taxa_alfabetizacao float, idade_mediana int, razao_sexo float,
                indice_envelhecimento float
            );
            """,
            f"""
            DROP TABLE IF EXISTS {self.schema}.censo_sec;
            CREATE TABLE {self.schema}.censo_sec (
                id_municipio int, id_setor_censitario bigint, pessoas int,
                domicilios int, media_moradores_domicilios float, area float,
                geometria text -- Alterado para text para evitar problemas com geography
            );
            """,
            f"""
            DROP TABLE IF EXISTS {self.schema}.rais_agg;
            CREATE TABLE {self.schema}.rais_agg (
                id_municipio int,
                remuneracao_media float
            );
            """,
            f"""
            DROP TABLE IF EXISTS {self.schema}.extra;
            CREATE TABLE {self.schema}.extra (
                ano int, sigla_uf varchar, id_municipio int, ibc float,
                cobertura_pop_4g5g float, fibra int, densidade_smp float,
                hhi_smp int, densidade_scm float, hhi_scm int, adensamento_estacoes float
            );
            """
        ]
        
        for query in queries:
            self.cur.execute(query)
        self.conn.commit()

    def load_csv_data(self):
        print(f"Carregando CSVs no schema '{self.schema}'...")
        
        # Mapeamento para arquivos SEM cabeçalho
        mappings_no_header = [
            (PROCESSED_FILES["censo_mun"], "censo_mun"),
            (PROCESSED_FILES["censo_sec"], "censo_sec"),
            (PROCESSED_FILES["extra"], "extra"),
        ]

        for file_path, table_name in mappings_no_header:
            print(f"-> Carregando {table_name} (sem header)...")
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    self.cur.copy_from(f, table=table_name, sep=";")
                    self.conn.commit()
            except Exception as e:
                print(f"Erro ao carregar {table_name}: {e}")
                self.conn.rollback()

        # Tratamento especial para arquivos COM header
        # Carrega arquivos com header: votacao do candidato e rais agregada
        mappings_with_header = []
        # Adiciona o arquivo de votação do candidato, se existir na configuração
        candidate_key = None
        for k in PROCESSED_FILES.keys():
            if k.startswith('votacao_dep_'):
                candidate_key = k
                break
        if candidate_key:
            mappings_with_header.append((PROCESSED_FILES[candidate_key], vot_table))
        mappings_with_header.append((PROCESSED_FILES["rais"], "rais_agg"))
        
        for file_path, table_name in mappings_with_header:
            print(f"-> Carregando {table_name} (com header)...")
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    copy_sql = f"COPY {self.schema}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ';'"
                    self.cur.copy_expert(sql=copy_sql, file=f)
                    self.conn.commit()
            except Exception as e:
                print(f"Erro ao carregar {table_name}: {e}")
                self.conn.rollback()

    def load_shapefiles(self):
        print(f"Carregando Shapefiles no schema '{self.schema}'...")
        
        shp_mappings = [
            (FILES["shp_mun"], "municipios_pr_2022"),
        ]

        for file_path, table_name in shp_mappings:
            print(f"-> Carregando GeoData {table_name}...")
            try:
                gdf = gpd.read_file(file_path)
                
                gdf.to_postgis(
                    table_name, 
                    self.engine, 
                    schema=self.schema,
                    if_exists="replace", 
                    index=False
                )
            except Exception as e:
                print(f"Erro ao carregar shapefile {table_name}: {e}")

    def close(self):
        self.cur.close()
        self.conn.close()
        self.engine.dispose()
        print("Conexão encerrada.")