import psycopg2
from sqlalchemy import create_engine, text
import geopandas as gpd
from config import DB_CONFIG, PROCESSED_FILES, FILES

class DatabaseManager:
    def __init__(self):
        self.schema = DB_CONFIG.get('schema', 'public')
        
        # String de conexão para SQLAlchemy
        self.conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        
        # Conexão Psycopg2
        self.conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            # Dica: Configura o search_path na conexão para facilitar o COPY
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
        
        queries = [
            f"""
            DROP TABLE IF EXISTS {self.schema}.resultados_secao;
            CREATE TABLE {self.schema}.resultados_secao (
                cd_municipio int, nm_municipio varchar, nr_zona int, nr_secao int,
                nr_votavel int, nm_votavel varchar, qt_votos int, sq_candidato bigint,
                nr_local_votacao int, nm_local_votacao varchar, ds_local_votacao_endereco varchar
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
                geom geography
            );
            """,
            f"""
            DROP TABLE IF EXISTS {self.schema}.mun_zona_sec_local;
            CREATE TABLE {self.schema}.mun_zona_sec_local (
                id_municipio int, zona int, secao int, geom geography
            );
            """,
            f"""
            DROP TABLE IF EXISTS {self.schema}.rais;
            CREATE TABLE {self.schema}.rais (
                ano int, sigla_uf varchar, id_municipio int, tipo_vinculo int,
                vinculo_ativo_3112 int, tipo_admissao int, tempo_emprego float,
                quantidade_horas_contratadas int, valor_remuneracao_media_sm float,
                valor_remuneracao_dezembro_sm float, cbo_2002 int, cnae_2 int,
                cnae_2_subclasse int, idade int, grau_instrucao_apos_2005 int,
                nacionalidade int, sexo int, raca_cor int, indicador_portador_deficiencia int,
                tipo_deficiencia int
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
        
        mappings = [
            (PROCESSED_FILES["votacao"], "resultados_secao"),
            (PROCESSED_FILES["censo_mun"], "censo_mun"),
            (PROCESSED_FILES["censo_sec"], "censo_sec"),
            #(PROCESSED_FILES["locais_votacao"], "mun_zona_sec_local"),
            (PROCESSED_FILES["rais"], "rais"),
            (PROCESSED_FILES["extra"], "extra"),
        ]

        for file_path, table_name in mappings:
            print(f"-> Carregando {table_name}...")
            with open(file_path, "r", encoding="utf-8") as f: 
                try:
                    # IMPORTANTE: Como definimos 'options="-c search_path=..."' na conexão,
                    # o copy_from vai procurar a tabela no schema correto automaticamente.
                    # Se não tivesse definido lá, teria que usar f"{self.schema}.{table_name}" aqui.
                    self.cur.copy_from(f, table=table_name, sep=";")
                    self.conn.commit()
                except Exception as e:
                    print(f"Erro ao carregar {table_name}: {e}")
                    self.conn.rollback()

    def load_shapefiles(self):
        print(f"Carregando Shapefiles no schema '{self.schema}'...")
        
        shp_mappings = [
            (FILES["shp_mun"], "geo_mun"),
            (FILES["shp_reg_ime"], "geo_reg_ime"),
            (FILES["shp_reg_int"], "geo_reg_int")
        ]

        for file_path, table_name in shp_mappings:
            print(f"-> Carregando GeoData {table_name}...")
            try:
                gdf = gpd.read_file(file_path)
                
                # AQUI ESTÁ O PULO DO GATO PARA GEOPANDAS:
                # Use o parâmetro 'schema'
                gdf.to_postgis(
                    table_name, 
                    self.engine, 
                    schema=self.schema,  # <--- Especifica o schema aqui
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