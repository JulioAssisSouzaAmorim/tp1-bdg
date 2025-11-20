import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sqlalchemy import text
from libpysal.weights.contiguity import Queen
from esda.moran import Moran, Moran_BV
from splot.esda import plot_moran, moran_scatterplot, lisa_cluster
from db_manager import DatabaseManager

# Configurações visuais
sns.set_theme(style="whitegrid")

class SpatialMetricsAnalysis:
    def __init__(self):
        self.db = DatabaseManager()
        self.gdf_mun = self.load_geometries()
        
    def load_geometries(self):
        """Carrega a geometria dos municípios e informações de regiões"""
        print("Carregando geometrias...")
        query = f"""SELECT * FROM {self.db.schema}.geo_mun"""
        gdf = gpd.read_postgis(query, self.db.engine, geom_col='geometry')
        
        # Garante que as colunas de código sejam inteiros para os joins funcionarem
        # Ajuste os nomes das colunas conforme sua tabela real (maiúsculo/minúsculo)
        if "CD_MUN_IBG" in gdf.columns:
            gdf['CD_MUN_IBG'] = gdf['CD_MUN_IBG'].astype(int)
        if "CD_MUN_TSE" in gdf.columns:
            gdf['CD_MUN_TSE'] = gdf['CD_MUN_TSE'].astype(int)
            
        return gdf

    def calculate_moran_i(self, gdf, variable_col, title="Moran's I"):
        """Calcula o I de Moran Global e exibe o resultado"""
        # Remove NaNs e geometrias vazias para evitar erros matemáticos
        gdf_clean = gdf.dropna(subset=[variable_col])
        gdf_clean = gdf_clean[~gdf_clean.is_empty]
        
        if len(gdf_clean) < 5:
            print(f"--- {title} ---")
            print("Dados insuficientes para calcular Moran I.")
            return None, None
        
        try:
            # Cria matriz de pesos espaciais (Queen contiguity)
            w = Queen.from_dataframe(gdf_clean)
            w.transform = 'r' # Padronização por linha
            
            # Calcula Moran's I
            y = gdf_clean[variable_col].values
            moran = Moran(y, w)
            
            print(f"--- {title} ---")
            print(f"I de Moran: {moran.I:.4f}")
            print(f"p-valor: {moran.p_sim:.4f}")
            conclusion = "Autocorrelação Espacial Significativa" if moran.p_sim < 0.05 else "Aleatório (Sem padrão espacial claro)"
            print(f"Conclusão: {conclusion}")
            print("-" * 30)
            
            return moran, w
        except Exception as e:
            print(f"Erro ao calcular Moran para {title}: {e}")
            return None, None

    def get_votes_by_candidate(self, candidate_name):
        """Retorna GeoDataFrame com % de votos do candidato por município"""
        # Usando aspas duplas para garantir Case Sensitivity nos nomes das colunas
        query = f"""
        WITH total_mun AS (
            SELECT cd_municipio, SUM(qt_votos) as total_votos
            FROM {self.db.schema}.resultados_secao GROUP BY 1
        ),
        cand_votos AS (
            SELECT cd_municipio, SUM(qt_votos) as votos_cand
            FROM {self.db.schema}.resultados_secao
            WHERE nm_votavel = '{candidate_name}'
            GROUP BY 1
        )
        SELECT 
            m."CD_MUN_IBG",
            m."CD_MUN_TSE",
            m.geometry,
            COALESCE(c.votos_cand, 0) * 100.0 / t.total_votos as pct_votos
        FROM {self.db.schema}.geo_mun m
        JOIN total_mun t ON CAST(m."CD_MUN_TSE" AS INTEGER) = t.cd_municipio
        LEFT JOIN cand_votos c ON CAST(m."CD_MUN_TSE" AS INTEGER) = c.cd_municipio
        """
        return gpd.read_postgis(query, self.db.engine, geom_col='geometry')

    def analyze_autocorrelation_candidates(self):
        """
        A. Autocorrelação espacial: candidatos escolhidos (Regional vs Amplo, Eleito vs Não-Eleito)
        """
        print("\n=== A. ANÁLISE DE AUTOCORRELAÇÃO POR CANDIDATO ===")
        
        # Consultas SQL
        top1_query = f"SELECT nm_votavel FROM {self.db.schema}.resultados_secao GROUP BY 1 ORDER BY SUM(qt_votos) DESC LIMIT 1"
        # Pega alguém da posição 100 para ser o 'regional/médio'
        mid_query = f"SELECT nm_votavel FROM {self.db.schema}.resultados_secao GROUP BY 1 ORDER BY SUM(qt_votos) DESC OFFSET 100 LIMIT 1"
        
        # CORREÇÃO: Usar engine.connect() para ter acesso ao exec_driver_sql
        with self.db.engine.connect() as conn:
            cand_amplo = conn.exec_driver_sql(top1_query).scalar()
            cand_regional = conn.exec_driver_sql(mid_query).scalar()
        
        candidates = {
            'Votação Ampla / Eleito': cand_amplo,
            'Votação Menor / Regional': cand_regional
        }
        
        for label, name in candidates.items():
            if name:
                print(f"\nProcessando: {name} ({label})")
                gdf = self.get_votes_by_candidate(name)
                self.calculate_moran_i(gdf, 'pct_votos', title=f"Moran I - {name}")

    def analyze_aggregated_levels(self):
        """
        B. Autocorrelação em nível agregado (Regiões Imediatas e Intermediárias)
        """
        print("\n=== B. ANÁLISE EM NÍVEIS AGREGADOS ===")
        
        top1_query = f"SELECT nm_votavel FROM {self.db.schema}.resultados_secao GROUP BY 1 ORDER BY SUM(qt_votos) DESC LIMIT 1"
        
        # CORREÇÃO: Usar engine.connect()
        with self.db.engine.connect() as conn:
            target_cand = conn.exec_driver_sql(top1_query).scalar()
            
        print(f"Candidato de referência: {target_cand}")
        
        # Níveis de agregação baseados nas colunas da sua tabela geo_mun
        levels = {
            'Região Imediata': 'NM_RGI',      # Ajuste se o nome da coluna for diferente (ex: nm_rgi)
            'Região Intermediária': 'NM_RGINT' # Ajuste se o nome da coluna for diferente
        }
        
        for label, col_agregacao in levels.items():
            # Query para agregar votos e unir geometrias
            # IMPORTANTE: Usamos aspas duplas em col_agregacao para respeitar o case sensitive da sua tabela
            query = f"""
            SELECT 
                g."{col_agregacao}" as regiao,
                ST_Union(g.geometry) as geometry,
                SUM(CASE WHEN r.nm_votavel = '{target_cand}' THEN r.qt_votos ELSE 0 END) * 100.0 / NULLIF(SUM(r.qt_votos), 0) as pct_votos
            FROM {self.db.schema}.geo_mun g
            JOIN {self.db.schema}.resultados_secao r ON CAST(g."CD_MUN_TSE" AS INTEGER) = r.cd_municipio
            GROUP BY 1
            """
            try:
                gdf_agg = gpd.read_postgis(query, self.db.engine, geom_col='geometry')
                self.calculate_moran_i(gdf_agg, 'pct_votos', title=f"Moran I Agregado - {label}")
            except Exception as e:
                print(f"Erro na agregação {label}: {e}")

    def analyze_socioeconomic_correlation(self):
        """
        C. e D. Correlação com dados socioeconômicos (RAIS/Censo) e Conectividade
        """
        print("\n=== C & D. CORRELAÇÃO SOCIOECONÔMICA E CONECTIVIDADE ===")
        
        top1_query = f"SELECT nm_votavel FROM {self.db.schema}.resultados_secao GROUP BY 1 ORDER BY SUM(qt_votos) DESC LIMIT 1"
        
        # CORREÇÃO: Usar engine.connect()
        with self.db.engine.connect() as conn:
            cand_name = conn.exec_driver_sql(top1_query).scalar()
        
        # Montar Dataset Completo
        # Atenção aos nomes das colunas nas tabelas censo_mun, rais, extra
        query = f"""
        WITH votos AS (
            SELECT cd_municipio, SUM(qt_votos) as total,
                   SUM(CASE WHEN nm_votavel = '{cand_name}' THEN qt_votos ELSE 0 END) as votos_cand
            FROM {self.db.schema}.resultados_secao GROUP BY 1
        ),
        rais_agg AS (
             SELECT id_municipio, AVG(valor_remuneracao_media_sm) as remuneracao_media
             FROM {self.db.schema}.rais GROUP BY 1
        )
        SELECT 
            g."CD_MUN_IBG",
            g.geometry,
            (v.votos_cand * 100.0 / NULLIF(v.total, 0)) as pct_votos,
            -- Dados Censo
            c.taxa_alfabetizacao,
            c.indice_envelhecimento,
            -- Dados RAIS
            r.remuneracao_media,
            -- Dados Extra (Conectividade)
            e.cobertura_pop_4g5g
        FROM {self.db.schema}.geo_mun g
        JOIN votos v ON CAST(g."CD_MUN_TSE" AS INTEGER) = v.cd_municipio
        LEFT JOIN {self.db.schema}.censo_mun c ON CAST(g."CD_MUN_IBG" AS INTEGER) = c.id_municipio
        LEFT JOIN rais_agg r ON CAST(g."CD_MUN_IBG" AS INTEGER) = r.id_municipio
        LEFT JOIN {self.db.schema}.extra e ON CAST(g."CD_MUN_IBG" AS INTEGER) = e.id_municipio
        """
        
        try:
            gdf = gpd.read_postgis(query, self.db.engine, geom_col='geometry')
            
            # Mapeamento: Nome para exibição -> Coluna no DataFrame
            variables = {
                'RAIS - Remuneração Média': 'remuneracao_media',
                'Censo - Alfabetização': 'taxa_alfabetizacao',
                'Censo - Envelhecimento': 'indice_envelhecimento',
                'Conectividade - Cobertura 4G/5G': 'cobertura_pop_4g5g'
            }
            
            # Matriz de pesos para Bivariate Moran
            gdf_clean = gdf.dropna(subset=['pct_votos'] + list(variables.values()))
            gdf_clean = gdf_clean[~gdf_clean.is_empty]
            
            if len(gdf_clean) > 5:
                w = Queen.from_dataframe(gdf_clean)
                w.transform = 'r'
                
                print(f"Análise para o candidato: {cand_name}")
                print(f"{'Variável':<35} | {'Corr (Pearson)':<15} | {'Moran Bivariado (I)':<20}")
                print("-" * 75)
                
                for label, col in variables.items():
                    subset = gdf_clean
                    
                    # Correlação de Pearson
                    corr = subset['pct_votos'].corr(subset[col])
                    
                    # Moran Bivariado
                    moran_bv = Moran_BV(subset['pct_votos'].values, subset[col].values, w)
                    
                    print(f"{label:<35} | {corr:.4f}          | {moran_bv.I:.4f} (p={moran_bv.p_sim:.3f})")
            else:
                print("Dados insuficientes após limpeza para correlação.")
                
        except Exception as e:
            print(f"Erro na análise de correlação: {e}")

    def analyze_party_autocorrelation(self):
        """
        E. Autocorrelação espacial por partido
        """
        print("\n=== E. ANÁLISE POR PARTIDO ===")
        
        # Identificar Top 3 Partidos (usando os 2 primeiros dígitos)
        top_parties_query = f"""
        SELECT LEFT(CAST(nr_votavel AS VARCHAR), 2) as partido, SUM(qt_votos)
        FROM {self.db.schema}.resultados_secao
        WHERE LENGTH(CAST(nr_votavel AS VARCHAR)) >= 4 
        GROUP BY 1 ORDER BY 2 DESC LIMIT 3
        """
        
        try:
            parties = pd.read_sql(top_parties_query, self.db.engine)['partido'].tolist()
            print(f"Top Partidos identificados: {parties}")
            
            for partido in parties:
                query = f"""
                WITH total_mun AS (
                    SELECT cd_municipio, SUM(qt_votos) as total_votos
                    FROM {self.db.schema}.resultados_secao GROUP BY 1
                ),
                partido_votos AS (
                    SELECT cd_municipio, SUM(qt_votos) as votos_partido
                    FROM {self.db.schema}.resultados_secao
                    WHERE LEFT(CAST(nr_votavel AS VARCHAR), 2) = '{partido}'
                    GROUP BY 1
                )
                SELECT 
                    g.geometry,
                    COALESCE(p.votos_partido, 0) * 100.0 / NULLIF(t.total_votos, 0) as pct_votos
                FROM {self.db.schema}.geo_mun g
                JOIN total_mun t ON CAST(g."CD_MUN_TSE" AS INTEGER) = t.cd_municipio
                LEFT JOIN partido_votos p ON CAST(g."CD_MUN_TSE" AS INTEGER) = p.cd_municipio
                """
                gdf = gpd.read_postgis(query, self.db.engine, geom_col='geometry')
                self.calculate_moran_i(gdf, 'pct_votos', title=f"Partido {partido}")
        except Exception as e:
            print(f"Erro na análise de partidos: {e}")

    def run_all(self):
        try:
            self.analyze_autocorrelation_candidates()
            self.analyze_aggregated_levels()
            self.analyze_socioeconomic_correlation()
            self.analyze_party_autocorrelation()
        finally:
            self.db.close()

if __name__ == "__main__":
    analysis = SpatialMetricsAnalysis()
    analysis.run_all()