import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import text
from db_manager import DatabaseManager
from config import DB_CONFIG

# Configuração de estilo visual
sns.set_theme(style="whitegrid")

def get_winning_candidates_map(db):
    """
    Gera mapa do candidato vencedor por município.
    Correção: Usa aspas duplas em "CD_MUN_TSE" para respeitar o Case Sensitivity do Postgres.
    """
    print("1. Gerando mapa de vencedores por município...")
    
    query = f"""
    WITH votos AS (
        SELECT cd_municipio, nm_votavel, SUM(qt_votos) as votos
        FROM {db.schema}.resultados_secao
        GROUP BY 1, 2
    ),
    rank AS (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY cd_municipio ORDER BY votos DESC) as rn
        FROM votos
    )
    SELECT 
        r.cd_municipio, 
        r.nm_votavel, 
        r.votos, 
        g.geometry
    FROM rank r
    JOIN {db.schema}.geo_mun g 
        ON r.cd_municipio = CAST(g."CD_MUN_TSE" AS INTEGER) 
    WHERE r.rn = 1
    """
    
    try:
        gdf = gpd.read_postgis(query, db.engine, geom_col='geometry')
        
        if gdf.empty:
            print("AVISO: DataFrame vazio no mapa de vencedores.")
            return

        fig, ax = plt.subplots(figsize=(14, 10))
        
        # Destacar apenas os Top 5 vencedores para a legenda não explodir
        top_winners = gdf['nm_votavel'].value_counts().nlargest(5).index
        gdf['legenda'] = gdf['nm_votavel'].apply(lambda x: x if x in top_winners else 'OUTROS')
        
        gdf.plot(column='legenda', ax=ax, legend=True, cmap='tab10', 
                 edgecolor='0.8', linewidth=0.5, legend_kwds={'loc': 'lower right'})
        
        ax.set_title("Candidato Vencedor por Município (Top 5)", fontsize=16)
        ax.set_axis_off()
        plt.tight_layout()
        plt.savefig("mapa_vencedores.png", dpi=300)
        print("-> Salvo: mapa_vencedores.png")
        
    except Exception as e:
        print(f"Erro no mapa de vencedores: {e}")

def analyze_correlations(db):
    """
    Analisa correlação entre votos e indicadores socioeconômicos.
    Correção: Usa conn.exec_driver_sql para evitar erro de dicionário imutável.
    """
    print("2. Analisando correlações (Renda, Idade, Conectividade)...")
    
    # Descobrir o candidato mais votado do estado
    top_cand_query = f"""
    SELECT nm_votavel FROM {db.schema}.resultados_secao 
    GROUP BY nm_votavel ORDER BY SUM(qt_votos) DESC LIMIT 1
    """
    
    try:
        with db.engine.connect() as conn:
            # CORREÇÃO CRÍTICA: exec_driver_sql não tenta parsear parâmetros
            top_candidate = conn.exec_driver_sql(top_cand_query).scalar()
        
        print(f"   Candidato foco da análise: {top_candidate}")
        
        # Query usando a tabela geo_mun como tradutor (IBGE <-> TSE)
        query = f"""
        WITH votos_agregados AS (
            SELECT 
                cd_municipio as cod_tse, 
                SUM(qt_votos) as total_validos,
                SUM(CASE WHEN nm_votavel = '{top_candidate}' THEN qt_votos ELSE 0 END) as votos_cand
            FROM {db.schema}.resultados_secao
            GROUP BY 1
        ),
        tradutor AS (
            SELECT DISTINCT "CD_MUN_IBG", "CD_MUN_TSE"
            FROM {db.schema}.geo_mun
        )
        SELECT 
            c.id_municipio,
            c.taxa_alfabetizacao, 
            c.idade_mediana,
            e.cobertura_pop_4g5g,
            r.renda_media,
            CASE WHEN v.total_validos > 0 
                 THEN (v.votos_cand * 100.0 / v.total_validos) 
                 ELSE 0 
            END as pct_votos
        FROM {db.schema}.censo_mun c
        JOIN tradutor t ON c.id_municipio = CAST(t."CD_MUN_IBG" AS INTEGER)
        LEFT JOIN votos_agregados v ON CAST(t."CD_MUN_TSE" AS INTEGER) = v.cod_tse
        LEFT JOIN {db.schema}.extra e ON c.id_municipio = e.id_municipio
        LEFT JOIN (
            SELECT id_municipio, AVG(valor_remuneracao_media_sm) as renda_media 
            FROM {db.schema}.rais 
            GROUP BY id_municipio
        ) r ON c.id_municipio = r.id_municipio
        """
        
        df = pd.read_sql(query, db.engine)
        
        if df.empty:
            print("AVISO: DataFrame de correlação vazio.")
            return

        # Plotagem
        vars_analise = ['renda_media', 'cobertura_pop_4g5g', 'taxa_alfabetizacao', 'idade_mediana']
        titles = ['Renda Média (Salários Mínimos)', 'Cobertura 4G/5G (%)', 'Taxa de Alfabetização', 'Idade Mediana']
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(f"Onde {top_candidate} performa melhor?", fontsize=16)
        
        for i, var in enumerate(vars_analise):
            ax = axes[i//2, i%2]
            subset = df.dropna(subset=[var, 'pct_votos'])
            
            if not subset.empty:
                # Scatter plot com linha de tendência
                sns.regplot(data=subset, x=var, y='pct_votos', ax=ax, 
                            scatter_kws={'alpha':0.4, 's':30}, line_kws={'color':'red'})
                
                ax.set_xlabel(titles[i])
                ax.set_ylabel("% de Votos na Cidade")
                
                # Calcula correlação de Pearson
                corr = subset[var].corr(subset['pct_votos'])
                ax.set_title(f"Correlação: {corr:.2f}", fontweight='bold')
            
        plt.tight_layout()
        plt.savefig("analise_correlacao.png")
        print("-> Salvo: analise_correlacao.png")

    except Exception as e:
        print(f"Erro na correlação: {e}")

def analyze_regional_performance(db):
    """
    NOVA FUNÇÃO: Analisa desempenho dos Top 5 candidatos por Região Intermediária (usando geo_mun para agrupar).
    Substitui a análise de densidade de pontos.
    """
    print("3. Analisando desempenho regional (Regiões Intermediárias)...")
    
    # 1. Pegar os Top 5 candidatos do estado geral
    top5_query = f"""
    SELECT nm_votavel 
    FROM {db.schema}.resultados_secao 
    GROUP BY 1 ORDER BY SUM(qt_votos) DESC LIMIT 5
    """
    
    try:
        with db.engine.connect() as conn:
            top5 = [row[0] for row in conn.exec_driver_sql(top5_query).fetchall()]
        
        candidates_str = "', '".join(top5)
        
        # 2. Query agregando por Região Intermediária (presente na geo_mun como NM_RGINT)
        query = f"""
        SELECT 
            g."NM_RGINT" as regiao,
            r.nm_votavel,
            SUM(r.qt_votos) as votos
        FROM {db.schema}.resultados_secao r
        JOIN {db.schema}.geo_mun g ON r.cd_municipio = CAST(g."CD_MUN_TSE" AS INTEGER)
        WHERE r.nm_votavel IN ('{candidates_str}')
        GROUP BY 1, 2
        ORDER BY 1, 3 DESC
        """
        
        df = pd.read_sql(query, db.engine)
        
        if df.empty:
            print("AVISO: DataFrame regional vazio.")
            return
            
        # Pivotar para ter Regiões nas linhas e Candidatos nas colunas
        df_pivot = df.pivot(index='regiao', columns='nm_votavel', values='votos').fillna(0)
        
        # Plotar gráfico de barras empilhadas ou lado a lado
        ax = df_pivot.plot(kind='bar', figsize=(14, 8), width=0.8)
        
        ax.set_title("Total de Votos por Região Intermediária (Top 5 Candidatos)", fontsize=16)
        ax.set_ylabel("Quantidade de Votos")
        ax.set_xlabel("Região Intermediária")
        plt.xticks(rotation=45, ha='right')
        plt.legend(title='Candidato')
        
        plt.tight_layout()
        plt.savefig("analise_regional.png")
        print("-> Salvo: analise_regional.png")
        
    except Exception as e:
        print(f"Erro na análise regional: {e}")

def plot_top5_performance(db):
    """
    Gera mapas individuais (Total e %) para cada um dos 5 candidatos mais votados.
    Salva um arquivo PNG separado para cada mapa, com estilo visual unificado (cores e contorno).
    """
    print("4. Gerando mapas detalhados dos Top 5 Candidatos (Individualmente, com estilo unificado)...")

    # 1. Descobrir quem são os Top 5
    top5_query = f"""
    SELECT nm_votavel 
    FROM {db.schema}.resultados_secao 
    GROUP BY 1 ORDER BY SUM(qt_votos) DESC LIMIT 5
    """
    
    try:
        with db.engine.connect() as conn:
            top5_candidates = [row[0] for row in conn.exec_driver_sql(top5_query).fetchall()]
        
        print(f"   Top 5: {', '.join(top5_candidates)}")
        
        # 2. Query Principal (Mantendo a correção das aspas para Case Sensitivity)
        pivot_columns = []
        for cand in top5_candidates:
            safe_name = "".join(x for x in cand if x.isalnum())
            pivot_columns.append(f"SUM(CASE WHEN r.nm_votavel = '{cand}' THEN r.qt_votos ELSE 0 END) as \"votes_{safe_name}\"")
        
        pivot_sql = ",\n".join(pivot_columns)
        
        query = f"""
        SELECT 
            g."CD_MUN_TSE",
            g.geometry,
            g."NM_MUN",
            SUM(r.qt_votos) as total_valid_votes,
            {pivot_sql}
        FROM {db.schema}.resultados_secao r
        JOIN {db.schema}.geo_mun g ON r.cd_municipio = CAST(g."CD_MUN_TSE" AS INTEGER)
        GROUP BY g."CD_MUN_TSE", g.geometry, g."NM_MUN"
        """
        
        gdf = gpd.read_postgis(query, db.engine, geom_col='geometry')
        
        if gdf.empty:
            print("AVISO: DataFrame vazio para Top 5.")
            return

        # --- AJUSTES DE ESTILO ---
        common_cmap = 'Reds' # Colormap único para todos
        common_edgecolor = 'black' # Contorno preto
        common_linewidth = 0.5     # Largura do contorno (ajuste se precisar mais fino/grosso)

        # --- LOOP 1: MAPAS DE TOTAL DE VOTOS (Salvar 1 por 1) ---
        print("   -> Salvando mapas de TOTAL de votos...")
        for i, cand in enumerate(top5_candidates):
            safe_name = "".join(x for x in cand if x.isalnum())
            col_name = f"votes_{safe_name}"
            
            if col_name not in gdf.columns:
                continue

            fig, ax = plt.subplots(figsize=(10, 8))
            
            # Plot com colormap e contorno ajustados
            gdf.plot(column=col_name, ax=ax, 
                     cmap=common_cmap, 
                     legend=True, 
                     legend_kwds={'orientation': "horizontal", 'fraction': 0.046, 'pad': 0.04, 'label': 'Total de Votos'},
                     edgecolor=common_edgecolor, # Contorno preto
                     linewidth=common_linewidth) # Espessura do contorno
            
            ax.set_title(f"Total de Votos: {cand}", fontsize=14, fontweight='bold')
            ax.set_axis_off()
            
            filename = f"mapa_total_{safe_name}.png"
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            plt.close(fig) 
            print(f"      Salvo: {filename}")

        # --- LOOP 2: MAPAS DE PERCENTUAL DE VOTOS (Salvar 1 por 1) ---
        print("   -> Salvando mapas de PERCENTUAL (%) de votos...")
        
        # Calcular percentuais
        for cand in top5_candidates:
            safe_name = "".join(x for x in cand if x.isalnum())
            vote_col = f"votes_{safe_name}"
            pct_col = f"pct_{safe_name}"
            
            if vote_col in gdf.columns:
                gdf[pct_col] = (gdf[vote_col] / gdf['total_valid_votes']) * 100
                gdf[pct_col] = gdf[pct_col].fillna(0)

        for i, cand in enumerate(top5_candidates):
            safe_name = "".join(x for x in cand if x.isalnum())
            col_name = f"pct_{safe_name}"
            
            if col_name in gdf.columns:
                fig, ax = plt.subplots(figsize=(10, 8))
                
                # Plot com colormap e contorno ajustados
                gdf.plot(column=col_name, ax=ax, 
                         cmap=common_cmap, 
                         legend=True,
                         vmax=50, 
                         legend_kwds={'orientation': "horizontal", 'fraction': 0.046, 'pad': 0.04, 'label': '% de Votos'},
                         edgecolor=common_edgecolor, # Contorno preto
                         linewidth=common_linewidth) # Espessura do contorno
                
                ax.set_title(f"% de Votos: {cand}", fontsize=14, fontweight='bold')
                ax.set_axis_off()

                filename = f"mapa_percentual_{safe_name}.png"
                plt.savefig(filename, dpi=300, bbox_inches='tight')
                plt.close(fig)
                print(f"      Salvo: {filename}")

    except Exception as e:
        print(f"Erro nos mapas Top 5: {e}")

def main():
    db = DatabaseManager()
    try:
        get_winning_candidates_map(db)
        analyze_correlations(db)
        analyze_regional_performance(db)
        plot_top5_performance(db)
        print("\n--- Todas as análises concluídas! ---")
    finally:
        db.close()

if __name__ == "__main__":
    main()