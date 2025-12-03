import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import warnings
from mgwr.gwr import GWR, MGWR
from mgwr.sel_bw import Sel_BW
from libpysal.weights import DistanceBand
import numpy as np
import importlib

# Ignorar warnings futuros
warnings.simplefilter(action='ignore', category=FutureWarning)

# --- Configurações do Banco de Dados ---
DB_CONFIG = {
    "user": "usuario",
    "password": "senha",
    "host": "localhost",
    "port": "5432",
    "dbname": "geodata",
    "schema": "public"
}

# --- Conexão com o Banco de Dados ---
try:
    conn_str = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    engine = create_engine(conn_str)
    print("Conexão com o banco de dados estabelecida com sucesso.")
except Exception as e:
    print(f"Erro ao conectar ao banco de dados: {e}")
    exit()

def fetch_data():
    """
    Busca dados socioeconômicos e de votação para o candidato configurado.
    """
    print("Buscando dados para a análise GWR...")
    config = importlib.import_module('db_builder.config')
    candidate_slug = getattr(config, 'CANDIDATE_SLUG', 'candidate')
    candidate_name = getattr(config, 'CANDIDATE_NAME', 'CANDIDATE')

    vot_table = f"votacao_dep_{candidate_slug}"

    # Use the candidate-specific processed table and a generic column name 'percentual_candidato'
    query = f"""
    SELECT
        g."CD_MUN" AS cd_municipio_ibge,
        g.geometry,
        c.taxa_alfabetizacao,
        c.idade_mediana,
        r.remuneracao_media,
        e.cobertura_pop_4g5g,
            v.percentual_candidato
    FROM {DB_CONFIG['schema']}.municipios_pr_2022 g
    LEFT JOIN {DB_CONFIG['schema']}.censo_mun c ON CAST(g."CD_MUN" AS INTEGER) = c.id_municipio
    LEFT JOIN {DB_CONFIG['schema']}.rais_agg r ON CAST(g."CD_MUN" AS INTEGER) = r.id_municipio
    LEFT JOIN {DB_CONFIG['schema']}.extra e ON CAST(g."CD_MUN" AS INTEGER) = e.id_municipio
        LEFT JOIN {DB_CONFIG['schema']}.{vot_table} v ON CAST(g."CD_MUN" AS INTEGER) = v.id_municipio;
    """
    
    try:
        gdf = gpd.read_postgis(query, engine, geom_col='geometry')
        print(f"Dados carregados. Total de {len(gdf)} municípios.")
        gdf.rename(columns={
            'taxa_alfabetizacao': 'Taxa_Alfabetizacao',
            'idade_mediana': 'Idade_Mediana',
            'remuneracao_media': 'Renda_Media_SM',
            'cobertura_pop_4g5g': 'Cobertura_4G_5G',
                'percentual_candidato': 'Votos_Khury_Perc'
        }, inplace=True)
        gdf['CANDIDATE_NAME'] = candidate_name
        return gdf
    except Exception as e:
        print(f"Erro ao buscar dados: {e}")
        return gpd.GeoDataFrame()

def preprocess_for_gwr(gdf):
    """
    Prepara os dados para a análise GWR.
    """
    print("Pré-processando dados para GWR...")
    
    # Variáveis independentes (X) e dependente (Y)
    y_name = 'Votos_Khury_Perc'
    x_names = ['Taxa_Alfabetizacao', 'Idade_Mediana', 'Renda_Media_SM', 'Cobertura_4G_5G']
    
    # Remove linhas com valores nulos em qualquer uma das variáveis de interesse
    gdf.dropna(subset=[y_name] + x_names, inplace=True)
    print(f"  - Removidos NAs. Total de municípios na análise: {len(gdf)}")
    
    # Extrai as variáveis
    y = gdf[y_name].values.reshape(-1, 1)
    X = gdf[x_names].values
    
    # Normaliza as variáveis independentes
    X = (X - X.mean(axis=0)) / X.std(axis=0)
    
    # Coordenadas dos centroides dos municípios
    coords = list(zip(gdf.geometry.centroid.x, gdf.geometry.centroid.y))
    
    return y, X, coords, x_names, gdf

def perform_gwr(y, X, coords):
    """
    Realiza a análise GWR.
    """
    print("Iniciando análise GWR...")
    
    # Encontra a largura de banda (bandwidth) ótima
    print("  - Encontrando a largura de banda ótima...")
    gwr_selector = Sel_BW(coords, y, X)
    gwr_bw = gwr_selector.search()
    print(f"  - Largura de banda ótima encontrada: {gwr_bw}")
    
    # Executa o modelo GWR
    print("  - Executando o modelo GWR...")
    gwr_model = GWR(coords, y, X, gwr_bw)
    gwr_results = gwr_model.fit()
    
    print("  - Análise GWR concluída.")
    return gwr_results

def analyze_and_visualize(gdf, gwr_results, x_names):
    """
    Analisa os resultados e gera visualizações.
    """
    print("Gerando mapas de resultados GWR...")
    
    # Adiciona os resultados ao GeoDataFrame
    gdf['local_r2'] = gwr_results.localR2
    
    # Mapa de R² Local
    fig, ax = plt.subplots(1, 1, figsize=(12, 12))
    gdf.plot(column='local_r2', ax=ax, legend=True,
             legend_kwds={'label': "R² Local", 'orientation': "horizontal"},
             cmap='viridis')
    ax.set_title('GWR: Poder de Explicação do Modelo (R² Local)', fontsize=16)
    ax.set_axis_off()
    plt.tight_layout()
    plt.savefig('gwr_mapa_r2_local.png', dpi=300)
    print("  - Mapa de R² Local salvo em 'gwr_mapa_r2_local.png'.")
    
    # Mapas de Coeficientes: adiciona os coeficientes locais ao GeoDataFrame
    coef_arrays = []
    for i, var_name in enumerate(x_names):
        arr = gwr_results.params[:, i+1]
        gdf[f'coef_{var_name}'] = arr
        coef_arrays.append(arr)

    # Normaliza a escala de cores entre todos os coeficientes (simétrica em torno de zero)
    all_coefs = np.concatenate(coef_arrays)
    # Usa percentis para aumentar o contraste visual
    p5 = np.nanpercentile(all_coefs, 5)
    p95 = np.nanpercentile(all_coefs, 95)
    max_abs = max(abs(p5), abs(p95))
    vmin, vmax = -max_abs, max_abs

    # Define o número de colunas para o subplot
    n_vars = len(x_names)
    n_cols = 2
    n_rows = int(np.ceil(n_vars / n_cols))

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 7 * n_rows))
    axes = axes.flatten()

    for i, var_name in enumerate(x_names):
        ax = axes[i]
        gdf.plot(column=f'coef_{var_name}', ax=ax, legend=True, cmap='coolwarm', vmin=vmin, vmax=vmax,
                 legend_kwds={'label': f"Coeficiente de {var_name}", 'orientation': "horizontal"})
        ax.set_title(f'Influência da "{var_name.replace("_", " ")}" no Voto')
        ax.set_axis_off()

    for i in range(n_vars, len(axes)):
        axes[i].set_axis_off()

    plt.tight_layout()
    plt.savefig('gwr_mapas_coeficientes.png', dpi=300)
    print("  - Mapas de coeficientes salvos em 'gwr_mapas_coeficientes.png'.")


def main():
    """
    Orquestra a execução da análise GWR.
    """
    print("--- Iniciando Análise Extra: Regressão Geograficamente Ponderada (GWR) ---")
    
    gdf = fetch_data()
    
    if gdf.empty:
        print("Nenhum dado retornado do banco. A análise não pode continuar.")
        return
        
    y, X, coords, x_names, gdf_clean = preprocess_for_gwr(gdf)
    
    gwr_results = perform_gwr(y, X, coords)
    
    analyze_and_visualize(gdf_clean, gwr_results, x_names)
    
    print("\n--- Análise GWR concluída com sucesso! ---")
    print("Arquivos gerados: 'gwr_mapa_r2_local.png', 'gwr_mapas_coeficientes.png'")

if __name__ == "__main__":
    main()
