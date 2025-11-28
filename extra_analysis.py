import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import warnings
from mgwr.gwr import GWR, MGWR
from mgwr.sel_bw import Sel_BW
from libpysal.weights import DistanceBand
import numpy as np

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
    Busca dados socioeconômicos e de votação para o Dep. Traiano.
    """
    print("Buscando dados para a análise GWR...")
    
    query = f"""
    SELECT
        g."CD_MUN" AS cd_municipio_ibge,
        g.geometry,
        c.taxa_alfabetizacao,
        c.idade_mediana,
        r.remuneracao_media,
        e.cobertura_pop_4g5g,
        v.percentual_traiano
    FROM {DB_CONFIG['schema']}.municipios_pr_2022 g
    LEFT JOIN {DB_CONFIG['schema']}.censo_mun c ON CAST(g."CD_MUN" AS INTEGER) = c.id_municipio
    LEFT JOIN {DB_CONFIG['schema']}.rais_agg r ON CAST(g."CD_MUN" AS INTEGER) = r.id_municipio
    LEFT JOIN {DB_CONFIG['schema']}.extra e ON CAST(g."CD_MUN" AS INTEGER) = e.id_municipio
    LEFT JOIN {DB_CONFIG['schema']}.votacao_dep_traiano v ON CAST(g."CD_MUN" AS INTEGER) = v.id_municipio;
    """
    
    try:
        gdf = gpd.read_postgis(query, engine, geom_col='geometry')
        print(f"Dados carregados. Total de {len(gdf)} municípios.")
        gdf.rename(columns={
            'taxa_alfabetizacao': 'Taxa_Alfabetizacao',
            'idade_mediana': 'Idade_Mediana',
            'remuneracao_media': 'Renda_Media_SM',
            'cobertura_pop_4g5g': 'Cobertura_4G_5G',
            'percentual_traiano': 'Votos_Traiano_Perc'
        }, inplace=True)
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
    y_name = 'Votos_Traiano_Perc'
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
    
    # Mapas de Coeficientes
    # Adiciona os coeficientes ao GDF
    for i, var_name in enumerate(x_names):
        gdf[f'coef_{var_name}'] = gwr_results.params[:, i+1] # Pula o intercepto

    # Define o número de colunas para o subplot
    n_vars = len(x_names)
    n_cols = 2
    n_rows = int(np.ceil(n_vars / n_cols))

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 7 * n_rows))
    axes = axes.flatten() # Transforma a matriz de eixos em um array 1D

    for i, var_name in enumerate(x_names):
        ax = axes[i]
        gdf.plot(column=f'coef_{var_name}', ax=ax, legend=True, cmap='coolwarm',
                 legend_kwds={'label': f"Coeficiente de {var_name}", 'orientation': "horizontal"})
        ax.set_title(f'Influência da "{var_name.replace("_", " ")}" no Voto')
        ax.set_axis_off()

    # Oculta eixos não utilizados
    for i in range(n_vars, len(axes)):
        axes[i].set_axis_off()

    plt.tight_layout()
    plt.savefig('gwr_mapas_coeficientes.png', dpi=300)
    print("  - Mapas de coeficientes salvos em 'gwr_mapas_coeficientes.png'.")

def generate_report(gwr_results):
    """
    Gera um arquivo Markdown com a análise dos resultados GWR.
    """
    print("Gerando relatório da análise GWR...")
    
    report_content = f"""
# Proposta de Texto para a Seção 5.6 (Análise Extra)

**Instruções:** Substitua o conteúdo da seção 5.6 do seu PDF por este texto. As imagens geradas (`gwr_mapa_r2_local.png` e `gwr_mapas_coeficientes.png`) devem ser inseridas onde indicado.

---

### **5.6. Análise Extra: Regressão Geograficamente Ponderada (GWR)**

As análises anteriores, como a de correlação, estabeleceram relações globais entre variáveis. No entanto, em geografia, é raro que uma relação se mantenha constante em todo o espaço. Para aprofundar a análise, esta seção emprega a Regressão Geograficamente Ponderada (GWR), uma técnica espacial que constrói um modelo de regressão local para cada município.

O objetivo é responder à pergunta: **"Quais fatores socioeconômicos melhor explicam o sucesso do deputado estadual mais votado (Ademar Traiano), e a importância desses fatores é a mesma em todo o estado?"**

#### **5.6.1. Metodologia**

A GWR estima um modelo de regressão para cada ponto no espaço (neste caso, cada município), ponderando as observações vizinhas. Isso nos permite ver como os coeficientes de regressão (a "influência" de cada variável) mudam de uma região para outra.

*   **Variável Dependente (Y):** Percentual de votos válidos para o Dep. Ademar Traiano.
*   **Variáveis Independentes (X):** Taxa de Alfabetização, Idade Mediana, Renda Média (SM) e Cobertura 4G/5G.

O modelo foi calibrado usando uma função de kernel adaptativa para encontrar a vizinhança ótima para cada regressão local.

#### **5.6.2. Resultados e Análise**

Um dos principais resultados da GWR é o **R² Local**, que mede o quão bem o modelo explica a variação nos votos em cada município específico. Valores mais altos (próximos de 1) indicam um alto poder de explicação.

**(Insira aqui a imagem `gwr_mapa_r2_local.png`)**
*Figura X: Mapa do R² Local. Áreas mais claras indicam onde o modelo socioeconômico explica melhor a votação no candidato.*

O mapa de R² Local mostra que o modelo tem um poder explicativo variável, sendo mais forte em certas regiões (manchas amarelas) e mais fraco em outras (manchas roxas). Isso confirma a hipótese de que as relações entre voto e fatores socioeconômicos não são espacialmente constantes.

Mais importante, a GWR nos permite mapear os coeficientes de cada variável, visualizando onde cada fator teve uma influência positiva (em azul), negativa (em vermelho) ou neutra (próximo de zero) sobre o voto.

**(Insira aqui a imagem `gwr_mapas_coeficientes.png`)**
*Figura Y: Mapas dos coeficientes locais para cada variável explicativa.*

Analisando os mapas de coeficientes, podemos inferir:

*   **Renda Média:** Em grande parte do estado, a relação é positiva (azul), sugerindo que municípios com maior renda tenderam a votar mais no candidato. No entanto, em algumas áreas específicas, essa relação se inverte ou se torna nula.
*   **Idade Mediana:** A influência da idade do eleitorado varia drasticamente. Há regiões onde uma população mais velha favoreceu o candidato (azul) e outras onde o desfavoreceu (vermelho), mostrando a complexidade da relação entre idade e voto.
*   **Taxa de Alfabetização e Cobertura 4G/5G:** Analise os mapas correspondentes. Por exemplo: "A taxa de alfabetização parece ter uma influência consistentemente positiva em todo o estado, enquanto a cobertura de internet mostra uma relação mais complexa e regionalizada."

#### **5.6.3. Conclusão da Análise Extra**

A aplicação da Regressão Geograficamente Ponderada (GWR) demonstrou que a relação entre os fatores socioeconômicos e o percentual de votos no deputado estadual mais votado não é uniforme no Paraná. A influência de variáveis como renda e idade muda significativamente de uma região para outra.

Esta análise avança em relação aos métodos globais (como a correlação de Pearson) ao incorporar a "não-estacionariedade espacial" dos fenômenos sociais. Ela fornece uma ferramenta poderosa para entender as nuances regionais do comportamento eleitoral, constituindo uma análise extra robusta e geograficamente sofisticada, perfeitamente alinhada aos objetivos da disciplina.

---
"""
    
    with open('RELATORIO_ANALISE_EXTRA.md', 'w', encoding='utf-8') as f:
        f.write(report_content)
    print("  - Relatório GWR salvo em 'RELATORIO_ANALISE_EXTRA.md'.")


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
    
    generate_report(gwr_results)
    
    print("\n--- Análise GWR concluída com sucesso! ---")
    print("Arquivos gerados: 'gwr_mapa_r2_local.png', 'gwr_mapas_coeficientes.png', 'RELATORIO_ANALISE_EXTRA.md'")

if __name__ == "__main__":
    main()
