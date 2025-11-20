import numpy as np
import pandas as pd
import geopandas as gpd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

dados_geo_municipio = gpd.read_file("./dados/PR_Municipios_2024/PR_Municipios_2024.shp")

dados_cd_municipio = pd.read_csv("./dados/mapa-cod-municipio.csv")

dados_geo_municipio['CD_MUN'] = dados_geo_municipio['CD_MUN'].astype(int)
dados_cd_municipio['id_municipio_ibge'] = dados_cd_municipio['id_municipio_ibge'].astype(int)
dados_geo_municipio['NM_MUN'] = dados_geo_municipio['NM_MUN'].astype(str)
dados_geo_municipio['CD_RGI'] = dados_geo_municipio['CD_RGI'].astype(int)
dados_geo_municipio['NM_RGI'] = dados_geo_municipio['NM_RGI'].astype(str)
dados_geo_municipio['CD_RGINT'] = dados_geo_municipio['CD_RGINT'].astype(int)
dados_geo_municipio['NM_RGINT'] = dados_geo_municipio['NM_RGINT'].astype(str)
dados_geo_municipio['CD_UF'] = dados_geo_municipio['CD_UF'].astype(int)
dados_geo_municipio['NM_UF'] = dados_geo_municipio['NM_UF'].astype(str)
dados_geo_municipio['SIGLA_UF'] = dados_geo_municipio['SIGLA_UF'].astype(str)
dados_geo_municipio['CD_REGIA'] = dados_geo_municipio['CD_REGIA'].astype(int)
dados_geo_municipio['NM_REGIA'] = dados_geo_municipio['NM_REGIA'].astype(str)
dados_geo_municipio['SIGLA_RG'] = dados_geo_municipio['SIGLA_RG'].astype(str)

df_merged = dados_geo_municipio.merge(
    dados_cd_municipio,
    left_on='CD_MUN',
    right_on='id_municipio_ibge',
    how='left'
)

df_merged = df_merged.rename(columns={'id_municipio_tse': 'CD_MUN_TSE'})
df_merged = df_merged.rename(columns={'id_municipio_ibge': 'CD_MUN_IBG'})
df_merged = df_merged.drop(columns=['CD_MUN'])

# removendo as colunas CD_CONCU e NM_CONCU e reordenando
df_merged = df_merged[['CD_MUN_TSE', 'CD_MUN_IBG', 'NM_MUN', 'CD_RGI', 'NM_RGI', 'CD_RGINT', 'NM_RGINT', 'CD_UF', 'NM_UF', 'SIGLA_UF', 'CD_REGIA', 'NM_REGIA', 'SIGLA_RG', 'AREA_KM2', 'geometry']]

df_merged.to_file(r"C:\Users\ruben\Desktop\Codigos-VScode\UFMG\BancoDadosGeo\TP-BDG\dados\PR_Municipios_2024_new\PR_Municipios_2024.shp", driver='ESRI Shapefile')
