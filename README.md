Fonte dos dados:

nota_fiscal_candidato_2022_PR -> https://dadosabertos.tse.jus.br/dataset/candidatos-2022

perfil_eleitor_secao_2022_PR -> https://dadosabertos.tse.jus.br/dataset/eleitorado-2022

rede_social_candidato_2022_PR -> https://dadosabertos.tse.jus.br/dataset/candidatos-2022

votacao_secao_2022_PR -> https://dadosabertos.tse.jus.br/dataset/resultados-2022

bq-results-20251015-170110-1760547700235 -> RAIS 2022 - Obtido via query no Base dos Dados dos dados da RAIS
-> https://basedosdados.org/dataset/3e7c4d58-96ba-448e-b053-d385a829ef00?table=86b69f96-0bfe-45da-833b-6edc9a0af213

query:
SELECT ano, sigla_uf, id_municipio, valor_remuneracao_dezembro_sm, valor_remuneracao_media_sm,cnae_1, idade, faixa_etaria, grau_instrucao_apos_2005, nacionalidade, sexo, raca_cor, indicador_portador_deficiencia
FROM basedosdados.br_me_rais.microdados_vinculos 
where ano = 2022 and sigla_uf = 'PR'

query ibge_setor_censitario_2022 - setor_censitario_ibge_2022 (censo 2022):
SELECT id_uf, id_municipio, id_setor_censitario, area, geometria, pessoas, domicilios, domicilios_particulares, domicilios_coletivos, domicilios_particulares_ocupados, media_moradores_domicilios, porcentagem_domicilios_imputados
FROM `basedosdados.br_ibge_censo_2022.setor_censitario` 
WHERE id_uf = '41'


shapefiles municipios, região imediata, região intermediária e uf - Malhas IBGE
https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/15774-malhas.html