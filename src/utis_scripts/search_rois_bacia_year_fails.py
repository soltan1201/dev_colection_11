#-*- coding utf-8 -*-
import ee
import os
import sys
import json
import collections
from pathlib import Path
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
projAccount = get_current_account()
print(f"projetos selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project= projAccount) # project='ee-cartassol'
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise

# Inicialize o Earth Engine (descomente e configure seu projeto se necessário)
# ee.Initialize(project='seu-projeto-aqui')

nameBacias = [
    '7754', '7691', '7581', '7625', '7584', '751', '7614', 
    '752', '7616', '745', '7424', '773', '7612', '7613', 
    '7618', '7561', '755', '7617', '7564', '761111','761112', 
    '7741', '7422', '76116', '7761', '7671', '7615', '7411', 
    '7764', '757', '771', '7712', '766', '7746', '753', '764', 
    '7541', '7721', '772', '7619', '7443', '765', '7544', '7438', 
    '763', '7591', '7592', '7622', '746'
]

anos_esperados = list(range(1985, 2026)) # 1985 até 2025
pasta_alvo = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesv1CC'

print(f"Lendo assets da pasta (isso pode levar alguns segundos devido à paginação)...")

# 1. Função com paginação para listar todos os assets da pasta (superando o limite de 1000)
def listar_todos_assets(folder_id):
    assets_encontrados = []
    page_token = None
    
    while True:
        params = {'parent': folder_id, 'pageSize': 1000}
        if page_token:
            params['pageToken'] = page_token
            
        resposta = ee.data.listAssets(params)
        assets_encontrados.extend(resposta.get('assets', []))
        
        page_token = resposta.get('nextPageToken')
        if not page_token:
            break
            
    return [asset['name'] for asset in assets_encontrados]

# Puxa a lista completa de caminhos dos assets
lista_caminhos = listar_todos_assets(pasta_alvo)
print(f"Total de assets encontrados na pasta: {len(lista_caminhos)}")

# 2. Extrai as bacias e anos que já existem no servidor
combinacoes_existentes = set()

for caminho in lista_caminhos:
    # Exemplo de caminho: projects/.../rois_fromGrade_7422_2013
    nome_arquivo = caminho.split('/')[-1]
    partes = nome_arquivo.split('_')
    
    if len(partes) >= 2:
        bacia_str = partes[2]
        ano_str = partes[3]
        
        if ano_str.isdigit():
            combinacoes_existentes.add((bacia_str, int(ano_str)))

# 3. Compara o esperado com o existente para encontrar as falhas
dict_faltantes = {}
total_faltantes = 0

for bacia in nameBacias:
    anos_faltantes_bacia = []
    for ano in anos_esperados:
        if (bacia, ano) not in combinacoes_existentes:
            anos_faltantes_bacia.append(ano)
            total_faltantes += 1
            
    # Se faltar algum ano, registra no dicionário
    if len(anos_faltantes_bacia) > 0:
        dict_faltantes[bacia] = anos_faltantes_bacia

# 4. Exibe o resumo no terminal
print("\n" + "="*50)
print("RESUMO DE ASSETS FALTANTES")
print("="*50)

if total_faltantes == 0:
    print("✅ Todas as bacias e anos estão completos! Nenhum asset faltando.")
else:
    print(f"⚠️ Faltam {total_faltantes} FeatureCollections no total.\n")
    for bacia, anos in dict_faltantes.items():
        print(f"Bacia {bacia} ({len(anos)} anos faltando): {anos}")

# 5. Salva o resultado em um JSON para usar no próximo script de processamento
arquivo_json = 'dict_bacias_anos_faltantes_col11.json'
with open(arquivo_json, 'w', encoding='utf-8') as f:
    json.dump(dict_faltantes, f, indent=4)

print(f"\n📁 Arquivo salvo: {arquivo_json}")