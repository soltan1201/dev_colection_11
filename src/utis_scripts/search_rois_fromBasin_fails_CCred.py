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
    ee.Initialize(project=projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise

nameBacias = [
    '765', '7544', '7541', '7411', '746', '7591', '7592',
    '761111', '761112', '7612', '7613', '7614', '7615',
    '771', '7712', '772', '7721', '773', '7741', '7746',
    '7754', '7761', '7764', '7691', '7581', '7625', '7584',
    '751', '752', '7616', '745', '7424', '7618', '7561',
    '755', '7617', '7564', '7422', '76116', '7671', '757',
    '766', '753', '764', '7619', '7443', '7438', '763',
    '7622'
]

anos_esperados = list(range(1985, 2026))  # 1985 até 2025
pasta_alvo = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesCCred'

print(f"Lendo assets da pasta: {pasta_alvo}")
print("(isso pode levar alguns segundos devido à paginação)...")


def listar_todos_assets(folder_id):
    """Lista todos os assets da pasta com suporte a paginação."""
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


# 1. Puxa a lista completa de assets salvos
lista_caminhos = listar_todos_assets(pasta_alvo)
print(f"Total de assets encontrados na pasta: {len(lista_caminhos)}")

# 2. Extrai as combinações (bacia, ano) que já existem
# Padrão esperado: rois_fromBasin_{nbacia}_{ano}
combinacoes_existentes = set()

for caminho in lista_caminhos:
    nome_arquivo = caminho.split('/')[-1]
    partes = nome_arquivo.split('_')

    # rois_fromBasin_7754_1985 → partes = ['rois', 'fromBasin', '7754', '1985']
    if len(partes) >= 4 and partes[0] == 'rois' and partes[1] == 'fromBasin':
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

    if len(anos_faltantes_bacia) > 0:
        dict_faltantes[bacia] = anos_faltantes_bacia

# 4. Exibe o resumo
print("\n" + "=" * 55)
print("RESUMO DE ASSETS FALTANTES — rois_fromBasin (CCred)")
print("=" * 55)

if total_faltantes == 0:
    print("Todas as bacias e anos estão completos! Nenhum asset faltando.")
else:
    print(f"Faltam {total_faltantes} FeatureCollections no total.\n")
    for bacia, anos in dict_faltantes.items():
        print(f"  Bacia {bacia} ({len(anos)} anos faltando): {anos}")

# 5. Salva o resultado em JSON
arquivo_json = 'dict_bacias_anos_faltantes_CCred_fromBasin.json'
with open(arquivo_json, 'w', encoding='utf-8') as f:
    json.dump(dict_faltantes, f, indent=4)

print(f"\nArquivo salvo: {arquivo_json}")
print('=' * 55)
print("            finish process              ")
