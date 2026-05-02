#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Script para juntar todos os anos (1985-2025) de classificação por bacia em
uma única imagem multi-banda.

Lê de:   projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1
Exporta: projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1joined

Cada imagem de saída tem uma banda por ano: classification_1985, ..., classification_2025
Propriedades mantidas: id_bacia, version, biome, classifier, collection, sensor, source, bands
'''

import ee
import os
import sys
from pathlib import Path
import collections
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
from gee_tools import switch_user, tasks

projAccount = get_current_account()
print(f"projeto selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project=projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise

# ============================================================
# PARÂMETROS
# ============================================================

ASSET_IN  = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1'
ASSET_OUT = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1joined'

YEARS = list(range(1985, 2026))  # 1985 a 2025 inclusive

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

VERSION = 3

# Fatia da lista de bacias via argumentos CLI: python script.py pos_inicio pos_fim
pos_inicio = int(sys.argv[1]) if len(sys.argv) > 1 else 0
pos_fim    = int(sys.argv[2]) if len(sys.argv) > 2 else len(nameBacias)
bacias_slice = nameBacias[pos_inicio:pos_fim]
print(f"Processando bacias [{pos_inicio}:{pos_fim}]: {bacias_slice}")

# ============================================================
# VERIFICAR ASSETS JÁ EXPORTADOS
# ============================================================

try:
    lst_assets_saved = [
        item['id'].split('/')[-1]
        for item in ee.data.listAssets({'parent': ASSET_OUT})['assets']
    ]
    print(f"{len(lst_assets_saved)} assets já existem no destino.")
except Exception as ex:
    print(f"Aviso ao listar assets de destino: {ex}")
    lst_assets_saved = []

# ============================================================
# CARREGAR A COLEÇÃO DE ENTRADA
# ============================================================

imgCol = (ee.ImageCollection(ASSET_IN)
            .filter(ee.Filter.eq('version', VERSION))
        )
# ============================================================
# PROCESSAMENTO POR BACIA
# ============================================================

for nbacia in bacias_slice:

    nome_saida = f'BACIA_{nbacia}_joined_GTB_col11_fm-v_{VERSION}'

    if nome_saida in lst_assets_saved:
        print(f"  BACIA {nbacia} já exportada — pulando.")
        continue

    print(f"\nProcessando bacia {nbacia} ...")

    # Filtra imagens desta bacia
    col_bacia = imgCol.filter(ee.Filter.eq('id_bacia', nbacia))

    n = col_bacia.size().getInfo()
    print(f"  {n} imagens encontradas para a bacia {nbacia}")

    if n == 0:
        print(f"  AVISO: nenhuma imagem encontrada para bacia {nbacia} — pulando.")
        continue

    # Pega propriedades da primeira imagem (sem 'year')
    primeira_img = ee.Image(col_bacia.first())
    props = {
        'id_bacia':   primeira_img.get('id_bacia'),
        'version':    primeira_img.get('version'),
        'biome':      primeira_img.get('biome'),
        'classifier': primeira_img.get('classifier'),
        'collection': primeira_img.get('collection'),
        'sensor':     primeira_img.get('sensor'),
        'source':     primeira_img.get('source')
    }

    # Empilha todos os anos como bandas
    def get_year_band(year):
        year = ee.Number(year).toInt()
        year_str = ee.Number(year).format('%d')
        band_name = ee.String('classification_').cat(year_str)
        img = ee.Image(
            col_bacia.filter(ee.Filter.eq('year', year)).first()
        )
        # Renomeia a banda de classificação para classification_{year}
        img_renamed = img.select([0], [band_name])
        return img_renamed

    years_ee = ee.List(YEARS)
    bands_list = years_ee.map(get_year_band)

    # ImageCollection de bandas → imagem multi-banda
    multi_band_img = ee.ImageCollection.fromImages(bands_list).toBands()

    # Corrige nomes das bandas: toBands() adiciona prefixo de índice
    # Renomeia para classification_1985 ... classification_2025
    correct_names = [f'classification_{y}' for y in YEARS]
    multi_band_img = multi_band_img.rename(correct_names)

    # Aplica propriedades
    multi_band_img = multi_band_img.set(props)

    # Região: footprint da primeira imagem
    region = primeira_img.geometry()

    # Exporta
    desc = nome_saida
    asset_id = f'{ASSET_OUT}/{nome_saida}'

    opt_exp = {
        'image': multi_band_img,
        'description': desc,
        'assetId': asset_id,
        'region': region,
        'scale': 30,
        'maxPixels': 1e13,
        'pyramidingPolicy': {'.default': 'mode'},
    }

    task = ee.batch.Export.image.toAsset(**opt_exp)
    task.start()
    print(f"  Exportando: {desc}")
    for k, v in dict(task.status()).items():
        print(f"    {k}: {v}")

print("\nTodas as bacias processadas.")
