#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2
@author: geodatin
"""

import ee
import os
import json
import sys
import collections
from pathlib import Path
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
from gee_tools import *
projAccount = get_current_account()
print(f"projetos selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project= projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise


# Inicializa o Earth Engine (ajuste o projeto se necessário)
# ee.Initialize(project='seu-projeto')

nameBacias = [
    '7411','7754', '7691', '7581', '7625', '7584', '751', '7614', 
    '752', '7616', '745', '7424', '773', '7612', '7613', 
    '7618', '7561', '755', '7617', '7564', '761111','761112', 
    '7741', '7422', '76116', '7761', '7671', '7615',  
    '7764', '757', '771', '7712', '766', '7746', '753', '764', 
    '7541', '7721', '772', '7619', '7443', '765', '7544', '7438', 
    '763', '7591', '7592', '7622', '746'
]

print(f"Iniciando a leitura de {len(nameBacias)} bacias...\n")

asset_base = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_byBasinInd/rois_fromGrade_'

# Dicionário que vai guardar a saída no formato exato que você pediu
dict_saida = {}

for cc, nbasin in enumerate(nameBacias):
    print(f"[{cc+1}/{len(nameBacias)}] Processando bacia {nbasin}...")
    asset_id = f"{asset_base}{nbasin}"
    
    try:
        # Carrega a FeatureCollection da bacia
        fc = ee.FeatureCollection(asset_id)
        dict_basin = {}
        for nyear in range(1985, 2025):
            print(f" ===== processing year  {nyear} =====  ")
            fc_tmp = fc.filter(ee.Filter.eq('year', nyear))
            # Pede ao servidor do GEE para contar os valores únicos da coluna 'class'
            # Isso retorna um dict: {"3": 1527, "4": 14503, "15": 260, ...}
            histograma_classes = fc_tmp.aggregate_histogram('class').getInfo()
            print(histograma_classes)
            dict_basin[str(nyear)] = histograma_classes
            
        # Adiciona diretamente no dicionário principal usando a bacia como chave
        dict_saida[nbasin] = dict_basin
            
    except Exception as e:
        print(f" ⚠️ Erro ao processar a bacia {nbasin}. Erro: {e}")

# =========================================================
# SALVA O ARQUIVO JSON
# =========================================================
arquivo_saida = 'amostras_por_bacia_col11.json'

with open(arquivo_saida, 'w', encoding='utf-8') as f:
    json.dump(dict_saida, f, indent=4)

print(f"\n✅ Finalizado! Dados exportados com sucesso para o arquivo '{arquivo_saida}'")