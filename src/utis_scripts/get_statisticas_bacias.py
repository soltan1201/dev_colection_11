#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
# SCRIPT DE RE SAMPLING POR BACIA
# Produzido por Geodatin - Dados e Geoinformacao
# DISTRIBUIDO COM GPLv2
'''

import ee 
import sys
import os
import json
from tqdm import tqdm
from pathlib import Path
import collections
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
print("ver >> ", pathparent)
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
projAccount = get_current_account()
from gee_tools import *
print(f"projetos selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project= projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise


# =========================================================================
# PARÂMETROS
# =========================================================================
assetMapbiomas100 = 'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_integration_v2'
asset_bacias = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions'

classMapB = [3, 4, 5, 9, 12, 13, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32, 33, 36, 39, 40, 41, 46, 47, 48, 49, 50, 62, 75]
classNew  = [3, 4, 3, 3, 12, 12, 15, 19, 19, 19, 21, 25, 25, 25, 25, 33, 29, 25, 33, 12, 33, 36, 19, 19, 19, 36, 36, 36,  4, 12, 19, 25]

ano_inicial = 1985
ano_final = 2024

# =========================================================================
# CARREGANDO DADOS NO GEE
# =========================================================================
bacias = ee.FeatureCollection(asset_bacias)
mapbiomas = ee.Image(assetMapbiomas100)

# Imagem de área do pixel convertida para hectares (10.000 m2)
area_img = ee.Image.pixelArea().divide(10000)

# Dicionários de saída
dict_sctat_area = {}
dict_sctat_pct = {}

print("Obtendo lista de bacias...")
try:
    lista_bacias = bacias.aggregate_array('nunivotto4').getInfo()
    for b in lista_bacias:
        dict_sctat_area[str(b)] = {}
        dict_sctat_pct[str(b)] = {}
except Exception as e:
    print(f"Erro ao acessar as bacias: {e}")
    sys.exit()

# =========================================================================
# PROCESSAMENTO ANO A ANO
# =========================================================================
print(f"\nIniciando cálculo de estatísticas ({ano_inicial} a {ano_final})...")

for year in range(ano_inicial, ano_final + 1):
    print(f"Processando ano {year}...")
    band_name = f'classification_{year}'
    
    # 1. Seleciona a banda do ano e faz o remapeamento das classes
    img_ano = mapbiomas.select([band_name]).remap(classMapB, classNew).rename('class')
    
    # 2. Empilha a imagem de área (banda 0) com a imagem de classes (banda 1)
    img_calc = area_img.addBands(img_ano)
    
    # 3. Calcula a soma da área agrupada por classe, recortando pelas bacias
    # Usamos tileScale=16 para dividir o cálculo em blocos menores e evitar erro de memória
    stats = img_calc.reduceRegions(
        collection=bacias,
        reducer=ee.Reducer.sum().group(
            groupField=1, 
            groupName='class'
        ),
        scale=30,
        tileScale=16 
    )
    
    # 4. Puxa os resultados deste ano para o Python
    try:
        resultados_ano = stats.getInfo()
    except Exception as e:
        print(f"Erro ao calcular o ano {year}: {e}")
        continue
    
    # 5. Organiza os dados no dicionário local
    for feat in resultados_ano['features']:
        id_bacia = str(feat['properties']['nunivotto4'])
        grupos = feat['properties'].get('groups', [])
        
        dict_classes_area = {}
        dict_classes_pct = {}
        
        # Soma a área total para calcular a porcentagem depois
        area_total_bacia = sum([g['sum'] for g in grupos])
        
        for g in grupos:
            classe = int(g['class'])
            area_ha = round(g['sum'], 2)
            
            # Calcula a porcentagem
            pct = round((area_ha / area_total_bacia) * 100, 2) if area_total_bacia > 0 else 0
            
            dict_classes_area[classe] = area_ha
            dict_classes_pct[classe] = pct
            
        dict_sctat_area[id_bacia][str(year)] = dict_classes_area
        dict_sctat_pct[id_bacia][str(year)] = dict_classes_pct

# =========================================================================
# SALVANDO OS DADOS
# =========================================================================
arquivo_area = 'estatisticas_bacias_area_ha.json'
arquivo_pct = 'estatisticas_bacias_porcentagem.json'

with open(arquivo_area, 'w', encoding='utf-8') as f:
    json.dump(dict_sctat_area, f, indent=4)
    
with open(arquivo_pct, 'w', encoding='utf-8') as f:
    json.dump(dict_sctat_pct, f, indent=4)

print("\n" + "="*50)
print(f"✅ Finalizado! Dados exportados com sucesso:")
print(f" -> {arquivo_area} (Formato do seu exemplo)")
print(f" -> {arquivo_pct} (Para análises de proporção)")
print("="*50)