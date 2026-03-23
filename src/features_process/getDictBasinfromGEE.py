#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2
@author: geodatin
"""
import os
import ee
import sys
import tqdm
import json
import collections
collections.Callable = collections.abc.Callable

from pathlib import Path

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
pathparent = str(Path(os.getcwd()).parents[1])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
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

getDictGera= {}


asset_bacias = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions'
bacias = ee.FeatureCollection(asset_bacias)
ls_name = bacias.reduceColumns(ee.Reducer.toList(), ['nunivotto4']).get('list').getInfo()
print(ls_name)

for name in tqdm.tqdm(ls_name):   
    bacia_a = bacias.filter(ee.Filter.eq('nunivotto4', name)).geometry();
    bacias_viz = bacias.filterBounds(bacia_a);
    ls_name_t = bacias_viz.reduceColumns(ee.Reducer.toList(), ['nunivotto4']).get('list').getInfo()
    # print( f'"{name}" : ',ls_name_t);
    getDictGera[name] = ls_name_t

print("dictBaciasViz = {")
for kk, lvv in getDictGera.items():
    print(f'    "{kk}" : {lvv},')

print("}")

with open('dict_basin_neigbor.json', 'w') as arquivo_json:
    json.dump(getDictGera, arquivo_json, indent=4)

print("dictionary saved as dict_basin_neigbor.json")