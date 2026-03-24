#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
# SCRIPT DE CLASSIFICACAO POR BACIA
# Produzido por Geodatin - Dados e Geoinformacao
# DISTRIBUIDO COM GPLv2
'''

import ee 
# import gee
import sys
import os
import glob
from pathlib import Path
from tqdm import tqdm
import collections
from pathlib import Path
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
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
# sys.setrecursionlimit(1000000000)

param = {    
    'asset_ROISall_joins': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/rois_grades_cerr_caat_embeddin'
}


#========================METODOS=============================
def listar_assets_na_pasta(folder_id):
    """
    Substitui o obsoleto ee.data.getList por ee.data.listAssets,
    que é o padrão atual da API do Google Earth Engine.
    """
    try:
        # Puxa a lista de assets (limitado nativamente se houver milhares, mas seguro)
        lista_completa = ee.data.listAssets({'parent': folder_id})['assets']
        colection_ptos = [asset['name'] for asset in lista_completa]
        return colection_ptos
    except Exception as e:
        print(f"Erro ao listar assets na pasta {folder_id}: {e}")
        return []


#========================METODOS=============================
#exporta a imagem classificada para o asset
def processoExportar(ROIsFeat, nameB, nfolder):    
    optExp = {
          'collection': ROIsFeat, 
          'description': nameB, 
          'folder': nfolder          
        }
    task = ee.batch.Export.table.toDrive(**optExp)
    task.start() 
    print("salvando ... " + nameB + "..!")    



lstAssetFolder = listar_assets_na_pasta(param['asset_ROISall_joins'])
list_baciaYearFaltan = []
# sys.exit()
for cc, assetFeats in enumerate(lstAssetFolder[:]):        
    nameFeat = assetFeats.split("/")[-1].split("_")[-1]
    print(f" #{cc + 1}/{len(lstAssetFolder)} loading FeatureCollection => ", assetFeats.split("/")[-1])
    try: 
        ROIs = ee.FeatureCollection(assetFeats)                    
        # print(nameFeat, " ", ROIs.size().getInfo())     
        processoExportar(ROIs, nameFeat, "ROIs_EMBEDDING_CC4")              
    except:
        # list_baciaYearFaltan.append(nameFeat)
        # arqFaltante.write(nameFeat + '\n')
        print("faltando ... " + nameFeat)

