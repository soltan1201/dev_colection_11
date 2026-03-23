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
    'asset_ROISall_joins': {'id': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/ROIs/ROIs_Savana'},
    'anoInicial': 1985,
    'anoFinal': 2024,
    'showFilesCSV' : False,
    'showAssetFeat': False
}

#lista de anos
list_anos = [k for k in range(param['anoInicial'],param['anoFinal'] + 1)]
print('lista de anos', list_anos)

#========================METODOS=============================
def GetPolygonsfromFolder(dictAsset):    
    getlistPtos = ee.data.getList(dictAsset)
    ColectionPtos = []
    # print("bacias vizinhas ", nBacias)
    for idAsset in tqdm(getlistPtos):         
        path_ = idAsset.get('id')        
        ColectionPtos.append(path_) 
        name = path_.split("/")[-1]
        if param['showAssetFeat']:
            print("Reading ", name)        
    
    return ColectionPtos


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


# sys.exit()

# get dir path of script 
npath = os.getcwd()
# get dir folder before to path scripts 
npath = str(Path(npath).parents[0])
# folder of CSVs ROIs
roisPath = '/dados/Col10_ROIs_grades/'
npath += roisPath
print("path of CSVs Rois is \n ==>",  npath)


lstPathCSV = glob.glob(npath + "*.csv")
lstNameFeat = []
for xpath in tqdm(lstPathCSV):
    nameCSV = xpath.split("/")[-1][:-4]
    if param['showFilesCSV']:
        print(" => " + nameCSV)
    lstNameFeat.append(nameCSV)

lstColImp = [
    'ndfia','gcvi_median_dry','nddi_median_dry','nbr_median_dry','red_median_contrast',
    'evi_median_dry','osavi_median_dry','ri_median_dry','slope','soil_median_dry',
    'ui_median_dry','wetness_median_dry','brightness_median_dry','evi_median_wet',
    'nddi_median_wet','ratio_median_dry','avi_median_dry','osavi_median_wet',
    'msavi_median_dry', 'msavi_median_wet','classe'
]

lstNameFeat = [
    "7438","752","7584","761111","7591", "751", "7422",
    "7619","765","7712","773","7746","7615","7411","7424",
    "745","755","7561", "7564",'7616','7443','746','753',
    '7541', '7544','757','7581','7592','761112','76116',
    '7612','7613','7614','7617','7618','7619','7622','7625',
    '763','764','766','7671','7691','771','772','7721','7741',
    '7754','7761','7764'
]
# lstNameFeat = []
# lstNameFeat = []
# sys.exit()
# iterando com cada uma das folders FeatC do asset
lstKeysFolder = ['asset_ROISall_joins']   
for assetKey in lstKeysFolder:
    lstAssetFolder = GetPolygonsfromFolder(param[assetKey])
    # print(lstAssetFolder[:5])
    list_baciaYearFaltan = []
    # sys.exit()
    for cc, assetFeats in enumerate(lstAssetFolder[:]):        
        nameFeat = assetFeats.split("/")[-1].split("_")[-1]
        # print(nameFeat)
        if str(nameFeat) in lstNameFeat:
            print(f" #{cc} loading FeatureCollection => ", assetFeats.split("/")[-1])
            try: 
                ROIs = (ee.FeatureCollection(assetFeats)
                            .select(lstColImp).remap([1,2], [1, 0], 'classe'))       
                # print(nameFeat, " ", ROIs.size().getInfo())     
                processoExportar(ROIs, nameFeat, "ROIs_Joined_Allv3")              
            except:
                # list_baciaYearFaltan.append(nameFeat)
                # arqFaltante.write(nameFeat + '\n')
                print("faltando ... " + nameFeat)
        else:
            print(f"basin < {nameFeat} > was prosseced")
        # arqFaltante.close()
        # cont = gerenciador(cont)