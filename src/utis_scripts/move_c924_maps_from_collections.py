#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
#SCRIPT DE CLASSIFICACAO POR BACIA
#Produzido por Geodatin - Dados e Geoinformacao
#DISTRIBUIDO COM GPLv2
'''

import ee
import os 
import sys
from pathlib import Path
import collections
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
print("parents ", pathparent)
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
# sys.setrecursionlimit(1000000000)


param = {
    'asset_bacias_buffer' : 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions',
    'assetMapbiomas90': 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1', 
    'classMapB': [3, 4, 5, 6, 9, 11, 12, 13, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32, 33, 35, 36, 39, 40, 41, 46, 47, 48, 49, 50, 62],
    'classNew':  [3, 4, 3, 3, 3, 12, 12, 12, 21, 21, 21, 21, 21, 22, 22, 22, 22, 33, 29, 22, 33, 12, 33, 21, 21, 21, 21, 21, 21, 21, 21,  4, 12, 21],
    'asset_collection_by_year': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/Classifier/ClassifyV2YX',
    'asset_output': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/Classifier/ClassifyVA',
    'bioma': "CAATINGA",
    'version_input': 10,
    'version_output': 10
}
nameBacias = [
    '7691', '7754', '7581', '7625', '7584', '751', '7614', 
    '7616', '745', '7424', '773', '7612', '7613', 
    '7618', '7561', '755', '7617', '7564', '761111','761112', 
    '7741', '7422', '76116', '7761', '7671', '7615', '7411', 
    '7764', '757', '771', '766', '7746', '753', '764', 
    '7541', '7721', '772', '7619', '7443','7544', '7438', 
    '763', '7591', '7592', '746','7712', '752', '765', '7622',
]
def processoExportar(mapaRF, regionB, nomeDesc):
    idasset =  os.path.join(param['asset_output'] , nomeDesc)
    optExp = {
        'image': mapaRF, 
        'description': nomeDesc, 
        'assetId':idasset, 
        'region':ee.Geometry(regionB), #['coordinates'] .getInfo()
        'scale': 30, 
        'maxPixels': 1e13,
        "pyramidingPolicy":{".default": "mode"},
        # 'priority': 1000
    }
    task = ee.batch.Export.image.toAsset(**optExp)
    task.start() 
    print("salvando ... " + nomeDesc + "..!")
    # print(task.status())
    for keys, vals in dict(task.status()).items():
        print ( "  {} : {}".format(keys, vals))

lst_year = [yyear for yyear in range(1985, 2026)]
lstbaciabuffer = ee.FeatureCollection(param['asset_bacias_buffer'])
        
imCol90 = ee.Image(param['assetMapbiomas90'])
# lstBaciasreproc = [ "7613","7746","7754","7741","773","761112","7591","7581","757"]
# lstBaciasreproc = [ "7613","7746","7741","7591","7581","757"]
lstBaciasreproc = ["7591"]
imColbyYear = ee.ImageCollection(param['asset_collection_by_year'])
# lstIdCodbYear = imColbyYear.reduceColumns(ee.Reducer.toList(), ['system:index']).get('list').getInfo()
# print(lstIdCodbYear)
# lstExc = ['751', '7625']
# lstNameBacias = [f'BACIA_{cbasin}_2024_GTB_col10-v_4' for cbasin in lstExc]
for nbacia in lstBaciasreproc[:]:
    print(f'load {nbacia} ')
    baciabuffer = lstbaciabuffer.filter(ee.Filter.eq('nunivotto4', nbacia))
    print(f"know about the geometry 'nunivotto4' >>  {nbacia} loaded < {baciabuffer.size().getInfo()} > geometry" ) 
    baciabuffer = baciabuffer.map(lambda f: f.set('id_codigo', 1))
    bacia_raster =  baciabuffer.reduceToImage(['id_codigo'], ee.Reducer.first()).gt(0)
    baciabuffer = baciabuffer.geometry()    
    colBacia = ee.Image().byte()
    lstBands = []
    for nyear in range(1985, 2025):
        bandActiva = f'classification_{nyear}' 
        if nbacia in lstBaciasreproc:
            newNameInd = f'BACIA_{nbacia}_{nyear}_GTB_col10-v_{param['version_input']}'
        else:
            newNameInd = f'BACIA_{nbacia}_{nyear}_GTB_col10-v_4'
        print("addding ", newNameInd)
        imMapsYY = imColbyYear.filter(ee.Filter.eq('system:index', newNameInd)).first()
        # print(imMapsYY.bandNames().getInfo())
        imMapsYY = (ee.Image(imMapsYY).remap(param['classMapB'], param['classNew'])
                                .toUint8().rename(bandActiva))
        colBacia = colBacia.addBands(imMapsYY)
        lstBands.append(bandActiva)
    
    colBacia = colBacia.select(lstBands)
    # print(colBacia.bandNames().getInfo())
    # sys.exit()
    mydict = {
            'id_bacia': nbacia,
            'version': param['version_output'],
            'biome': param['bioma'],
            'classifier': 'GTB',
            'collection': '10.0',
            'sensor': 'Landsat',
            'source': 'geodatin'        
        }                    
    colBacia = colBacia.set(mydict)
    colBacia = colBacia.set("system:footprint", baciabuffer.coordinates())
    processoExportar(colBacia, baciabuffer, f'BACIA_{nbacia}_GTB_col10-v_{param['version_output']}')
