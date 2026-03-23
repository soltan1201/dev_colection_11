#!/usr/bin/env python2
# -*- coding: utf-8 -*-

##########################################################
## CRIPT DE EXPORTAÇÃO DO RESULTADO FINAL PARA O ASSET  ##
## DE mAPBIOMAS                                         ##
## Produzido por Geodatin - Dados e Geoinformação       ##
##  DISTRIBUIDO COM GPLv2                               ##
#########################################################

import ee 
import gee
import json
import csv
import sys
import collections
collections.Callable = collections.abc.Callable

try:
    ee.Initialize()
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise


def gerenciador(cont, paramet):
    #0, 18, 36, 54]
    #=====================================#
    # gerenciador de contas para controlar# 
    # processos task no gee               #
    #=====================================#
    numberofChange = [kk for kk in paramet['conta'].keys()]
    
    if str(cont) in numberofChange:

        print("conta ativa >> {} <<".format(paramet['conta'][str(cont)]))        
        gee.switch_user(paramet['conta'][str(cont)])
        gee.init()        
        gee.tasks(n= paramet['numeroTask'], return_list= True)        
    
    elif cont > paramet['numeroLimit']:
        return 0
    
    cont += 1    
    return cont


param = {
    'asset_caat_buffer': 'users/CartasSol/shapes/caatinga_buffer5km',
    'outputAsset': 'projects/mapbiomas-workspace/COLECAO9/classificacao', 
    'inputAsset': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/Classifier/toExport',
    # 'newinputAsset': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/Classifier/toExport',  # BACIA_778_mixed_V244
    # projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/Classifier/toExport/BACIA_7742_mixed_V26
    'biome': 'CAATINGA', #configure como null se for tema transversal
    'version': 26,
    'collection': 9.0,
    'source': 'geodatin',
    'setUniqueCount': True,
    'theme': None, 
    'numeroTask': 0,
    'numeroLimit': 39,
    'conta' : {
        '0': 'caatinga01',   # 
        '2': 'caatinga02',
        '4': 'caatinga03',
        '6': 'caatinga04',
        '8': 'caatinga05',        
        '10': 'solkan1201',    
        '12': 'solkanGeodatin',
        '14': 'diegoUEFS',
        '16': 'superconta' 
    }
}

nameBacias = [
    '744','741','7421', '7422','745','746','7492','751','752',
    '753','755','758','759','7621','7622','764','765','766',
    '767','771','7741','772','7742','773','775', '777', '778',
    '76111','76116','7612','7614','7615','7616','7617','7618','7619', 
    '7613','756','757','763','776', '754',
]
countFix = 16
processExport = True
metadados = {}
bioma5kbuf = ee.FeatureCollection(param['asset_caat_buffer']).geometry()
# imgColExpss = ee.ImageCollection(param['inputAsset']).filter(
#                                             ee.Filter.eq('version', 26))
# numMaps = imgColExpss.size().getInfo()
# print(f' We have {numMaps} imagens maps by basin in this asset')

lstImg = ee.List([])
for cc, id_bacias in enumerate(nameBacias):    
    nameImg = f'BACIA_{id_bacias}_mixed_V26'
    try:
        imgExpss = ee.Image(param['inputAsset'] + '/' + nameImg)
        print(f"merge {cc} === {nameImg} >>> {imgExpss.get('system:index').getInfo()}")
    except:
        imgColExpss = ee.ImageCollection(param['inputAsset']).filter(
                                            ee.Filter.eq('system:index', nameImg)).first()
    lstImg = lstImg.add(imgExpss)

imgColExp = ee.ImageCollection.fromImages(lstImg)

# sys.exit()
# if numMaps != 42:
#     lstIds = imgColExpss.reduceColumns(ee.Reducer.toList(),['id_bacia']).get('list').getInfo()
#     print("lista de bacias ", lstIds)
#     sys.exit()

imgColExp = imgColExp.map(lambda img: ee.Image.cat(img).toByte()).min()
print("lista de bandas da imagem min \n ", imgColExp.bandNames().getInfo())

for ii, year in enumerate(range(2023, 2024)):  #    
    # if param['setUniqueCount']:
    #     countFix = gerenciador(countFix, param)
    #     countFix = 16
    # else:
    #     countFix = gerenciador(ii , param)
 
    bandaAct = 'classification_' + str(year) 
    # print("Banda activa: " + bandaAct)
    # img_banda = 'CAATINGA-' + str(year) +  '-' + str(param['version'])
    imgExtraBnd = imgColExp.select([bandaAct], ['classification'])
    imgYear = imgExtraBnd.clip(bioma5kbuf).set('biome', param['biome'])\
                    .set('year', year)\
                    .set('version', str(param['version']))\
                    .set('collection', param['collection'])\
                    .set('source', param['source'])\
                    .set('theme',None)\
                    .set('system:footprint', bioma5kbuf)    

    
    name = param['biome'] + '-' + str(year) + '-' + str(param['version'])
    if processExport:
        optExp = {   
            'image': imgYear.byte(), 
            'description': name, 
            'assetId': param['outputAsset'] + '/' + name, 
            'region': bioma5kbuf.getInfo()['coordinates'], #
            'scale': 30, 
            'maxPixels': 1e13,
            "pyramidingPolicy": {".default": "mode"}
        }
        task = ee.batch.Export.image.toAsset(**optExp)
        task.start() 
        print("salvando ... banda  " + name + "..!")
    else:
        print(f"verficando => {name} >> {imgYear.bandNames().getInfo()}")
        
        # sys.exit()