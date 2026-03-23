#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
#SCRIPT DE CLASSIFICACAO POR BACIA
#Produzido por Geodatin - Dados e Geoinformacao
#DISTRIBUIDO COM GPLv2
'''

import ee 
import sys
import os
from pathlib import Path
import collections
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
pathparent = str(Path(os.getcwd()).parents[1])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account

projAccount = get_current_account()
print(f"projetos selecionado >>> {projAccount} <<<")
try:
    ee.Initialize( project= projAccount )
    print(' 🕸️ 🌵 The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise


params = {
    'asset_Col6' : 'projects/mapbiomas-public/assets/brazil/lulc/collection6/mapbiomas_collection60_integration_v1',
    'asset_Col71' : 'projects/mapbiomas-public/assets/brazil/lulc/collection7_1/mapbiomas_collection71_integration_v1',
    'asset_Col8' : 'projects/mapbiomas-public/assets/brazil/lulc/collection8/mapbiomas_collection80_integration_v1',
    'asset_Col9' : "projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1",
    'asset_Col10' : "projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_coverage_v2",
    'asset_biomas': 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019-raster',
    'asset_sphBiomas': "projects/mapbiomas-workspace/AUXILIAR/biomas-2019",
    'assetOutput': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/aggrements',
    "br_estados_raster": 'projects/mapbiomas-workspace/AUXILIAR/estados-2016-raster',
    "BR_ESTADOS_2022" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/BR_ESTADOS_2022",
    'vetor_biomas_250': 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
}

"""
//   var ano = 2021
//   AZUL somente na col 10
//   VERMELHO somente col9
//   CINZA mapeado nos 2 
//   listar anos para poerformar a análise
//   years = [2021];
"""
dictEst = {
    '21': 'MARANHÃO',
    '22': 'PIAUÍ',
    '23': 'CEARÁ',
    '24': 'RIO GRANDE DO NORTE',
    '25': 'PARAÍBA',
    '26': 'PERNAMBUCO',
    '27': 'ALAGOAS',
    '28': 'SERGIPE',
    '29': 'BAHIA',
    '31': 'MINAS GERAIS',
    '32': 'ESPÍRITO SANTO'
}

##############################################
###     Helper function
###    @param item 
##############################################
def convert2featCollection (item):
    item = ee.Dictionary(item)
    feature = ee.Feature(ee.Geometry.Point([0, 0])).set(
        'conc', item.get('conc'),"area", item.get('sum'))        
    return feature

#########################################################################
####     Calculate area crossing a cover map (deforestation, mapbiomas)
####     and a region map (states, biomes, municipalites)
####      @param image 
####      @param geometry
#########################################################################
# https://code.earthengine.google.com/5a7c4eaa2e44f77e79f286e030e94695
def calculateArea (image, pixelArea, geometry):

    pixelArea = pixelArea.addBands(image.rename('conc')).clip(geometry)#.addBands(
                                # ee.Image.constant(yyear).rename('year'))
    reducer = ee.Reducer.sum().group(1, 'conc')
    optRed = {
        'reducer': reducer,
        'geometry': geometry,
        'scale': 30,
        'bestEffort': True, 
        'maxPixels': 1e13
    }    
    areas = pixelArea.reduceRegion(**optRed)

    areas = ee.List(areas.get('groups')).map(lambda item: convert2featCollection(item))
    areas = ee.FeatureCollection(areas)    
    return areas

def iterandoXanoImCruda(imgAreaRef, imgMapp, limite, nclass, nyear):
    CD_Bioma = 2
    estados_raster = ee.Image(params["br_estados_raster"])
    lstEstCruz = [22,23,24,25,26,27,28,29,31]
    areaGeral = ee.FeatureCollection([]) 
    limitGeometria = ee.FeatureCollection(params["vetor_biomas_250"])
    limitGeometria = limitGeometria.filter(ee.Filter.eq("CD_Bioma", CD_Bioma))

    for estadoCod in lstEstCruz:        
        print(f"processing Estado {dictEst[str(estadoCod)]} with code {estadoCod}")
        maskRasterEstado = estados_raster.eq(estadoCod)
        rasterMapEstado = imgMapp.updateMask(maskRasterEstado)
        imgAreaEst = imgAreaRef.updateMask(maskRasterEstado)
        regEst = (
            ee.FeatureCollection(params['BR_ESTADOS_2022'])
            .filter(ee.Filter.eq("CD_UF", str(estadoCod)))
            .geometry().intersection(limitGeometria.geometry())            
        )

        areaTemp = calculateArea (rasterMapEstado, imgAreaEst, regEst)        
        areaTemp = areaTemp.map( lambda feat: feat.set(
                                            'year', nyear, 
                                            'classe', nclass,                                            
                                            'estado_codigo', estadoCod
                                        ))
        areaGeral = areaGeral.merge(areaTemp)

    return areaGeral


def processoExportar(areaFeat, nameT):      
    optExp = {
          'collection': areaFeat, 
          'description': nameT, 
          'folder': 'areas_aggrements_2025'        
        }    
    task = ee.batch.Export.table.toDrive(**optExp)
    task.start() 
    print("salvando ... " + nameT + "..!")    

#//exporta a imagem classificada para o asset
def processoExportarImage (imageMap, nameB, idAssetF, regionB):
    idasset =  idAssetF + "/" + nameB
    print("saving ")
    optExp = {
            'image': imageMap, 
            'description': nameB, 
            'assetId': idasset, 
            'region': regionB.getInfo()['coordinates'], #//['coordinates']
            'scale': 30, 
            'maxPixels': 1e13,
            "pyramidingPolicy":{".default": "mode"}
        }
        
    task = ee.batch.Export.image.toAsset(**optExp)
    task.start() 
    print("salvando mapa ... " + nameB + "..!");


# // 2. PARÂMETROS DE CLASSES
classMapB = [3, 4, 5, 9,11,12,13,15,18,19,20,21,22,23,24,25,26,29,30,33,36,39,40,41,42,43,44,45,46,47,48,49,50,75]
classNew =  [1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 1, 2, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1, 2]

dictClasses = {
  'natural': 1,
  'antropico': 2,
  'Agua': 3
}
bioma =  'Caatinga'
biomes = ee.Image(params['asset_biomas']).eq(5).selfMask()
limitBioma = ee.FeatureCollection(params['asset_sphBiomas']).filter(ee.Filter.eq("Bioma", bioma))

pixelArea = ee.Image.pixelArea().divide(10000).updateMask(biomes)
class_col90 = ee.Image(params['asset_Col9']).updateMask(biomes)
class_col100 = ee.Image(params['asset_Col10']).updateMask(biomes)
#Map.addLayer(ee.Image.constant(1), {min: 0, max: 1}, 'base');
# print('list bandas ', class_col100.bandNames().getInfo())
# sys.exit()

cont = 2      
#// listar classes para performar a análise 
# lst_classes = [3,4,12,29,15,18,21,22,25,33]
lst_classes = [1, 2, 3] # natuairas
for year_j in range(1985, 2024):
    # // para cada classe 
    # // para cada ano  
    band_act = f'classification_{year_j}'
    print("processing year >> ", band_act)
    year_j = str(year_j)

    col9_j = class_col90.select(band_act).remap(classMapB, classNew)
    col10_j = class_col100.select(band_act).remap(classMapB, classNew)
    featColCC = ee.FeatureCollection([])
    for class_i in lst_classes:
        print(f"select class {class_i} in aggrements ")
        images = ee.Image(0)
        #// selecionar a classificação do ano j        
        #// calcular concordância
        conc = ee.Image(0).where(col9_j.eq(class_i).And(col10_j.eq(class_i)), 1).where(   #// [1]: Concordância
                            col10_j.eq(class_i).And(col9_j.neq(class_i)), 2).where(  #// [2]: Apenas col8
                            col10_j.neq(class_i).And(col9_j.eq(class_i)), 3)  #// [3]: Apenas Col7.1
                            #//.updateMask(biomes.eq(4));
        
        conc = conc.updateMask(conc.neq(0)).rename(f'territory_{year_j}')
        areaCC = iterandoXanoImCruda(pixelArea, conc, limitBioma, class_i, year_j)
        featColCC = featColCC.merge(areaCC)
    
    nameSHP = f'Agreement_change_col910_{year_j}'
    processoExportar(featColCC, nameSHP)
    
  