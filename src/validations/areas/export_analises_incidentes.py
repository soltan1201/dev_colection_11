#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
# SCRIPT DE CLASSIFICACAO POR BACIA
# Produzido por Geodatin - Dados e Geoinformacao
# DISTRIBUIDO COM GPLv2
'''

import ee 
import sys
import os
import copy
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
    'asset_Col71' : 'projects/mapbiomas-public/assets/brazil/lulc/collection7_1/mapbiomas_collection71_integration_v1',
    'asset_Col8' : 'projects/mapbiomas-public/assets/brazil/lulc/collection8/mapbiomas_collection80_integration_v1',
    'asset_Col9' : "projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1",
    'asset_Col10' : "projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_coverage_v2",
    'asset_biomas_raster': 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019-raster',
    'asset_sphBiomas': "projects/mapbiomas-workspace/AUXILIAR/biomas-2019",
    'assetOutput': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/aggrements',
    "br_estados_raster": 'projects/mapbiomas-workspace/AUXILIAR/estados-2016-raster',
    "BR_ESTADOS_2022" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/BR_ESTADOS_2022",
    'vetor_biomas_250': 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
    'classMapB': [3, 4, 5, 9,12,13,15,18,19,20,21,22,23,24,25,26,29,30,31,32,33,36,39,40,41,46,47,48,49,50,62],
    'classNew':  [3, 4, 3, 3,12,12,21,21,21,21,21,22,22,22,22,33,29,22,33,12,33,21,21,21,21,21,21,21, 3,12,21],
}

"""
//   '#faf3dd' >> 'Classe 1' Concordante
//   '#c8d5b9' >> 'Classe 2' Concordante recente 
//   '#f19c79' >> 'Classe 3' Discordante recente
//   '#fec601' >> 'Classe 4' Discordante
//   '#013a63' >> 'Classe 5' Muito discordante
dict_layer_color = {
    '0': '#fbe79c',
    '1': '#faf3dd',
    '2': '#c8d5b9',
    '3': '#f19c79',
    '4': '#fec601',
    '5': '#013a63'
}
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
        'classe', item.get('classe'),"area", item.get('sum'))
        
    return feature

#########################################################################
####     Calculate area crossing a cover map (deforestation, mapbiomas)
####     and a region map (states, biomes, municipalites)
####      @param image 
####      @param geometry
#########################################################################
# https://code.earthengine.google.com/5a7c4eaa2e44f77e79f286e030e94695
def calculateArea (image, pixelArea, reg_geom):

    pixelArea = pixelArea.addBands(image.rename('classe'))
    reducer = ee.Reducer.sum().group(1, 'classe')
    optRed = {
        'reducer': reducer,
        'geometry': reg_geom,
        'scale': 30,
        'bestEffort': True, 
        'maxPixels': 1e13
    }    
    areas = pixelArea.reduceRegion(**optRed)

    areas = ee.List(areas.get('groups')).map(lambda item: convert2featCollection(item))
    areas = ee.FeatureCollection(areas)    
    return areas

def iterandoXanoImCruda(imgAreaP, imgMapp1, imgMapp2,  imgMapp3, regEst, estadoCod):

    areaGeral = ee.FeatureCollection([]) 
    for nyear in range(1985, 2023):
        # para cada ano  
        print("processing year >> ", nyear)
        nyear = str(nyear)
        band_select = 'classification_'+ nyear

        raster1YY = (
            imgMapp1.select(band_select)
                .remap(params["classMapB"], params["classNew"])
        )
        raster2YY = (
            imgMapp2.select(band_select)
                .remap(params["classMapB"], params["classNew"])
        )
        raster3YY = (
            imgMapp3.select(band_select)
                .remap(params["classMapB"], params["classNew"])
        )
        rasterClass = raster1YY.addBands(raster2YY).addBands(raster3YY)
        incidentes = rasterClass.reduce(ee.Reducer.countRuns()).subtract(1).rename('incidentes')
        states = rasterClass.reduce(ee.Reducer.countDistinctNonNull()).rename('states')

        moda = rasterClass.reduce(ee.Reducer.mode())
        # ///logica de definição de classes está embasada no fato de termos 3 coleções de entrada
        # //para analisar mais coleções a logica precisa ser reestruturada
        clas1 = incidentes.eq(0).selfMask()
        clas2 = incidentes.eq(1).And(rasterClass.select(2).subtract(moda).eq(0)).selfMask()
        clas3 = incidentes.eq(1).And(rasterClass.select(0).subtract(moda).eq(0)).selfMask()
        clas4 = incidentes.eq(2).And(states.eq(2)).selfMask()
        clas5 = incidentes.eq(2).And(states.eq(3)).selfMask()

        outMapsYY = (
            clas1.blend(clas2.multiply(2))
                .blend(clas3.multiply(3))
                    .blend(clas4.multiply(4))
                        .blend(clas5.multiply(5))
                            .rename('classes')
        )

        areaTemp = calculateArea (outMapsYY, imgAreaP, regEst)        
        areaTemp = areaTemp.map( lambda feat: feat.set(
                                            'year', nyear,                                          
                                            'estado_codigo', estadoCod,
                                        ))
        areaGeral = areaGeral.merge(areaTemp)

    return areaGeral


def processoExportar(areaFeat, nameT):      
    optExp = {
          'collection': areaFeat, 
          'description': nameT, 
          'folder': 'mudanzas_filtersXClass'        
        }    
    task = ee.batch.Export.table.toDrive(**optExp)
    task.start() 
    print("salvando ... " + nameT + "..!")    

# bioma =  'Caatinga'; 
CD_Bioma = 2
estados_raster = ee.Image(params["br_estados_raster"])
lstEstCruz = [22,23,24,25,26,27,28,29,31]

limitGeometria = ee.FeatureCollection(params["vetor_biomas_250"])
limitGeometria = limitGeometria.filter(ee.Filter.eq("CD_Bioma", CD_Bioma))
pixelArea = ee.Image.pixelArea().divide(10000)
rasterCaat = ee.Image(params['asset_biomas_raster']).eq(5)
#Map.addLayer(ee.Image.constant(1), {min: 0, max: 1}, 'base');
mapsCol71 = ee.Image(params["asset_Col71"]).updateMask(rasterCaat)
mapsCol80 = ee.Image(params["asset_Col8"]).updateMask(rasterCaat)
#  9.0 - Classificação Integração 
mapsCol90 = ee.Image(params["asset_Col9"]).updateMask(rasterCaat)
#  10.0 - Classificação Integração 
mapsCol100 = ee.Image(params["asset_Col10"]).updateMask(rasterCaat)
colecoes = 'cols789' #'cols8910'

for estadoCod in lstEstCruz:        
    print(f"processing Estado {dictEst[str(estadoCod)]} with code {estadoCod}")
    maskRasterEstBioma = estados_raster.eq(estadoCod).multiply(rasterCaat)
    
    rasterMapEst80 = mapsCol80.updateMask(maskRasterEstBioma)
    rasterMapEst90 = mapsCol90.updateMask(maskRasterEstBioma)
    
    imgPixelAreaEst = pixelArea.updateMask(maskRasterEstBioma)
    regionEst = (
        ee.FeatureCollection(params['BR_ESTADOS_2022'])
        .filter(ee.Filter.eq("CD_UF", str(estadoCod)))
        .geometry().intersection(limitGeometria.geometry())            
    )        
    # imgAreaP, imgMapp71, imgMapp80, imgMapp90, regEst, estadoCod
    if colecoes == 'cols8910':
        rasterMapEst100 = mapsCol100.updateMask(maskRasterEstBioma)
        areaCC = iterandoXanoImCruda(pixelArea, rasterMapEst80, rasterMapEst90, rasterMapEst100, regionEst, estadoCod)

    else:
        rasterMapEst71 = mapsCol71.updateMask(maskRasterEstBioma)
        areaCC = iterandoXanoImCruda(pixelArea, rasterMapEst71, rasterMapEst80, rasterMapEst90, regionEst, estadoCod)

    print(f"====================== ESTADO {estadoCod} =================================")
    nameSHP = f'incidentes_pixels_est_{colecoes}_{estadoCod}'
    processoExportar(areaCC, nameSHP)
    # sys.exit()
