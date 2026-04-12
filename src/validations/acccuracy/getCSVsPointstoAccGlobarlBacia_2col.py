#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
#SCRIPT DE CLASSIFICACAO POR BACIA
#Produzido por Geodatin - Dados e Geoinformacao
#DISTRIBUIDO COM GPLv2
'''

import ee 
import os
import json
import csv
import sys
import collections
collections.Callable = collections.abc.Callable

from pathlib import Path

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
pathparent = str(Path(os.getcwd()).parents[1])
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



#exporta a imagem classificada para o asset
def processoExportar(ROIsFeat, nameT, porAsset):  

    if porAsset:
        if 'project' in nameT:
            asset_ids = param['assetpointLapig24rc'] + nameT
            nameT = nameT.split("/")[-1]
        else:
            asset_ids = "projects/geo-data-s/assets/accuracy/" + nameT
        
        optExp = {
          'collection': ROIsFeat, 
          'description': nameT, 
          'assetId': asset_ids        
        }
        task = ee.batch.Export.table.toAsset(**optExp)
        task.start() 
        print("salvando ... " + nameT + "..!")
    else:
        optExp = {
            'collection': ROIsFeat, 
            'description': nameT, 
            'folder':"ptosAccCol10corr",
            # 'priority': 1000          
            }
        task = ee.batch.Export.table.toDrive(**optExp)
        task.start() 
        print("salvando ... " + nameT + "..!")
        # print(task.status())
    


#nome das bacias que fazem parte do bioma
nameBacias = [
    '765', '7544', '7541', '7411', '746', '7591', '7592', 
    '761111', '761112', '7612', '7613', '7614', '7615', 
    '771', '7712', '772', '7721', '773', '7741', '7746', '7754', 
    '7761', '7764', '7581', '7625', '7584', '751',     
    '7616', '745', '7424', '7618', '7561', '755', '7617', 
    '7564', '7422', '76116', '7671', '757', '766', '753', '764',
    '7619', '7443', '7438', '763', '7622', '752'  #    '7691',  
] 

param = {
    'lsBiomas': ['CAATINGA'],
    'asset_bacias': 'projects/ee-solkancengine17/assets/shape/bacias_shp_caatinga_div_49_regions', # asset bacia revisado 
    'assetBiomas' : 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
    # 'assetpointLapig23': 'projects/mapbiomas-workspace/VALIDACAO/mapbiomas_85k_col3_points_w_edge_and_edited_v2', 
    'assetpointLapig23': 'projects/mapbiomas-workspace/VALIDACAO/mapbiomas_85k_col5_points_w_edge_and_edited_v3',
    'assetpointLapig24rc': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/Classifier/mapbiomas_85k_col4_points_w_edge_and_edited_v1_Caat',   
    'limit_bacias': "users/CartasSol/shapes/bacias_limit",
    'asset_caat_buffer': 'users/CartasSol/shapes/caatinga_buffer5km',

    'asset_Map_col9' : "projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1",
    'asset_Map_col10' : "projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_coverage_v2",
    # 'assetCol6': path_asset + "class_filtered/maps_caat_col6_v2_4",
    'classMapB': [3, 4, 5, 9,12,13,15,18,19,20,21,22,23,24,25,26,29,30,31,32,33,36,39,40,41,46,47,48,49,50,62],
    'classNew':  [3, 4, 3, 3,12,12,21,21,21,21,21,22,22,22,22,33,29,22,33,12,33,21,21,21,21,21,21,21, 3,12,21],
    'classesMapAmp':  [3, 4, 3, 3,12,12,15,18,18,18,21,22,22,22,22,33,29,22,33,12,33,18,18,18,18,18,18,18, 3,12,18],
    'inBacia': False,
    'anoInicial': 1985,
    'anoFinal': 2024,  # 2019
    'numeroTask': 6,
    'numeroLimit': 2,
    'changeAcount': False,
    'conta' : {
        '0': 'solkanGeodatin'              
    },
    'lsProp': ['BIOMA_250K', 'CARTA_2','DECLIVIDAD','PESO_AMOS', 'LON', 'LAT'],
    "amostrarImg": True,
    'isImgCol': False
}

def change_value_class(feat):
    ## Load dictionary of class
    dictRemap =  {
        "FORMAÇÃO FLORESTAL": 3,
        "FORMAÇÃO SAVÂNICA": 4,        
        "MANGUE": 3,
        "RESTINGA HERBÁCEA": 3,
        "FLORESTA PLANTADA": 21,
        "FLORESTA INUNDÁVEL": 3,
        "CAMPO ALAGADO E ÁREA PANTANOSA": 12,
        "APICUM": 12,
        "FORMAÇÃO CAMPESTRE": 12,
        "AFLORAMENTO ROCHOSO": 22,
        "OUTRA FORMAÇÃO NÃO FLORESTAL":12,
        "PASTAGEM": 21,
        "CANA": 21,
        "LAVOURA TEMPORÁRIA": 21,
        "LAVOURA PERENE": 21,
        "MINERAÇÃO": 22,
        "PRAIA E DUNA": 22,
        "INFRAESTRUTURA URBANA": 22,
        "VEGETAÇÃO URBANA": 22,
        "OUTRA ÁREA NÃO VEGETADA": 22,
        "RIO, LAGO E OCEANO": 33,
        "AQUICULTURA": 33,
        "NÃO OBSERVADO": 27  
    }
    pts_remap = ee.Dictionary(dictRemap);

    prop_select = [ 'BIOMA_250K', 'CARTA_2','DECLIVIDAD','PESO_AMOS', 'LON', 'LAT']
    
    feat_tmp = feat.select(prop_select)
    for year in range(1985, 2025):
        nam_class = "CLASS_" + str(year)
        set_class = "CLASS_" + str(year)
        valor_class = ee.String(feat.get(nam_class))
        feat_tmp = feat_tmp.set(set_class, pts_remap.get(valor_class))
    
    return feat_tmp


def getPointsAccuraciaFromIC (ptosAccCorreg):
    """

    """
    imClassCol9 = ee.Image(param['asset_Map_col9'])
    imClassCol10 = ee.Image(param['asset_Map_col10'])
    print(imClassCol9.bandNames().getInfo())
    print("Número de pontos ", ptosAccCorreg.size().getInfo())
    
    #lista de anos
    list_anosC10 = [str(k) for k in range(param['anoInicial'], param['anoFinal'] + 1)]
    list_bandasC10 = [f"c10_classification_{k}" for k in list_anosC10]
    list_bandasC9 = [f"c9_classification_{k}" for k in range(param['anoInicial'], param['anoFinal'])]
    print(list_bandasC9)
    imClassCol9 = imClassCol9.rename(list_bandasC9)
    print(imClassCol9.bandNames().getInfo())
    imClassCol10 = imClassCol10.rename(list_bandasC10)
    imClassCol10 = imClassCol10.addBands(imClassCol9)
    list_bandasCC = imClassCol10.bandNames().getInfo()
    print("bands de 2 coleção ", list_bandasCC)
    

    # sys.exit()
    # print('lista de anos', list_anos)
    # update properties 
    lsAllprop = param['lsProp'].copy()
    for ano in list_anosC10:
        band = 'CLASS_' + str(ano)
        lsAllprop.append(band)

    # featureCollection to export colected 
    pointAll = ee.FeatureCollection([])
    ftcol_bacias = ee.FeatureCollection(param['asset_bacias'])
    sizeFC = ee.Number(0 )
    
    for cc, _nbacia in enumerate(nameBacias[:]):    
        # nameImg = 'mapbiomas_collection80_Bacia_v' + str(version) 
        print(f"-------  📢📢 processando img #  {cc} na bacia {_nbacia}  🫵 -------- ")
        baciaTemp = ftcol_bacias.filter(ee.Filter.eq('nunivotto4', _nbacia)).geometry()    
        geomRecBacia = ee.FeatureCollection([ee.Feature(ee.Geometry(baciaTemp), {'id_codigo': 1})])
        maskRecBacia = geomRecBacia.reduceToImage(['id_codigo'], ee.Reducer.first()).gt(0) 
        
        pointTrueTemp = ptosAccCorreg.filterBounds(baciaTemp)
        ptoSize = pointTrueTemp.size()#.getInfo()
        print(cc, " - bacia - ", _nbacia, " points Reference ")  
        sizeFC = sizeFC.add(ptoSize)
    

        print(" 🚨  reading the one image ")
        mapClassBacia = ee.Image().byte()            
        for band_act in list_bandasCC:
            mapClassBacia = mapClassBacia.addBands(
                                ee.Image(imClassCol10).select(band_act).updateMask(maskRecBacia)
                                .remap(param['classMapB'], param['classNew'])
                                .rename(band_act)
            )
        mapClassBacia = mapClassBacia.select(list_bandasCC)
            # print(mapClassBacia.bandNames().getInfo())
            # sys.exit()
        try:
            #
            pointAccTemp = mapClassBacia.unmask(0).sampleRegions(
                collection= pointTrueTemp, 
                properties= lsAllprop, 
                scale= 30, 
                geometries= True
            )
            pointAccTemp = pointAccTemp.map(lambda Feat: Feat.set('bacia', _nbacia))
            # print("size of points Acc coletados ", pointAccTemp.size().getInfo())
            pointAll = ee.Algorithms.If(  
                            ee.Algorithms.IsEqual(ee.Number(ptoSize).eq(0), 1),
                            pointAll,
                            ee.FeatureCollection(pointAll).merge(pointAccTemp)
                        )
        except:
            print("⚠️ ERRO WITH LOADING IMAGE MAP 🚨")


    name =  f"occTab_corr_col90_col10_85k_col5_points_w_edge_and_edited_v3"
    processoExportar(pointAll, name, True)
    print()
    print(" 📢 numero de ptos ", sizeFC.getInfo())

    # sys.exit()



expPointLapig = True
knowImgcolg = True
param['isImgCol'] = False
param['inBacia'] = True

bioma250mil = ee.FeatureCollection(param['assetBiomas'])\
                    .filter(ee.Filter.eq('Bioma', 'Caatinga')).geometry()
## os pontos só serão aqueles que representam a Caatinga 
caatingaBuffer = ee.FeatureCollection(param['asset_caat_buffer'])


pointTrue = ee.FeatureCollection(param['assetpointLapig23']).filterBounds(bioma250mil) 
print("Carregamos {} points ".format(pointTrue.size().getInfo()))  # pointTrue.size().getInfo()
# print("know the first points ", pointTrue.first().getInfo())

print("########## 🔊 LOADING MAP RASTER ###############")
### call to function samples  #######
#                           ptosAccCorreg, , version, exportByBasin, exportarAsset,subbfolder

getPointsAccuraciaFromIC (pointTrue)

