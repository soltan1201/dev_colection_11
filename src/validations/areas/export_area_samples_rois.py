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
    'asset_coletion71' : 'projects/mapbiomas-public/assets/brazil/lulc/collection7_1/mapbiomas_collection71_integration_v1',
    "asset_coletion80": 'projects/mapbiomas-public/assets/brazil/lulc/collection8/mapbiomas_collection80_integration_v1',
    "asset_coletion90": 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1',
    "asset_coletion100": 'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_coverage_v2',
    "asset_fogo_anual": 'projects/mapbiomas-workspace/FOGO/COLLECTIONS/COL04/1_Subprodutos/mapbiomas-fire-collection4-annual-burned-area_ha-v1',
    "asset_desmatamento": 'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_deforestation_secondary_vegetation_v2',
    'asset_biomas_raster': 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019-raster',
    'asset_sphBiomas': "projects/mapbiomas-workspace/AUXILIAR/biomas-2019",
    'assetOutput': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/aggrements',
    "br_estados_raster": 'projects/mapbiomas-workspace/AUXILIAR/estados-2016-raster',
    "BR_ESTADOS_2022" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/BR_ESTADOS_2022",
    'vetor_biomas_250': 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
    'classMapB': [3, 4, 5, 9,12,13,15,18,19,20,21,22,23,24,25,26,29,30,31,32,33,36,39,40,41,46,47,48,49,50,62,75],
    'classNew':  [3, 4, 3, 3,12,12,21,21,21,21,21,22,22,22,22,33,29,22,33,12,33,21,21,21,21,21,21,21, 3,12,21,22],
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
#exporta a imagem classificada para o asset
def processoExportar(mapaRF, nameB, regionB):

    idasset =  os.path.join(params['assetOutput'] , nameB)

    optExp = {
        'image': mapaRF, 
        'description': nameB, 
        'assetId':idasset, 
        'region':ee.Geometry(regionB), #['coordinates'] .getInfo()
        'scale': 30, 
        'maxPixels': 1e13,
        "pyramidingPolicy":{".default": "mode"},
        # 'priority': 1000
    }
    task = ee.batch.Export.image.toAsset(**optExp)
    task.start() 
    print("salvando ... " + nameB + "..!")
    # print(task.status())
    for keys, vals in dict(task.status()).items():
        print ( "  {} : {}".format(keys, vals))

def iterandoXanoImCruda(raster_estados, regEst, estadoCod):

    mapsCol80 = ee.Image(params["asset_coletion80"]).updateMask(raster_estados)
    #  9.0 - Classificação Integração 
    mapsCol90 = ee.Image(params["asset_coletion90"]).updateMask(raster_estados)
    #  10.0 - Classificação Integração 
    mapsCol100 = ee.Image(params["asset_coletion100"]).updateMask(raster_estados)
    print("Mapas de Cobertura em raster", mapsCol100.bandNames().getInfo())

    fogo_anual = ee.Image(params["asset_fogo_anual"]).updateMask(raster_estados)# .select("scar_area_ha_" + year_courrent).unmask(0).gt(0);
    print("Queimada em raster Annual", fogo_anual.bandNames().getInfo())
    desmatamento_raster = ee.Image(params["asset_desmatamento"]).updateMask(raster_estados) # .select("classification_" + year_courrent);
    print("Desmatamento em raster Annual", desmatamento_raster.bandNames().getInfo())

    layer_area_samples = ee.Image().byte()
    list_bands = []
    for nyear in range(1985, 2025):
        # para cada ano  
        print("processing year >> ", nyear)
        # --- DEFINIÇÃO DOS ANOS PARA ESTABILIDADE TEMPORAL (X-1, X, X+1) ---
        # Garante que não busque anos fora do limite (1985 a 2024)
        ano_prev = max(1985, nyear - 1)
        ano_next = min(2024, nyear + 1)

        nyear = str(nyear)
        band_select = 'classification_'+ nyear
        if int(nyear) < 2023:
            raster1YY = (
                mapsCol80.select(band_select)
                    .remap(params["classMapB"], params["classNew"])
            )
        else:
            raster1YY = (
                mapsCol80.select('classification_2022')
                    .remap(params["classMapB"], params["classNew"])                   
            )
        if int(nyear) < 2024:
            raster2YY = (
                mapsCol90.select(band_select)
                    .remap(params["classMapB"], params["classNew"])
            )
        else:
            raster2YY = (
                mapsCol90.select('classification_2023')
                    .remap(params["classMapB"], params["classNew"])                    
            )
        
        raster3YY = (
            mapsCol100.select(band_select)
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
        layer_area_samples_tmp = outMapsYY.lt(3)

        # --- LÓGICA DE ESTABILIDADE TEMPORAL (COLEÇÃO 100) ---
        # Remapeia o X-1 e X+1 para manter a consistência com as classes agregadas.
        # O ano corrente (X) já foi remapeado na variável 'raster3YY'.
        c100_prev = mapsCol100.select(f'classification_{ano_prev}').remap(params["classMapB"], params["classNew"])
        c100_next = mapsCol100.select(f'classification_{ano_next}').remap(params["classMapB"], params["classNew"])

        # Confirma se X-1 == X e se X == X+1
        estabilidade_temporal = c100_prev.eq(raster3YY).And(raster3YY.eq(c100_next))
        # ------------------------------------------------------
        
        raster_desmatamento = (desmatamento_raster.select(f"classification_{nyear}").eq(4)
                        .add(desmatamento_raster.select(f"classification_{nyear}").eq(6)))
        raster_desmatamento = raster_desmatamento.eq(0)

        fogo_anual_yy =  fogo_anual.select(f"scar_area_ha_{nyear}").unmask(0).eq(0)

        layer_area_samples_tmp = layer_area_samples_tmp.And(raster_desmatamento).And(fogo_anual_yy).And(estabilidade_temporal)
        band_activa = f'layer_samples_{nyear}'
        list_bands.append(band_activa)

        layer_area_samples = layer_area_samples.addBands(layer_area_samples_tmp.rename(band_activa))         
         
    layer_area_samples = (layer_area_samples.select(list_bands)
                            .set( 
                                "CD_UF", str(estadoCod), 
                                "system:footprint", regEst
                            )    
                    )

    return layer_area_samples

# https://code.earthengine.google.com/ca21328043aae9a52542fb9633286127
# bioma =  'Caatinga'; 
CD_Bioma = 2
estados_raster = ee.Image(params["br_estados_raster"])
lstEstCruz = [22,23,24,25,26,27,28,29,31]
colecoes = 'cols8910' #'cols8910'
limitGeometria = ee.FeatureCollection(params["vetor_biomas_250"])
limitGeometria = limitGeometria.filter(ee.Filter.eq("CD_Bioma", CD_Bioma))

rasterCaat = ee.Image(params['asset_biomas_raster']).eq(5)


for estadoCod in lstEstCruz:        
    print(f"processing Estado {dictEst[str(estadoCod)]} with code {estadoCod}")
    maskRasterEstBioma = estados_raster.eq(estadoCod).multiply(rasterCaat)

    regionEst = (
        ee.FeatureCollection(params['BR_ESTADOS_2022'])
        .filter(ee.Filter.eq("CD_UF", str(estadoCod)))
        .geometry()#.intersection(limitGeometria.geometry())            
    )
    
    mask_area_confiavel = iterandoXanoImCruda(maskRasterEstBioma, regionEst, estadoCod)

    print(f"====================== ESTADO {estadoCod} =================================")
    nameSHP = f'incidentes_pixels_est_{colecoes}_{estadoCod}'
    processoExportar(mask_area_confiavel, nameSHP, regionEst)
    # sys.exit()
