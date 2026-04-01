#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
SCRIPT DE CLASSIFICACAO POR BACIA
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2
'''

import ee
import os
import sys
from pathlib import Path
import arqParametros_class as arqParams

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account  # noqa: E402
from gee_tools import *  # noqa: E402, F403

projAccount = get_current_account()
print(f"projeto selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project=projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise

# ============================================================
# PARÂMETROS GLOBAIS
# ============================================================
VERSION = 1

ASSET_BACIAS  = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions'
ASSET_COLECAO = 'LANDSAT/COMPOSITES/C02/T1_L2_32DAY'
ASSET_ROIS    = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesCCred'
ASSET_OUT     = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1'

ANO_INIT = 1985
ANO_FIN  = 2025

BND_L = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']

PMT_GTB = {
    'numberOfTrees': 35,
    'shrinkage': 0.1,
    'samplingRate': 0.65,
    'loss': 'LeastSquares',
    'seed': 0,
}

LST_FEAT_SELECT = [
    'green_median_dry', 'green_median_wet',
    'red_median_dry',   'red_median_wet',    'swir1_median_wet',
    'swir2_median_wet', 'swir2_median',      'swir2_median_dry',
    # 'ndti_median_dry',  'ndti_median_wet',   'ndti_median',
    # 'brba_median_dry',  'brba_median_wet',
    'gli_median_dry',   #'mbi_median_dry',    'shape_median_dry',
    'ndvi_median',      'ndvi_median_dry',   'ndvi_median_wet',
    # 'ndwi_median_dry',  'ndwi_median',       'ndwi_median_wet',
    # 'gcvi_median',      'bsi_median',        'ui_median',
    'awei_median',      'awei_median_wet',   'awei_median_dry',
]

nameBacias = [
    '765', '7544', '7541', '7411', '746', '7591', '7592',
    '761111', '761112', '7612', '7613', '7614', '7615',
    '771', '7712', '772', '7721', '773', '7741', '7746', '7754',
    '7761', '7764', '7691', '7581', '7625', '7584', '751',
    '752', '7616', '745', '7424', '7618', '7561', '755', '7617',
    '7564', '7422', '76116', '7671', '757', '766', '753', '764',
    '7619', '7443', '7438', '763', '7622'
]


# ============================================================
# FUNÇÕES
# ============================================================

def check_dir(file_name):
    if not os.path.exists(file_name):
        arq = open(file_name, 'w+')
        arq.close()


def build_mosaic(baciabuffer, nyear):
    '''Constrói mosaico de 3 períodos.
    col é um único objeto Python — garante deduplicação na serialização GEE.
    Usa clip(geometry) para evitar multiplicar subgrafos de reduceToImage.'''
    col = (ee.ImageCollection(ASSET_COLECAO)
             .filterBounds(baciabuffer)
             .filter(ee.Filter.date(f'{nyear}-01-01', f'{nyear}-12-31'))
             .select(BND_L))

    bnd_median = [b + '_median'     for b in BND_L]
    bnd_wet    = [b + '_median_wet' for b in BND_L]
    bnd_dry    = [b + '_median_dry' for b in BND_L]

    mosaic_year = col.max().rename(bnd_median)
    mosaic_wet  = col.filter(ee.Filter.date(f'{nyear}-01-01', f'{nyear}-07-31')).max().rename(bnd_wet)
    mosaic_dry  = col.filter(ee.Filter.date(f'{nyear}-08-01', f'{nyear}-12-31')).max().rename(bnd_dry)

    return ee.Image.cat([mosaic_year, mosaic_wet, mosaic_dry]).clip(baciabuffer)


def add_spectral_indices(mosaic, lst_feat):
    '''Calcula índices espectrais. Todos referenciam `mosaic` diretamente
    para manter o grafo plano (sem nós intermediários).'''
    sufixos       = ['_median', '_median_wet', '_median_dry']
    formulas_base = arqParams.FORMULAS_INDICES_ESPECTRAIS
    indices = []

    for s in sufixos:
        for nome_indice, expressao in formulas_base.items():
            nome_banda = f"{nome_indice}{s}"
            if nome_banda in lst_feat:
                expr = expressao.format(s=s)
                indices.append(mosaic.expression(f"float({expr})").rename(nome_banda))

    return ee.Image.cat([mosaic] + indices)


def classify_bacia(nbacia, nyear):
    print(f"\n=== classificando bacia {nbacia} ano {nyear} ===")

    # geometria — clip simples, sem reduceToImage
    bacias_fc   = ee.FeatureCollection(ASSET_BACIAS).filter(ee.Filter.eq('nunivotto4', nbacia))
    baciabuffer = bacias_fc.geometry()

    # mosaico
    mosaic = build_mosaic(baciabuffer, nyear)

    # índices espectrais
    mosaic_full   = add_spectral_indices(mosaic, LST_FEAT_SELECT)
    mosaic_classif = mosaic_full.select(LST_FEAT_SELECT)

    # amostras
    ano_amostra = nyear if nyear <= 2024 else 2024
    rois_path   = os.path.join(ASSET_ROIS, f"rois_fromBasin_{nbacia}_{ano_amostra}")
    ROIs = ee.FeatureCollection(rois_path)

    # classificador
    classifier = (ee.Classifier.smileGradientTreeBoost(**PMT_GTB)
                    .train(ROIs, 'class', LST_FEAT_SELECT))

    # classificação
    band_result = f'classification_{nyear}'
    classified  = mosaic_classif.classify(classifier, band_result)
    classified  = classified.set({
        'id_bacia':   nbacia,
        'version':    VERSION,
        'biome':      'CAATINGA',
        'classifier': 'GTB',
        'collection': '11.0',
        'sensor':     'Landsat',
        'year':       nyear,
        'bands':      'fm',
    })
    classified = classified.set('system:footprint', baciabuffer.coordinates())

    # export
    nome_desc = f"BACIA_{nbacia}_{nyear}_GTB_col11_BND_fm-v_{VERSION}"
    asset_id  = os.path.join(ASSET_OUT, nome_desc)

    task = ee.batch.Export.image.toAsset(
        image=classified,
        description=nome_desc,
        assetId=asset_id,
        region=baciabuffer,
        scale=30,
        maxPixels=1e13,
        pyramidingPolicy={'.default': 'mode'},
    )
    task.start()
    print(f"  task enviada: {nome_desc}")
    for k, v in dict(task.status()).items():
        print(f"    {k}: {v}")


# ============================================================
# LOOP PRINCIPAL
# ============================================================

registros_proc = "registros/lsBaciasClassifyfeitasv_1.txt"
pathFolder = os.getcwd()
path_MGRS = os.path.join(pathFolder, registros_proc)
baciasFeitas = []
check_dir(path_MGRS)

arqFeitos = open(path_MGRS, 'r')
for ii in arqFeitos.readlines():
    ii = ii[:-1]
    baciasFeitas.append(ii)
arqFeitos.close()
arqFeitos = open(path_MGRS, 'a+')

knowMapSaved = False
lst_year_serie = list(range(ANO_INIT, ANO_FIN + 1))
print(f"list of year to process {len(lst_year_serie)}")
print(f"we have {len(nameBacias)} bacias")

listBacFalta = []

lst_bacias_saved = (ee.ImageCollection(ASSET_OUT)
                        .reduceColumns(ee.Reducer.toList(), ['system:index'])
                        .get('list').getInfo())
print(f" ====== we have {len(lst_bacias_saved)} maps saved ====")
print(lst_bacias_saved[:2])

for _nbacia in nameBacias[2:10]:
    if knowMapSaved:
        try:
            nameMap = 'BACIA_' + _nbacia + '_' + 'GTB_col11_BND_fm-v_' + str(VERSION)
            imgtmp = ee.Image(os.path.join(ASSET_OUT, nameMap))
            print(" loading ", nameMap, " ", len(imgtmp.bandNames().getInfo()), " bandas")
        except:
            listBacFalta.append(_nbacia)
    else:
        print("-------------------------------------------------------------")
        print(f"-------------    classificando bacia nova << {_nbacia} >> ---------------")
        print("-------------------------------------------------------------")

        lst_temporal = [raster_bacia for raster_bacia in lst_bacias_saved if f'BACIA_{_nbacia}_' in raster_bacia]
        lst_years = []
        for ii in lst_temporal:
            print(ii)
            lst_years.append(int(ii.split("_")[2]))
        print(f" ---- {len(lst_years)} years feitos ")
        lst_year_process = [nyear for nyear in lst_year_serie if nyear not in lst_years]
        print(lst_year_process)
        for nyear in lst_year_process:
            classify_bacia(_nbacia, nyear)

    # sys.exit()
arqFeitos.close()
