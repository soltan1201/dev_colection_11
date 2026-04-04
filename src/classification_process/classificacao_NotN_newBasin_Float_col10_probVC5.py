#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
SCRIPT DE CLASSIFICACAO POR GRADE — VC5
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2

Lógica:
  - Verifica quais bacia/ano faltam em Classify_fromEEMV1 (VC4)
  - Para cada faltante classifica GRADE a GRADE
  - Função de classificação recebe 1 geometria (grade) + 1 ano → grafo simples
  - Usa a mesma abordagem de mosaico e índices do VC4 (sem SMA/NDFIa/slope)
  - Exporta em Classify_fromEEMV1grid: GRADE_{indice}_{bacia}_{ano}_GTB_col11_BND_fm-v_{VERSION}
'''

import argparse
import ee
import os
import json
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
VERSION  = 1
ANO_INIT = 1985
ANO_FIN  = 2025

ASSET_BACIAS   = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions'
ASSET_GRADE    = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/basegrade30KMCaatinga'
ASSET_COLECAO  = 'LANDSAT/COMPOSITES/C02/T1_L2_32DAY'
ASSET_ROIS     = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesCCred'
ASSET_OUT_BACIA = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1'
# ASSET_OUT_GRID  = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1grid'

BND_L = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']

PMT_GTB = {
    'numberOfTrees': 35,
    'shrinkage':     0.1,
    'samplingRate':  0.65,
    'loss':          'LeastSquares',
    'seed':          0,
}

LST_FEAT_SELECT = [
    'green_median_dry',  'green_median_wet',
    'red_median_dry',    'red_median_wet',   'swir1_median_wet',
    'swir2_median_wet',  'swir2_median',     'swir2_median_dry',
    'ndti_median_dry',   'ndti_median_wet',  'ndti_median',
    'gli_median_dry',
    'ndvi_median',       'ndvi_median_dry',  'ndvi_median_wet',
    'ndwi_median_dry',   'ndwi_median',      'ndwi_median_wet',
    'awei_median',       'awei_median_wet',  'awei_median_dry',
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
# FUNÇÕES DE MOSAICO E ÍNDICES  (mesma lógica do VC4)
# ============================================================

def build_mosaic(gradeGeom, nyear):
    '''Mosaico simples de 3 períodos clipado na geometria da grade.'''
    col = (ee.ImageCollection(ASSET_COLECAO)
           .filterBounds(gradeGeom)
           .filter(ee.Filter.date(f'{nyear}-01-01', f'{nyear}-12-31'))
           .select(BND_L))

    bnd_median = [b + '_median'     for b in BND_L]
    bnd_wet    = [b + '_median_wet' for b in BND_L]
    bnd_dry    = [b + '_median_dry' for b in BND_L]

    mosaic_year = col.max().rename(bnd_median)
    mosaic_wet  = col.filter(ee.Filter.date(f'{nyear}-01-01', f'{nyear}-07-31')).max().rename(bnd_wet)
    mosaic_dry  = col.filter(ee.Filter.date(f'{nyear}-08-01', f'{nyear}-12-31')).max().rename(bnd_dry)

    return ee.Image.cat([mosaic_year, mosaic_wet, mosaic_dry]).clip(gradeGeom)


def add_spectral_indices(mosaic):
    '''Calcula apenas os índices presentes em LST_FEAT_SELECT.'''
    sufixos       = ['_median', '_median_wet', '_median_dry']
    formulas_base = arqParams.FORMULAS_INDICES_ESPECTRAIS
    indices = []

    for s in sufixos:
        for nome_indice, expressao in formulas_base.items():
            nome_banda = f"{nome_indice}{s}"
            if nome_banda in LST_FEAT_SELECT:
                banda = mosaic.expression(
                    f"float({expressao.format(s=s)})"
                ).rename(nome_banda)
                indices.append(banda)

    return ee.Image.cat([mosaic] + indices)


# ============================================================
# FUNÇÃO PRINCIPAL: 1 grade + 1 ano
# ============================================================

def classify_grade(gradeGeom, nyear, ROIs):
    '''
    Recebe uma única geometria de grade e um único ano.
    Retorna a imagem classificada pronta para exportar.
    '''
    # mosaico simples
    mosaic = build_mosaic(gradeGeom, nyear)

    # índices espectrais
    mosaic_full    = add_spectral_indices(mosaic)
    mosaic_classif = mosaic_full.select(LST_FEAT_SELECT)

    # treina e classifica
    classifier = (ee.Classifier.smileGradientTreeBoost(**PMT_GTB)
                  .train(ROIs, 'class', LST_FEAT_SELECT))

    band_result = f'classification_{nyear}'
    return mosaic_classif.classify(classifier, band_result)


def export_grade(classified, gradeGeom, idGrade, nbacia, nyear):
    nomeDesc = f"GRADE_{idGrade}_{nbacia}_{nyear}_GTB_col11_BND_fm-v_{VERSION}"
    assetId  = os.path.join(ASSET_OUT_GRID, nomeDesc)

    classified = classified.set({
        'indice':     int(idGrade),
        'id_bacia':   nbacia,
        'version':    VERSION,
        'biome':      'CAATINGA',
        'classifier': 'GTB',
        'collection': '11.0',
        'sensor':     'Landsat',
        'year':       nyear,
        'bands':      'fm',
    })
    classified = classified.set('system:footprint', gradeGeom.coordinates())

    task = ee.batch.Export.image.toAsset(
        image=classified,
        description=nomeDesc,
        assetId=assetId,
        region=gradeGeom,
        scale=30,
        maxPixels=1e13,
        pyramidingPolicy={'.default': 'mode'},
    )
    task.start()
    print(f"    task enviada: {nomeDesc}")
    for k, v in dict(task.status()).items():
        print(f"      {k}: {v}")


# ============================================================
# AUXILIAR
# ============================================================

def check_dir(file_name):
    if not os.path.exists(file_name):
        open(file_name, 'w+').close()


# ============================================================
# LOOP PRINCIPAL
# ============================================================

parser = argparse.ArgumentParser()
parser.add_argument('position_t0', type=int, default=0, nargs='?', help='inicio da lista de bacias')
parser.add_argument('position_t1', type=int, default=5, nargs='?', help='fim da lista de bacias')
args     = parser.parse_args()
pos_inic = args.position_t0
pos_end  = args.position_t1

# carrega dict grade → bacia
pathDictGrade = os.path.join(pathparent, 'samples_process', 'dict_basin_49_lista_grades.json')
with open(pathDictGrade, 'r') as f:
    dictGradeBacia = json.load(f)

# assets já salvos
lst_bacias_saved = (ee.ImageCollection(ASSET_OUT_BACIA)
                    .reduceColumns(ee.Reducer.toList(), ['system:index'])
                    .get('list').getInfo())
lst_grades_saved = (ee.ImageCollection(ASSET_OUT_GRID)
                    .reduceColumns(ee.Reducer.toList(), ['system:index'])
                    .get('list').getInfo())

print(f" ====== {len(lst_bacias_saved)} mapas por bacia (VC4) ======")
print(f" ====== {len(lst_grades_saved)} mapas por grade (VC5) ======")

lst_year_serie = list(range(ANO_INIT, ANO_FIN + 1))
gradeFC        = ee.FeatureCollection(ASSET_GRADE)

registros_proc = 'registros/lsBaciasGradeClassifyfeitasv5.txt'
check_dir(os.path.join(os.getcwd(), registros_proc))
arqFeitos = open(os.path.join(os.getcwd(), registros_proc), 'a+')

print(f"processando bacias [{pos_inic}:{pos_end}] de {len(nameBacias)}")

for _nbacia in nameBacias[pos_inic:pos_end]:
    print("=================================================================")
    print(f"=== bacia << {_nbacia} >> ===")

    # anos já exportados pelo VC4 nessa bacia
    lst_temporal  = [r for r in lst_bacias_saved if f'BACIA_{_nbacia}_' in r]
    lst_years_ok  = [int(r.split('_')[2]) for r in lst_temporal]
    lst_year_falta = [y for y in lst_year_serie if y not in lst_years_ok]

    print(f"  VC4 feitos: {len(lst_years_ok)} | faltam: {len(lst_year_falta)}")

    if not lst_year_falta:
        print(f"  bacia {_nbacia} completa no VC4. Nada a fazer.")
        continue

    lst_grades = dictGradeBacia.get(_nbacia, [])
    if not lst_grades:
        print(f"  AVISO: bacia {_nbacia} sem grades no dict.")
        continue

    print(f"  grades da bacia: {len(lst_grades)}")

    for nyear in lst_year_falta:
        print(f"\n  --- ano {nyear} ---")

        # ROIs da bacia (carregado uma vez por bacia/ano)
        ano_amostra    = nyear if nyear <= 2024 else 2024
        dir_asset_rois = os.path.join(ASSET_ROIS, f"rois_fromBasin_{_nbacia}_{ano_amostra}")
        ROIs           = ee.FeatureCollection(dir_asset_rois)

        for idGrade in lst_grades:
            idGrade_str = str(idGrade)
            nomeDesc    = f"GRADE_{idGrade_str}_{_nbacia}_{nyear}_GTB_col11_BND_fm-v_{VERSION}"

            if nomeDesc in lst_grades_saved:
                print(f"    grade {idGrade_str} | {nyear} já salva. Pulando.")
                continue

            # busca geometria da grade
            gradeFeature = gradeFC.filter(ee.Filter.eq('indice', int(idGrade)))
            if gradeFeature.size().getInfo() == 0:
                print(f"    grade {idGrade_str} não encontrada no asset. Pulando.")
                continue

            gradeGeom = gradeFeature.geometry()

            print(f"    classificando grade {idGrade_str} ...")
            try:
                classified = classify_grade(gradeGeom, nyear, ROIs)
                export_grade(classified, gradeGeom, idGrade_str, _nbacia, nyear)
            except Exception as e:
                print(f"    ERRO grade {idGrade_str} | {nyear}: {e}")
                continue

    arqFeitos.write(_nbacia + '\n')

arqFeitos.close()
print("=== FIM ===")
