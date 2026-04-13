#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
SCRIPT DE COLETA DE PONTOS PARA ACURÁCIA — getCSVsPointstoAccGlobarlBacia_2col.py
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2

Coleta amostras dos pontos de referência sobre as camadas de classificação/filtros
para cálculo de acurácia por bacia.

Argumentos:
    --tipo       Obrigatório. Tipo de entrada:
                   filter  = filtro POS-CLASS (use --filtro para especificar qual)
                   class   = classificação direta (assetCol)
                   colecao = coleção anterior MapBiomas (use --colecao para especificar qual)
    --filtro     gap_fill | temporalN | temporalA | spatial_int | frequency | spatial_all
                 (obrigatório quando --tipo=filter)
    --colecao    Map71 | Map80 | Map90 | Map100
                 (obrigatório quando --tipo=colecao)
    --version    Versão da classificação/filtro  (default: 10)
    --num_class  Número de classes: 7 ou 10  (default: 10)
    --janela     Janela temporal: 3, 4 ou 5
                 (obrigatório para --filtro temporalN ou temporalA)

Exemplos:
    python getCSVsPointstoAccGlobarlBacia_2col.py --tipo filter  --filtro spatial_all --version 10 --num_class 10
    python getCSVsPointstoAccGlobarlBacia_2col.py --tipo filter  --filtro temporalN   --version 10 --num_class 7  --janela 5
    python getCSVsPointstoAccGlobarlBacia_2col.py --tipo filter  --filtro frequency   --version 3  --num_class 10
    python getCSVsPointstoAccGlobarlBacia_2col.py --tipo class   --version 10 --num_class 10
    python getCSVsPointstoAccGlobarlBacia_2col.py --tipo colecao --colecao Map100 --num_class 7
'''

import ee
import os
import sys
import argparse
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
    ee.Initialize(project=projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise

# ---------------------------------------------------------------------------
# Argument Parsing
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(
    description='Coleta de pontos de acurácia por bacia — MapBiomas Caatinga',
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog=(
        'Exemplos:\n'
        '  %(prog)s --tipo filter  --filtro spatial_all --version 10 --num_class 10\n'
        '  %(prog)s --tipo filter  --filtro temporalN   --version 10 --num_class 7  --janela 5\n'
        '  %(prog)s --tipo filter  --filtro frequency   --version 3  --num_class 10\n'
        '  %(prog)s --tipo class   --version 10 --num_class 10\n'
        '  %(prog)s --tipo colecao --colecao Map100 --num_class 7\n'
    )
)
parser.add_argument(
    '--tipo', required=True,
    choices=['filter', 'class', 'colecao'],
    help=('"filter" = filtro POS-CLASS  |  '
          '"class" = classificação direta (assetCol)  |  '
          '"colecao" = coleção anterior (Map71/Map80/Map90/Map100)')
)
parser.add_argument(
    '--filtro',
    choices=['gap_fill', 'temporalN', 'temporalA', 'spatial_int', 'frequency', 'spatial_all'],
    help='Filtro POS-CLASS específico (obrigatório com --tipo filter)'
)
parser.add_argument(
    '--colecao',
    choices=['Map71', 'Map80', 'Map90', 'Map100'],
    help='Coleção anterior (obrigatório com --tipo colecao)'
)
parser.add_argument(
    '--version', type=int, default=10,
    help='Versão da classificação/filtro (default: 10)'
)
parser.add_argument(
    '--num_class', type=int, choices=[7, 10], default=10,
    help='Número de classes: 7 ou 10 (default: 10)'
)
parser.add_argument(
    '--janela', type=int, choices=[3, 4, 5],
    help='Janela temporal — obrigatório para --filtro temporalN ou temporalA'
)
args = parser.parse_args()

# Validações cruzadas
if args.tipo == 'filter' and not args.filtro:
    parser.error('--filtro é obrigatório quando --tipo=filter')
if args.tipo == 'colecao' and not args.colecao:
    parser.error('--colecao é obrigatório quando --tipo=colecao')
if args.filtro in ('temporalN', 'temporalA') and not args.janela:
    parser.error('--janela é obrigatório para filtros temporalN e temporalA')

print(f"\n{'='*60}")
print(f"  tipo      : {args.tipo}")
print(f"  filtro    : {args.filtro or '-'}")
print(f"  colecao   : {args.colecao or '-'}")
print(f"  version   : {args.version}")
print(f"  num_class : {args.num_class}")
print(f"  janela    : {args.janela or '-'}")
print(f"{'='*60}\n")

# ---------------------------------------------------------------------------
# Bacias e parâmetros fixos
# ---------------------------------------------------------------------------
nameBacias = [
    '765', '7544', '7541', '7411', '746', '7591', '7592',
    '761111', '761112', '7612', '7613', '7614', '7615',
    '771', '7712', '772', '7721', '773', '7741', '7746', '7754',
    '7761', '7764', '7581', '7625', '7584', '751',
    '7616', '745', '7424', '7618', '7561', '755', '7617',
    '7564', '7422', '76116', '7671', '757', '766', '753', '764',
    '7619', '7443', '7438', '763', '7622', '752'
]

param = {
    'lsBiomas': ['CAATINGA'],
    'asset_bacias': 'projects/ee-solkancengine17/assets/shape/bacias_shp_caatinga_div_49_regions',
    'assetBiomas':  'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
    # pontos de referência para acurácia
    'assetpointLapig23':   'projects/mapbiomas-workspace/VALIDACAO/mapbiomas_85k_col5_points_w_edge_and_edited_v3',
    'assetpointLapig24rc': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/Classifier/mapbiomas_85k_col4_points_w_edge_and_edited_v1_Caat',
    'asset_caat_buffer':   'users/CartasSol/shapes/caatinga_buffer5km',
    'asset_biomas_raster': 'projects/mapbiomas-workspace/AUXILIAR/biomas-raster-41',
    # assets de classificação/filtros col11
    'assetCol': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1joined',
    'asset_filters': {
        'gap_fill':    'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/POS-CLASS/Gap-fill',
        'temporalN':   'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/POS-CLASS/TemporalNat',
        'temporalA':   'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/POS-CLASS/TemporalAnt',
        'spatial_int': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/POS-CLASS/Spatials_int',
        'frequency':   'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/POS-CLASS/Frequency',
        'spatial_all': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/POS-CLASS/Spatials_all',
    },
    'colecoes_ant': {
        'Map71':  'projects/mapbiomas-public/assets/brazil/lulc/collection7_1/mapbiomas_collection71_integration_v1',
        'Map80':  'projects/mapbiomas-public/assets/brazil/lulc/collection8/mapbiomas_collection80_integration_v1',
        'Map90':  'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1',
        'Map100': 'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_integration_v2',
    },
    'anoInicial': 1985,
    'anoFinal':   2025,  # ano final dos pontos de referência disponíveis
    'lsProp': ['BIOMA_250K', 'CARTA_2', 'DECLIVIDAD', 'PESO_AMOS', 'LON', 'LAT'],
    'scale': 30,
    'driverFolder': 'ptosAccCol11',
    'changeAcount': False,
    'numeroTask':   6,
    'numeroLimit':  2,
    'conta': {
        '0': 'solkanGeodatin',
    }
}

# ---------------------------------------------------------------------------
# Remapeamento de classes segundo --num_class
#
# classMapB   : classes originais MapBiomas (entrada do remap)
# classNew_10 : saída com 10 classes — mantém afloramento (29) distinto de (25)
# classNew_7  : saída com 7 classes  — une afloramento (29) → área não vegetada (25)
# ---------------------------------------------------------------------------
classMapB = [
     0,  3,  4,  5,  6,  9, 11, 12, 13, 15, 18, 19, 20, 21, 22,
    23, 24, 25, 26, 29, 30, 31, 32, 33, 36, 37, 38, 39, 40, 41,
    42, 43, 44, 45, 46, 47, 48, 49, 50, 62, 75
]
classNew_10 = [
    27,  3,  4,  3,  3,  3, 12, 12, 12, 15, 19, 19, 19, 21, 25,
    25, 25, 25, 33, 29, 25, 33, 12, 33, 19, 33, 33, 19, 19, 19,
    36, 36, 36, 36, 36, 36, 36,  4, 12, 36, 25
]
classNew_7  = [
    27,  3,  4,  3,  3,  3, 12, 12, 12, 21, 21, 21, 21, 21, 25,
    25, 25, 25, 33, 25, 25, 33, 12, 33, 21, 33, 33, 21, 21, 21,
    21, 21, 21, 21, 21, 21, 21,  4, 12, 21, 25
]
classNew = classNew_10 if args.num_class == 10 else classNew_7

# ---------------------------------------------------------------------------
# Flags e asset derivados dos argumentos
# ---------------------------------------------------------------------------
isImgCol = (args.tipo in ('filter', 'class'))
isFilter = (args.tipo == 'filter')
version  = args.version

if args.tipo == 'filter':
    assetFilters = param['asset_filters'][args.filtro]
elif args.tipo == 'class':
    assetFilters = param['assetCol']
else:
    assetFilters = param['colecoes_ant'][args.colecao]

# Subfolder para nome do arquivo exportado
if args.tipo == 'filter':
    subfolder = f"_{args.filtro}"
    if args.janela:
        subfolder += f"_J{args.janela}"
elif args.tipo == 'class':
    subfolder = "_class"
else:
    subfolder = f"_{args.colecao}"
subfolder += f"_nc{args.num_class}"

# Ano final — coleções anteriores têm cobertura menor
anoFinal = param['anoFinal']
if args.tipo == 'colecao':
    offset_map = {'Map100': 1, 'Map90': 2, 'Map80': 3, 'Map71': 4}
    anoFinal -= offset_map.get(args.colecao, 0)

print(f"asset    : {assetFilters}")
print(f"subfolder: {subfolder}")
print(f"anos     : {param['anoInicial']} → {anoFinal}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def change_value_class(feat):
    """Remapeia as classes de referência (texto → código numérico)."""
    dictRemap = {
        "FORMAÇÃO FLORESTAL": 3,      "FORMAÇÃO SAVÂNICA": 4,
        "MANGUE": 3,                   "RESTINGA HERBÁCEA": 3,
        "FLORESTA PLANTADA": 36,       "FLORESTA INUNDÁVEL": 3,
        "CAMPO ALAGADO E ÁREA PANTANOSA": 12,
        "APICUM": 12,                  "FORMAÇÃO CAMPESTRE": 12,
        "AFLORAMENTO ROCHOSO": 29,     "OUTRA FORMAÇÃO NÃO FLORESTAL": 12,
        "PASTAGEM": 15,                "CANA": 19,
        "LAVOURA TEMPORÁRIA": 19,      "LAVOURA PERENE": 36,
        "MINERAÇÃO": 25,               "PRAIA E DUNA": 25,
        "INFRAESTRUTURA URBANA": 25,   "VEGETAÇÃO URBANA": 25,
        "OUTRA ÁREA NÃO VEGETADA": 25,
        "RIO, LAGO E OCEANO": 33,      "AQUICULTURA": 33,
        "NÃO OBSERVADO": 27,
    }
    pts_remap   = ee.Dictionary(dictRemap)
    prop_select = ['BIOMA_250K', 'CARTA_2', 'DECLIVIDAD', 'PESO_AMOS', 'LON', 'LAT']
    feat_tmp    = feat.select(prop_select)
    for year in range(param['anoInicial'], anoFinal + 1):
        nam_class = 'CLASS_' + str(year)
        feat_tmp  = feat_tmp.set(
            nam_class, pts_remap.get(ee.String(feat.get(nam_class)))
        )
    return feat_tmp


def processoExportar(ROIsFeat, nameT, porAsset=False):
    if porAsset:
        asset_ids = 'projects/geo-data-s/assets/accuracy/' + nameT
        optExp = {
            'collection':  ROIsFeat,
            'description': nameT,
            'assetId':     asset_ids,
        }
        task = ee.batch.Export.table.toAsset(**optExp)
    else:
        optExp = {
            'collection':  ROIsFeat,
            'description': nameT,
            'folder':      param['driverFolder'],
        }
        task = ee.batch.Export.table.toDrive(**optExp)
    task.start()
    print(f"salvando ... {nameT} ... !")


# ---------------------------------------------------------------------------
# Função principal: coleta pontos de acurácia da camada selecionada
# ---------------------------------------------------------------------------
def getPointsAccuraciaFromLayer(ptosAccCorreg, imgMosaic, listBandas):
    """
    Amostra pontos de referência sobre imgMosaic (imagem multi-banda)
    para cada bacia e exporta os resultados.

    Args:
        ptosAccCorreg : FeatureCollection com pontos de referência
        imgMosaic     : ee.Image com bandas classification_YEAR
        listBandas    : lista de nomes de banda ['classification_1985', ...]
    """
    ftcol_bacias = ee.FeatureCollection(param['asset_bacias'])

    # propriedades a preservar no CSV final
    lsAllprop = param['lsProp'].copy()
    for ano in range(param['anoInicial'], anoFinal + 1):
        lsAllprop.append('CLASS_' + str(ano))

    pointAll = ee.FeatureCollection([])
    sizeFC   = ee.Number(0)
    numBacias = len(nameBacias)

    for cc, nbacia in enumerate(nameBacias):
        print(f"# {cc+1}/{numBacias} +++ bacia {nbacia} +++")
        baciaTemp    = ftcol_bacias.filter(ee.Filter.eq('nunivotto4', nbacia)).geometry()
        geomRecBacia = ee.FeatureCollection([ee.Feature(ee.Geometry(baciaTemp), {'id_codigo': 1})])
        maskRecBacia = geomRecBacia.reduceToImage(['id_codigo'], ee.Reducer.first()).gt(0)

        pointTrueTemp = ptosAccCorreg.filterBounds(baciaTemp)
        ptoSize       = pointTrueTemp.size()
        print(f"   pontos na bacia: {ptoSize.getInfo()}")

        # aplica máscara da bacia e remap de classes por banda
        mapClassBacia = ee.Image().byte()
        for band_act in listBandas:
            mapClassBacia = mapClassBacia.addBands(
                imgMosaic.select(band_act)
                .updateMask(maskRecBacia)
                .remap(classMapB, classNew)
                .rename(band_act)
            )
        mapClassBacia = mapClassBacia.select(listBandas)

        try:
            pointAccTemp = (mapClassBacia.unmask(0)
                            .sampleRegions(
                                collection=pointTrueTemp,
                                properties=lsAllprop,
                                scale=param['scale'],
                                geometries=True
                            ))
            pointAccTemp = pointAccTemp.map(lambda feat: feat.set('bacia', nbacia))
            sizeFC   = sizeFC.add(ptoSize)
            pointAll = ee.Algorithms.If(
                ee.Algorithms.IsEqual(ee.Number(ptoSize).eq(0), 1),
                pointAll,
                ee.FeatureCollection(pointAll).merge(pointAccTemp)
            )
        except Exception as exc:
            print(f"⚠️ ERRO na bacia {nbacia}: {exc}")

    nameExport = f"acc_col11{subfolder}_vers_{version}"
    processoExportar(ee.FeatureCollection(pointAll), nameExport, False)
    print(f"\n📢 total de pontos: {sizeFC.getInfo()}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
bioma250mil = (ee.FeatureCollection(param['assetBiomas'])
               .filter(ee.Filter.eq('Bioma', 'Caatinga'))
               .geometry())

# Pontos de referência filtrados para o bioma
pointTrue = (ee.FeatureCollection(param['assetpointLapig23'])
             .filterBounds(bioma250mil))
print(f"Carregados {pointTrue.size().getInfo()} pontos de referência")

# Bandas a amostrar (classification_YEAR de anoInicial até anoFinal)
listBandas = ['classification_' + str(yy) for yy in range(param['anoInicial'], anoFinal + 1)]

# Monta a imagem de classificação para amostragem
if isImgCol:
    print("-------- carregando ImageCollection -----")
    imgsMaps = ee.ImageCollection(assetFilters)
    print('version histogram:', imgsMaps.aggregate_histogram('version').getInfo())

    if isFilter and args.filtro in ('temporalN', 'temporalA'):
        imgsMaps = imgsMaps.filter(ee.Filter.eq('janela', args.janela))
        print(f"filtrado por janela={args.janela}, size =", imgsMaps.size().getInfo())

    mapFiltered = imgsMaps.filter(ee.Filter.eq('version', version))
    print(f"filtrado por versão={version}, size = {mapFiltered.size().getInfo()}")

    # mosaico mínimo (bacias não se sobrepõem → .min() = valor da bacia correspondente)
    imgMosaic = mapFiltered.min()

else:
    print("-------- carregando Image raster (coleção anterior) -----")
    bioCaat   = ee.Image(param['asset_biomas_raster']).eq(5)
    imgMosaic = ee.Image(assetFilters).byte().updateMask(bioCaat)
    print("bandas disponíveis:", imgMosaic.bandNames().getInfo())

print(f"\nIniciando coleta de pontos de acurácia (num_class={args.num_class}, anos={param['anoInicial']}-{anoFinal})...")
getPointsAccuraciaFromLayer(pointTrue, imgMosaic, listBandas)
