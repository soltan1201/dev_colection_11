#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
SCRIPT DE CLASSIFICACAO POR GRADE (fallback do VC4 por bacia)
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2

Lógica:
  - Verifica quais bacia/ano faltam em Classify_fromEEMV1 (asset do VC4)
  - Para cada faltante, classifica GRADE a GRADE usando a lista do
    dict_basin_49_lista_grades.json
  - Mantém toda a complexidade espectral do VC2 (SMA, NDFIa, slope, indices)
  - Exporta em Classify_fromEEMV1grid com nome GRADE_{idGrade}_{bacia}_{ano}_...
'''

import ee
import os
import json
import copy
import sys
from pathlib import Path
import arqParametros_class as arqParams
import collections
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
from gee_tools import *

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

# Assets de referência
ASSET_BACIAS        = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions'
ASSET_GRADE         = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/basegrade30KMCaatinga'
ASSET_GRADE_BUFFER  = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/basegrade30KMBufferCaatinga'
ASSET_COLECAO       = 'LANDSAT/COMPOSITES/C02/T1_L2_32DAY'
ASSET_MOSAIC_MB     = 'projects/nexgenmap/MapBiomas2/LANDSAT/BRAZIL/mosaics-2'
ASSET_ROIS          = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesCCred'

# Asset de saída — grid
ASSET_OUT_BACIA     = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1'
ASSET_OUT_GRID      = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1grid'

ANO_INIT = 1985
ANO_FIN  = 2025

# ============================================================
# CLASSE PRINCIPAL (mesma complexidade do VC2)
# ============================================================
class ClassMosaic_indexs_Spectral(object):

    options = {
        'bnd_L':            ['blue', 'green', 'red', 'nir', 'swir1', 'swir2'],
        'bnd_fraction':     ['gv', 'npv', 'soil'],
        'biomas':           ['CERRADO', 'CAATINGA', 'MATAATLANTICA'],
        'bioma':            'CAATINGA',
        'version':          VERSION,
        'lsBandasMap':      [],
        'asset_bacias_buffer': ASSET_BACIAS,
        'asset_grad':          ASSET_GRADE,
        'asset_grad_buffer':   ASSET_GRADE_BUFFER,
        'asset_collectionId':  ASSET_COLECAO,
        'asset_mosaic':        ASSET_MOSAIC_MB,
        'asset_joinsGrBa':     ASSET_ROIS,
        'assetOut':            ASSET_OUT_BACIA,
        'assetOutGrid':        ASSET_OUT_GRID,
        'pmtGTB': {
            'numberOfTrees': 10,
            'shrinkage':     0.1,
            'samplingRate':  0.65,
            'loss':          'LeastSquares',
            'seed':          0,
        },
    }

    lst_feat_select = [
        'ndti_median_dry',   'brba_median_wet',   'ndti_median_wet',
        'slope',             'npv_median_dry',     'wetness_median',    'soil_median_wet',
        'awei_median',       'soil_median',        'awei_median_wet',   'npv_median_wet',  'swir2_median',
        'brba_median_dry',   'brightness_median',  'gli_median_dry',    'spri_median_dry',
        'spri_median_wet',   'red_median_wet',     'ndti_median',       'npv_median',      'awei_median_dry',
        'green_median_dry',  'shade_median_dry',   'green_median_wet',  'swir1_median_wet',
        'pri_median_dry',    'pri_median',         'swir2_median_dry',  'mbi_median_dry',
        'shape_median_dry',  'ndfia_median_dry',   'soil_median_dry',   'wetness_median_wet',
        'brightness_median_dry', 'swir2_median_wet', 'red_median_dry',  'evi_median_dry',
        'pri_median_wet',    'evi_median_wet',     'evi_median',
        'gcvi_median',       'avi_median',         'bsi_median',        'ui_median',
        'ndvi_median',       'ndvi_median_dry',    'ndwi_median_dry',
        'ndwi_median',       'ndvi_median_wet',    'ndwi_median_wet',
    ]

    def __init__(self):
        # assets já salvos no output de bacia (VC4)
        imgMapSaved = ee.ImageCollection(self.options['assetOut'])
        self.lstIDassetS = (imgMapSaved
                            .reduceColumns(ee.Reducer.toList(), ['system:index'])
                            .get('list').getInfo())
        print(f" ====== {len(self.lstIDassetS)} mapas por bacia salvos ====")

        # assets já salvos no output de grade (VC5)
        imgGridSaved = ee.ImageCollection(self.options['assetOutGrid'])
        self.lstIDgridS = (imgGridSaved
                           .reduceColumns(ee.Reducer.toList(), ['system:index'])
                           .get('list').getInfo())
        print(f" ====== {len(self.lstIDgridS)} mapas por grade salvos ====")

        self.lst_year = list(range(ANO_INIT, ANO_FIN + 1))
        self.options['lsBandasMap'] = ['classification_' + str(k) for k in self.lst_year]

        # hiperparâmetros por bacia
        pathHiper = os.path.join(pathparent, 'dados', 'dictBetterModelpmtCol10v1.json')
        with open(pathHiper, 'r') as f:
            self.dictHiperPmtTuning = json.load(f)

        self.pathFSJson = self._getPathCSV('FS_col11_json/')
        print(f"path FS JSONs >>> {self.pathFSJson}")
        self.lstBandMB = self._get_bands_mosaicos()

        # dict grade → bacia
        pathDictGrade = os.path.join(pathparent, 'samples_process', 'dict_basin_49_lista_grades.json')
        with open(pathDictGrade, 'r') as f:
            self.dictGradeBacia = json.load(f)
        print(f"dict_basin_49_lista_grades carregado: {len(self.dictGradeBacia)} bacias")

    # ----------------------------------------------------------
    # UTILITÁRIOS
    # ----------------------------------------------------------
    def _getPathCSV(self, nfolder):
        mpath = os.getcwd()
        pp = str(Path(mpath).parents[0])
        return pp + '/dados/' + nfolder

    def _get_bands_mosaicos(self):
        band_year = [b + '_median'     for b in self.options['bnd_L']]
        band_drys = [b + '_median_dry' for b in band_year]
        band_wets = [b + '_median_wet' for b in band_year]
        return band_year + [b + '_wet' for b in band_year] + [b + '_dry' for b in band_year]

    # ----------------------------------------------------------
    # SLOPE / HILLSHADE
    # ----------------------------------------------------------
    def addSlopeAndHilshade(self, img):
        dem   = ee.Image('NASA/NASADEM_HGT/001').select('elevation')
        slope = ee.Terrain.slope(dem).divide(500).toFloat()
        return img.addBands(slope.rename('slope'))

    # ----------------------------------------------------------
    # SMA / NDFIa
    # ----------------------------------------------------------
    def GET_NDFIA(self, IMAGE, sufixo):
        lstBands     = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']
        lstBandsSuf  = [b + sufixo for b in lstBands]
        lstFractions = ['gv', 'shade', 'npv', 'soil', 'cloud']
        lstFracSuf   = [f + sufixo for f in lstFractions]

        endmembers = [
            [0.05, 0.09, 0.04, 0.61, 0.30, 0.10],  # gv
            [0.14, 0.17, 0.22, 0.30, 0.55, 0.30],  # npv
            [0.20, 0.30, 0.34, 0.58, 0.60, 0.58],  # soil
            [0.0,  0.0,  0.0,  0.0,  0.0,  0.0 ],  # shade
            [0.90, 0.96, 0.80, 0.78, 0.72, 0.65],  # cloud
        ]

        fractions = (ee.Image(IMAGE).select(lstBandsSuf)
                     .unmix(endmembers=endmembers, sumToOne=True, nonNegative=True)
                     .float().rename(lstFractions))

        ndfia = fractions.expression(
            "float(((b('gv') / (1 - b('shade'))) - b('soil')) / "
            "((b('gv') / (1 - b('shade'))) + b('npv') + b('soil')))"
        ).rename('ndfia' + sufixo)

        return ee.Image.cat([fractions.rename(lstFracSuf).toFloat(), ndfia.toFloat()])

    def agregate_Bands_SMA_NDFIa(self, img):
        return (img
                .addBands(self.GET_NDFIA(img, '_median'))
                .addBands(self.GET_NDFIA(img, '_median_wet'))
                .addBands(self.GET_NDFIA(img, '_median_dry')))

    # ----------------------------------------------------------
    # ÍNDICES ESPECTRAIS
    # ----------------------------------------------------------
    def agregateBandswithSpectralIndex(self, img):
        sufixos            = ['_median', '_median_wet', '_median_dry']
        formulas_base      = arqParams.FORMULAS_INDICES_ESPECTRAIS
        bandas_base_tex    = ['osavi', 'gcvi', 'avi', 'bsi', 'ui', 'awei']
        novas_bandas_base  = []

        for s in sufixos:
            for nome_indice, expressao in formulas_base.items():
                nome_banda = f"{nome_indice}{s}"
                if (nome_banda in self.lst_feat_select) or (nome_indice in bandas_base_tex):
                    banda_calc = img.expression(
                        f"float({expressao.format(s=s)})"
                    ).rename(nome_banda)
                    novas_bandas_base.append(banda_calc)

        img_com_base      = ee.Image.cat([img] + novas_bandas_base)
        bandas_dependentes = []
        for s in sufixos:
            spri = img_com_base.expression(
                f"float((b('pri{s}') + 1) / 2)"
            ).rename(f'spri{s}')
            bandas_dependentes.append(spri)

        img_quase_pronta = ee.Image.cat([img_com_base] + bandas_dependentes)
        imagem_final     = self.addSlopeAndHilshade(img_quase_pronta)
        imagem_final     = self.agregate_Bands_SMA_NDFIa(imagem_final)
        return imagem_final

    # ----------------------------------------------------------
    # MOSAICOS
    # ----------------------------------------------------------
    def make_mosaicofromIntervalo(self, colMosaic, year_courrent, semetral=False):
        bnd_L = self.options['bnd_L']
        band_year = [b + '_median'     for b in bnd_L]
        band_wets = [b + '_median_wet' for b in bnd_L]
        band_drys = [b + '_median_dry' for b in bnd_L]

        dictPer = {
            'year': {'start': f'{year_courrent}-01-01', 'end': f'{year_courrent}-12-31', 'bnds': band_year},
            'dry':  {'start': f'{year_courrent}-08-01', 'end': f'{year_courrent}-12-31', 'bnds': band_drys},
            'wet':  {'start': f'{year_courrent}-01-01', 'end': f'{year_courrent}-07-31', 'bnds': band_wets},
        }

        lstPeriodo = ['year', 'wet'] if semetral else ['year', 'dry', 'wet']
        mosaico = None
        for periodo in lstPeriodo:
            mosaictmp = (
                colMosaic.select(bnd_L)
                .filter(ee.Filter.date(dictPer[periodo]['start'], dictPer[periodo]['end']))
                .max()
                .rename(dictPer[periodo]['bnds'])
            )
            if periodo == 'year':
                mosaico = copy.deepcopy(mosaictmp)
            else:
                mosaico = mosaico.addBands(mosaictmp)

        if semetral:
            imgUnos = ee.Image.constant([1] * len(band_year)).rename(dictPer['dry']['bnds'])
            mosaico = mosaico.addBands(imgUnos)

        return mosaico

    def make_mosaicofromIntervalo_y25(self, colMosaic, year_courrent, semetral=False):
        bnd_L = self.options['bnd_L']
        band_year = [b + '_median'     for b in bnd_L]
        band_wets = [b + '_median_wet' for b in bnd_L]
        band_drys = [b + '_median_dry' for b in bnd_L]

        mosaico = (
            colMosaic.select(bnd_L)
            .filter(ee.Filter.date(f'{year_courrent}-01-01', f'{year_courrent}-07-31'))
            .max()
            .rename(band_wets)
        )
        if semetral:
            mosaico = (mosaico
                       .addBands(ee.Image.constant([1] * len(bnd_L)).rename(band_drys))
                       .addBands(ee.Image.constant([1] * len(bnd_L)).rename(band_year)))
        return mosaico

    # ----------------------------------------------------------
    # HIPERPARÂMETROS
    # ----------------------------------------------------------
    def _get_pmt_classifier(self, _nbacia):
        gradeExpMemo = [
            '7625', '7616', '7613', '7618', '7617', '761112', '7741',
            '7615', '7721', '7619', '7443', '763', '746'
        ]
        pmtro = copy.deepcopy(self.options['pmtGTB'])
        if _nbacia in gradeExpMemo:
            pmtro['numberOfTrees'] = 18
            pmtro['shrinkage']     = 0.1
        else:
            pmtro['shrinkage'] = self.dictHiperPmtTuning[_nbacia]['learning_rate']
            lstBacias_prob = ['7541', '7544', '7592', '7612', '7615', '7712', '7721', '7741', '7746']
            if _nbacia in lstBacias_prob:
                numberTrees = 18
                pmtro['numberOfTrees'] = (
                    self.dictHiperPmtTuning[_nbacia]['n_estimators'] - 3
                    if self.dictHiperPmtTuning[_nbacia]['n_estimators'] < numberTrees
                    else numberTrees
                )
        return pmtro

    # ----------------------------------------------------------
    # CLASSIFICAÇÃO POR GRADE
    # ----------------------------------------------------------
    def iterate_grades_in_bacia(self, _nbacia, lista_years_feitos):
        '''
        Para cada ano faltante na bacia, classifica grade a grade.

        lista_years_feitos: anos já exportados em Classify_fromEEMV1 (VC4)
        '''
        lst_grades = self.dictGradeBacia.get(_nbacia, [])
        if not lst_grades:
            print(f"  AVISO: bacia {_nbacia} sem grades no dict. Pulando.")
            return

        print(f"  grades da bacia {_nbacia}: {len(lst_grades)} grades")

        # Coleções base (carregadas uma vez por bacia)
        imagens_mosaicoEE = (ee.ImageCollection(self.options['asset_collectionId'])
                              .select(self.options['bnd_L']))
        imagens_mosaico   = (ee.ImageCollection(self.options['asset_mosaic'])
                              .filter(ee.Filter.inList('biome', self.options['biomas']))
                              .filter(ee.Filter.inList('satellite', ['l5', 'l7', 'l8']))
                              .select(self.lstBandMB))

        pmtroClass    = self._get_pmt_classifier(_nbacia)
        bandas_import = self.lst_feat_select[:45]

        # Geometry da bacia (para máscara MapBiomas e ROIs)
        bacias_fc   = ee.FeatureCollection(self.options['asset_bacias_buffer']).filter(
                          ee.Filter.eq('nunivotto4', _nbacia))
        baciabuffer_geom = bacias_fc.geometry()
        baciabuffer_fc   = bacias_fc.map(lambda f: f.set('id_codigo', 1))
        bacia_raster     = baciabuffer_fc.reduceToImage(['id_codigo'], ee.Reducer.first()).gt(0)

        # Grade com buffer (geometria de clip do mosaico)
        gradeFC = ee.FeatureCollection(self.options['asset_grad_buffer'])

        lsNamesBaciasViz = arqParams.basinVizinhasNew[_nbacia]

        for nyear in self.lst_year:

            if nyear in lista_years_feitos:
                print(f"    bacia {_nbacia} ano {nyear} já feito no VC4. Pulando.")
                continue

            print(f"\n  === {_nbacia} | {nyear} — classificando por grade ===")

            # ROIs da bacia
            ano_amostra = nyear if nyear <= 2024 else 2024
            nameFeatROIs  = f"rois_fromBasin_{_nbacia}_{ano_amostra}"
            dir_asset_rois = os.path.join(self.options['asset_joinsGrBa'], nameFeatROIs)
            ROIs_toTrain   = ee.FeatureCollection(dir_asset_rois)

            # Treinamento do classificador (único por bacia/ano)
            print(f"    treinando GTB ({pmtroClass}) ...")
            classifierGTB = (ee.Classifier.smileGradientTreeBoost(**pmtroClass)
                              .train(ROIs_toTrain, 'class', bandas_import))

            # Mosaico com gap-fill MapBiomas (clip pela bacia)
            date_inic = ee.Date.fromYMD(int(nyear), 1, 1)
            date_end  = ee.Date.fromYMD(int(nyear), 12, 31)

            if nyear < 2026:
                lstCoef      = [0.8425, 0.8957, 0.9097, 0.3188, 0.969,  0.9578]
                lstIntercept = [106.7546, 115.1553, 239.0688, 1496.4408, 392.3453, 366.57]
                bandsCoef      = ee.Image.constant(lstCoef      * 3)
                bandsIntercept = ee.Image.constant(lstIntercept * 3)

                colMB = (imagens_mosaico
                         .filter(ee.Filter.eq('year', nyear))
                         .median()
                         .updateMask(bacia_raster))
                imagens_mosaicoEEv = colMB.multiply(bandsCoef).add(bandsIntercept).divide(10000)

                mosaicColGoogle  = imagens_mosaicoEE.filter(ee.Filter.date(date_inic, date_end))
                mosaicoBuilded   = self.make_mosaicofromIntervalo(mosaicColGoogle, nyear)
                mosaicoBuilded   = mosaicoBuilded.updateMask(bacia_raster)
                maskGaps         = mosaicoBuilded.unmask(-9999).eq(-9999).updateMask(bacia_raster)
                mosaicoBuilded   = mosaicoBuilded.unmask(-9999).where(maskGaps, imagens_mosaicoEEv)
                mosaicoBuilded   = mosaicoBuilded.updateMask(mosaicoBuilded.neq(-9999)).updateMask(bacia_raster)
            else:
                mosaicColGoogle = imagens_mosaicoEE.filter(ee.Filter.date(date_inic, date_end))
                mosaicoBuilded  = self.make_mosaicofromIntervalo_y25(mosaicColGoogle, nyear, True)

            # Índices espectrais no mosaico completo da bacia
            mosaicProcess = ee.Image(
                self.agregateBandswithSpectralIndex(mosaicoBuilded)
            )

            bandActiva = f'classification_{nyear}'

            # ---- loop por grade --------------------------------
            for idGrade in lst_grades:
                idGrade_str = str(idGrade)

                # verifica se já foi exportado
                nomeDesc = f"GRADE_{idGrade_str}_{_nbacia}_{nyear}_GTB_col11_BND_fm-v_{VERSION}"
                if nomeDesc in self.lstIDgridS:
                    print(f"    grade {idGrade_str} | {nyear} já salva. Pulando.")
                    continue

                print(f"    exportando grade {idGrade_str} ...")

                # geometria da grade com buffer
                gradeFeature = gradeFC.filter(ee.Filter.eq('id', int(idGrade)))
                gradeGeom    = gradeFeature.geometry()

                # classifica clipado na grade
                classifiedGrade = (mosaicProcess
                                   .clip(gradeGeom)
                                   .classify(classifierGTB, bandActiva))

                classifiedGrade = classifiedGrade.set({
                    'indice':     int(idGrade),
                    'id_bacia':   _nbacia,
                    'version':    VERSION,
                    'biome':      'CAATINGA',
                    'classifier': 'GTB',
                    'collection': '11.0',
                    'sensor':     'Landsat',
                    'year':       nyear,
                    'bands':      'fm',
                })
                classifiedGrade = classifiedGrade.set(
                    'system:footprint', gradeGeom.coordinates()
                )

                self._exportar_grade(classifiedGrade, gradeGeom, nomeDesc)

    # ----------------------------------------------------------
    # EXPORT
    # ----------------------------------------------------------
    def _exportar_grade(self, imagem, region, nomeDesc):
        assetId = os.path.join(self.options['assetOutGrid'], nomeDesc)
        optExp  = {
            'image':            imagem,
            'description':      nomeDesc,
            'assetId':          assetId,
            'region':           ee.Geometry(region),
            'scale':            30,
            'maxPixels':        1e13,
            'pyramidingPolicy': {'.default': 'mode'},
        }
        task = ee.batch.Export.image.toAsset(**optExp)
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

def gerenciador(cont, param):
    numberofChange = list(param['conta'].keys())
    if str(cont) in numberofChange:
        print(f"trocando conta #{cont} -> {param['conta'][str(cont)]}")
        switch_user(param['conta'][str(cont)])
        projAcc = get_project_from_account(param['conta'][str(cont)])
        try:
            ee.Initialize(project=projAcc)
            print('Earth Engine reinicializado!')
        except ee.EEException:
            print('Falha ao reinicializar o Earth Engine.')
        tasks(n=param['numeroTask'], return_list=True)
    elif cont > param['numeroLimit']:
        return 0
    return cont + 1

# ============================================================
# LOOP PRINCIPAL
# ============================================================
param = {
    'conta': {
        '0': 'caatinga01',
        '1': 'caatinga02',
        '2': 'caatinga03',
        '3': 'caatinga04',
        '4': 'caatinga05',
        '5': 'solkan1201',
        '6': 'solkanGeodatin',
        '7': 'superconta',
    },
    'numeroTask':  6,
    'numeroLimit': 10,
}

nameBacias = [
    '765', '7544', '7541', '7411', '746', '7591', '7592',
    '761111', '761112', '7612', '7613', '7614', '7615',
    '771', '7712', '772', '7721', '773', '7741', '7746', '7754',
    '7761', '7764', '7691', '7581', '7625', '7584', '751',
    '752', '7616', '745', '7424', '7618', '7561', '755', '7617',
    '7564', '7422', '76116', '7671', '757', '766', '753', '764',
    '7619', '7443', '7438', '763', '7622'
]

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('position_t0', type=int, default=0,  nargs='?', help='inicio da lista de bacias')
parser.add_argument('position_t1', type=int, default=5,  nargs='?', help='fim da lista de bacias')
args = parser.parse_args()
pos_inic = args.position_t0
pos_end  = args.position_t1

registros_proc = 'registros/lsBaciasGradeClassifyfeitasv5.txt'
pathFolder     = os.getcwd()
path_MGRS      = os.path.join(pathFolder, registros_proc)
check_dir(path_MGRS)

process_classification = ClassMosaic_indexs_Spectral()
lst_bacias_saved = process_classification.lstIDassetS  # assets do VC4 por bacia
lst_year_serie   = list(range(ANO_INIT, ANO_FIN + 1))

print(f"processando bacias [{pos_inic}:{pos_end}] de {len(nameBacias)}")

arqFeitos = open(path_MGRS, 'a+')

for _nbacia in nameBacias[pos_inic:pos_end]:
    print("=================================================================")
    print(f"=== bacia << {_nbacia} >> ===")
    print("=================================================================")

    # anos já feitos pelo VC4 nessa bacia
    lst_temporal  = [r for r in lst_bacias_saved if f'BACIA_{_nbacia}_' in r]
    lst_years_ok  = [int(r.split('_')[2]) for r in lst_temporal]
    lst_year_falta = [y for y in lst_year_serie if y not in lst_years_ok]

    print(f"  VC4 feitos: {len(lst_years_ok)} anos | faltam: {len(lst_year_falta)} anos")

    if not lst_year_falta:
        print(f"  bacia {_nbacia} completa no VC4. Nada a fazer.")
        continue

    process_classification.iterate_grades_in_bacia(_nbacia, lst_years_ok)
    arqFeitos.write(_nbacia + '\n')

arqFeitos.close()
print("=== FIM ===")
