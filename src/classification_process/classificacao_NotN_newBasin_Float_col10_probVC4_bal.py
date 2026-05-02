#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
#SCRIPT DE CLASSIFICACAO POR BACIA — versão 4 (VC4_bal)
#Produzido por Geodatin - Dados e Geoinformacao
#DISTRIBUIDO COM GPLv2
#
# Diferença em relação a VC3:
#   - Mosaico usa redutor MEDIANA (não max) em make_mosaicofromIntervalo
#   - Após construir mosaico, aplica normalização min-max por bacia/ano
#     usando percentis p01/p99 de src/dados/dict_percentis_p01_p99_bacia_ano.json
#     (gerado por src/samples_process/gerar_percentis_p01_p99_bacia_ano.py)
#   - Índices espectrais são calculados sobre as bandas brutas normalizadas
#   - version = 3  → assets nomeados fm-v_3
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
print("parents ", pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
from gee_tools import *
projAccount = get_current_account()
print(f"projetos selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project=projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise

# ==== Constantes de normalização ====
P_LOW  = 1
P_HIGH = 99

LST_BAND_IMP = [
    'blue_median',     'green_median',     'red_median',     'nir_median',     'swir1_median',     'swir2_median',
    'blue_median_dry', 'green_median_dry', 'red_median_dry', 'nir_median_dry', 'swir1_median_dry', 'swir2_median_dry',
    'blue_median_wet', 'green_median_wet', 'red_median_wet', 'nir_median_wet', 'swir1_median_wet', 'swir2_median_wet',
]

#============================================================
#============== FUNCTIONS FO SPECTRAL INDEX =================


class ClassMosaic_indexs_Spectral(object):

    # default options
    options = {
        'bnd_L': ['blue','green','red','nir','swir1','swir2'],
        'bnd_fraction': ['gv','npv','soil'],
        'biomas': ['CERRADO','CAATINGA','MATAATLANTICA'],
        'bioma': "CAATINGA",
        'version': 3,
        'lsBandasMap': [],
        'asset_bacias_buffer' : 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions',
        'asset_grad': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/basegrade30KMCaatinga',
        'asset_collectionId': 'LANDSAT/COMPOSITES/C02/T1_L2_32DAY',
        'asset_mosaic': 'projects/nexgenmap/MapBiomas2/LANDSAT/BRAZIL/mosaics-2',
        'asset_joinsGrBa': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesCCredv2',
        'assetOut': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1',
        'lsClasse': [  3,   4,  12,  15,  19,  21,  22,  29,  33,  36],
        'lsPtos':   [300, 800, 300, 650, 250, 100, 150, 150, 300, 200],
        "anoIntInit": 1985,
        "anoIntFin": 2025,
        'dict_classChangeBa': arqParams.dictClassRepre,
        'pmtGTB': {
            'numberOfTrees': 30,
            'shrinkage': 0.1,
            'samplingRate': 0.65,
            'loss': "LeastSquares",
            'seed': 0
        },
    }

    lst_feat_select = [
            'ndti_median_dry',  'brba_median_wet', 'ndti_median_wet',
            'slope', 'npv_median_dry', 'wetness_median', 'soil_median_wet',
            'awei_median', 'soil_median', 'awei_median_wet', 'npv_median_wet', 'swir2_median',
            'brba_median_dry', 'brightness_median', 'gli_median_dry', 'spri_median_dry',
            'spri_median_wet', 'red_median_wet', 'ndti_median', 'npv_median', 'awei_median_dry',
            'green_median_dry', 'shade_median_dry', 'green_median_wet', 'swir1_median_wet',
            'pri_median_dry', 'pri_median', 'swir2_median_dry', 'mbi_median_dry',
            'shape_median_dry', 'ndfia_median_dry', 'soil_median_dry', 'wetness_median_wet',
            'brightness_median_dry', 'swir2_median_wet', 'red_median_dry', 'evi_median_dry',
            'pri_median_wet', 'evi_median_wet', 'evi_median', 'gcvi_median', 'avi_median',
            'bsi_median', 'ui_median', 'ndvi_median', 'ndvi_median_dry', 'ndwi_median_dry',
            'ndwi_median', 'ndvi_median_wet', 'ndwi_median_wet'
        ]

    def __init__(self):

        imgMapSaved = ee.ImageCollection(self.options['assetOut'])
        self.lstIDassetS = imgMapSaved.reduceColumns(ee.Reducer.toList(), ['system:index']).get('list').getInfo()
        print(f" ====== we have {len(self.lstIDassetS)} maps saved ====")
        print(self.lstIDassetS[:2])
        print("==================================================")
        self.lst_year = [k for k in range(self.options['anoIntInit'], self.options['anoIntFin'] + 1)]
        print("lista de anos ", self.lst_year)
        self.options['lsBandasMap'] = ['classification_' + str(kk) for kk in self.lst_year]

        pathHiperpmtros = os.path.join(pathparent, 'dados', 'dictBetterModelpmtCol10v1.json')
        b_file = open(pathHiperpmtros, 'r')
        self.dictHiperPmtTuning = json.load(b_file)
        self.pathFSJson = getPathCSV("FS_col11_json/")
        print("==== path of CSVs of Features Selections ==== \n >>> ", self.pathFSJson)
        self.lstBandMB = self.get_bands_mosaicos()

        path_qt_limit_v2 = os.path.join(pathparent, 'dados', 'reprocess_v2', 'dict_qt_limit_v2_por_bacia.json')
        with open(path_qt_limit_v2, 'r') as f:
            self.dictQtLimitV2 = json.load(f)
        print(f"  dictQtLimitV2 carregado: {len(self.dictQtLimitV2)} bacias  ({path_qt_limit_v2})")

        path_percentis = os.path.join(pathparent, 'dados', 'dict_percentis_p01_p99_bacia_ano.json')
        with open(path_percentis, 'r') as f:
            self.dictPercentis = json.load(f)
        print(f"  dictPercentis carregado: {len(self.dictPercentis)} bacias  ({path_percentis})")
        print("bandas mapbiomas ", self.lstBandMB)


    def addSlopeAndHilshade(self, img):
        dem   = ee.Image('NASA/NASADEM_HGT/001').select('elevation')
        slope = ee.Terrain.slope(dem).divide(500).toFloat()
        return img.addBands(slope.rename('slope'))

    def GET_NDFIA(self, IMAGE, sufixo):
        lstBands     = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']
        lstBandsSuf  = [bnd + sufixo for bnd in lstBands]
        lstFractions = ['gv', 'shade', 'npv', 'soil', 'cloud']
        lstFractionsSuf = [frac + sufixo for frac in lstFractions]

        endmembers = [
            [0.05, 0.09, 0.04, 0.61, 0.30, 0.10], # gv
            [0.14, 0.17, 0.22, 0.30, 0.55, 0.30], # npv
            [0.20, 0.30, 0.34, 0.58, 0.60, 0.58], # soil
            [0.0 , 0.0,  0.0 , 0.0 , 0.0 , 0.0 ], # Shade
            [0.90, 0.96, 0.80, 0.78, 0.72, 0.65]  # cloud
        ]

        fractions = (ee.Image(IMAGE).select(lstBandsSuf)
                                .unmix(endmembers=endmembers, sumToOne=True, nonNegative=True)
                                .float())
        fractions = fractions.rename(lstFractions)

        NDFI_ADJUSTED = fractions.expression(
                                "float(((b('gv') / (1 - b('shade'))) - b('soil')) / ((b('gv') / (1 - b('shade'))) + b('npv') + b('soil')))"
                                ).rename('ndfia' + sufixo)

        NDFI_ADJUSTED = NDFI_ADJUSTED.toFloat()
        fractions = fractions.rename(lstFractionsSuf)
        RESULT_IMAGE = fractions.toFloat().addBands(NDFI_ADJUSTED)

        return ee.Image(RESULT_IMAGE).toFloat()

    def agregate_Bands_SMA_NDFIa(self, img):
        indSMA_median    = self.GET_NDFIA(img, '_median')
        indSMA_med_wet   = self.GET_NDFIA(img, '_median_wet')
        indSMA_med_dry   = self.GET_NDFIA(img, '_median_dry')
        return img.addBands(indSMA_median).addBands(indSMA_med_wet).addBands(indSMA_med_dry)

    def agregateBandsContextoEstrutural(self, img):
        kernel = ee.Kernel.square(5)
        bandas_textura = ['osavi_median', 'gcvi_median', 'avi_median', 'bsi_median', 'ui_median', 'ndfia_median', 'awei_median']

        img_base = img.select(bandas_textura)
        mean_img = img_base.reduceNeighborhood(reducer=ee.Reducer.mean(), kernel=kernel)
        std_img  = img_base.reduceNeighborhood(reducer=ee.Reducer.stdDev(), kernel=kernel)

        mean_bands = mean_img.bandNames().map(lambda b: ee.String(b).cat('_mean'))
        std_bands  = std_img.bandNames().map(lambda b: ee.String(b).cat('_stdDev'))

        mean_img = mean_img.rename(mean_bands)
        std_img  = std_img.rename(std_bands)

        return img.addBands(mean_img).addBands(std_img)

    def agregateBandswithSpectralIndex(self, img):
        sufixos       = ['_median', '_median_wet', '_median_dry']
        formulas_base = arqParams.FORMULAS_INDICES_ESPECTRAIS
        novas_bandas_base = []
        bandas_base_textura = ['osavi', 'gcvi', 'avi', 'bsi', 'ui', 'awei']

        for s in sufixos:
            print("sufixos ==> ", s)
            for nome_indice, expressao in formulas_base.items():
                nome_banda = f"{nome_indice}{s}"
                if (nome_banda in self.lst_feat_select) or (nome_indice in bandas_base_textura):
                    expr_formatada = expressao.format(s=s)
                    banda_calc = img.expression(f"float({expr_formatada})").rename(nome_banda)
                    novas_bandas_base.append(banda_calc)

        img_com_base = ee.Image.cat([img] + novas_bandas_base)

        bandas_dependentes = []
        for s in sufixos:
            lai  = img_com_base.expression(f"float(3.618 * (b('evi{s}') - 0.118))").rename(f"lai{s}")
            spri = img_com_base.expression(f"float((b('pri{s}') + 1) / 2)").rename(f"spri{s}")
            bandas_dependentes.extend([lai, spri])

        img_quase_pronta = ee.Image.cat([img_com_base] + bandas_dependentes)
        imagem_final = self.addSlopeAndHilshade(img_quase_pronta)
        imagem_final = self.agregate_Bands_SMA_NDFIa(imagem_final)
        print("-----ADICIONOU TODOS OS INDICES ESPTRAIS AO PASSAR POR AQUI -----------")
        return imagem_final

    def apply_percentile_scaling(self, img, nbacia, nyear):
        """Normalização min-max p01/p99 nas bandas brutas — valores ficam em [0, 10000]."""
        key_year    = min(nyear, 2024)  # 2025 usa stats de 2024
        bacia_stats = self.dictPercentis.get(str(nbacia), {}).get(str(key_year), {})
        if not bacia_stats:
            print(f"  WARN: sem percentis para bacia={nbacia} ano={nyear}. Scaling ignorado.")
            return img

        scaled_bands = []
        for band in LST_BAND_IMP:
            p_low  = float(bacia_stats.get(f"{band}_p{P_LOW}",  0)     or 0)
            p_high = float(bacia_stats.get(f"{band}_p{P_HIGH}", 10000) or 10000)
            range_ = max(p_high - p_low, 1.0)

            scaled = (img.select(band)
                         .subtract(p_low)
                         .divide(range_)
                         .clamp(0, 1)
                         .multiply(10000)
                         .rename(band))
            scaled_bands.append(scaled)

        return ee.Image.cat(scaled_bands)

    def make_mosaicofromIntervalo(self, colMosaic, year_courrent, semetral=False):
        band_year = [nband + '_median'     for nband in self.options['bnd_L']]
        band_wets = [bnd  + '_wet'         for bnd   in band_year]
        band_drys = [bnd  + '_dry'         for bnd   in band_year]
        dictPer = {
            'year': {
                'start': str(year_courrent) + '-01-01',
                'end':   str(year_courrent) + '-12-31',
                'bnds':  band_year
            },
            'dry': {
                'start': str(year_courrent) + '-08-01',
                'end':   str(year_courrent) + '-12-31',
                'bnds':  band_drys
            },
            'wet': {
                'start': str(year_courrent) + '-01-01',
                'end':   str(year_courrent) + '-07-31',
                'bnds':  band_wets
            }
        }
        mosaico    = None
        lstPeriodo = ['year', 'wet'] if semetral else ['year', 'dry', 'wet']
        for periodo in lstPeriodo:
            mosaictmp = (
                colMosaic.select(self.options['bnd_L'])
                    .filter(ee.Filter.date(dictPer[periodo]['start'], dictPer[periodo]['end']))
                    .median()  # MEDIANA — substitui .max() da VC3
                    .rename(dictPer[periodo]['bnds'])
            )
            if periodo == 'year':
                mosaico = copy.deepcopy(mosaictmp)
            else:
                mosaico = mosaico.addBands(mosaictmp)

        if semetral:
            bands_period = dictPer['dry']['bnds']
            imgUnos = ee.Image.constant([1] * len(band_year)).rename(bands_period)
            mosaico = mosaico.addBands(imgUnos)

        return mosaico

    def make_mosaicofromIntervalo_y25(self, colMosaic, year_courrent, semetral=False):
        band_year = [nband + '_median'     for nband in self.options['bnd_L']]
        band_wets = [bnd  + '_wet'         for bnd   in band_year]
        band_drys = [bnd  + '_dry'         for bnd   in band_year]

        mosaico = (
            colMosaic.select(self.options['bnd_L'])
                .filter(ee.Filter.date(f'{year_courrent}-01-01', f'{year_courrent}-07-31'))
                .median()  # MEDIANA — substitui .max() da VC3
                .rename(band_wets)
        )

        if semetral:
            imgUnos_dry  = ee.Image.constant([1] * len(band_year)).rename(band_drys)
            imgUnos_year = ee.Image.constant([1] * len(band_year)).rename(band_year)
            mosaico = mosaico.addBands(imgUnos_dry).addBands(imgUnos_year)

        return mosaico

    def get_bands_mosaicos(self):
        band_year = [nband + '_median' for nband in self.options['bnd_L']]
        band_drys = [bnd + '_dry'      for bnd   in band_year]
        band_wets = [bnd + '_wet'      for bnd   in band_year]
        return band_year + band_wets + band_drys

    def down_samples_ROIs(self, rois_train, nbacia):
        # dictQtLimit_v1 = {
        #     '3': 520, '4': 1200, '12': 300, '15': 1070,
        #     '19': 180, '21': 200, '22': 200, '25': 250,
        #     '29': 200, '33': 100, '36': 260
        # }
        dictQtLimit_v1 = {
            '3': 320, '4': 800, '12': 100, '15': 670,
            '19': 100, '21': 80, '22': 100, '25': 150,
            '29': 100, '33': 88, '36': 260
        }
        limits_bacia = self.dictQtLimitV2.get(str(nbacia), {})
        dictQtLimit  = {k: limits_bacia.get(k, v) for k, v in dictQtLimit_v1.items()}
        print(f"  [bal-v2] bacia {nbacia} dictQtLimit => {dictQtLimit}")

        lstFeats = ee.FeatureCollection([])
        def make_random_select(featCC, limit):
            featCC = featCC.randomColumn()
            featCC = featCC.sort('random').limit(limit)
            return featCC

        all_classes = list(dictQtLimit.keys())
        for cclass in all_classes:
            feattmp = rois_train.filter(ee.Filter.eq('class', int(cclass)))
            sizeFC  = feattmp.size()
            limit   = dictQtLimit[str(cclass)]
            feattmp = ee.Algorithms.If(
                        ee.Algorithms.IsEqual(ee.Number(sizeFC).gt(ee.Number(limit)), 1),
                        make_random_select(feattmp, limit),
                        feattmp)
            lstFeats = lstFeats.merge(feattmp)

        classes_not_limited = rois_train.filter(
            ee.Filter.inList('class', [int(c) for c in all_classes]).Not()
        )
        lstFeats = lstFeats.merge(classes_not_limited)
        return ee.FeatureCollection(lstFeats)

    def get_ROIs_from_neighbor(self, lst_bacias, asset_root, yyear):
        featGeral = ee.FeatureCollection([])
        for jbasin in lst_bacias:
            nameFeatROIs  = f"{jbasin}_{yyear}_cd"
            dir_asset_rois = os.path.join(asset_root, nameFeatROIs)
            feat_tmp = ee.FeatureCollection(dir_asset_rois)
            feat_tmp = feat_tmp.map(lambda f: f.set('class', ee.Number.parse(f.get('class')).toFloat().toInt8()))
            featGeral = featGeral.merge(feat_tmp)
        return featGeral

    def iterate_bacias(self, _nbacia, myModel, makeProb, process_mosaic_EE, lista_years):

        baciabuffer = ee.FeatureCollection(self.options['asset_bacias_buffer']).filter(
                            ee.Filter.eq('nunivotto4', _nbacia))
        print(f"know about the geometry 'nunivotto4' >>  {_nbacia} loaded < {baciabuffer.size().getInfo()} > geometry")
        baciabuffer = baciabuffer.map(lambda f: f.set('id_codigo', 1))
        bacia_raster = baciabuffer.reduceToImage(['id_codigo'], ee.Reducer.first()).gt(0)
        baciabuffer  = baciabuffer.geometry()

        lstSat = ["l5","l7","l8"]
        imagens_mosaicoEE = (
            ee.ImageCollection(self.options['asset_collectionId'])
                    .select(self.options['bnd_L'])
        )
        imagens_mosaico = (
            ee.ImageCollection(self.options['asset_mosaic'])
                .filter(ee.Filter.inList('biome', self.options['biomas']))
                .filter(ee.Filter.inList('satellite', lstSat))
                .select(self.lstBandMB)
        )

        print("---------------------------------------------------------------")
        pmtroClass = copy.deepcopy(self.options['pmtGTB'])

        path_ptrosFS = os.path.join(self.pathFSJson, f"feat_sel_{_nbacia}.json")
        print("load features json ", path_ptrosFS)
        with open(path_ptrosFS, 'r') as file:
            bandas_fromFS = json.load(file)

        print(f"lista de Bacias Anos no dict de FS  {len(bandas_fromFS.keys())} years  ")
        print(' as primeiras 3 \n ==> ', list(bandas_fromFS.keys())[:3])
        lsNamesBaciasViz = arqParams.basinVizinhasNew[_nbacia]
        lstSoViz = [kk for kk in lsNamesBaciasViz if kk != _nbacia]
        print("lista de Bacias vizinhas", lstSoViz)

        for nyear in self.lst_year[:]:
            bandActiva = 'classification_' + str(nyear)
            print("banda activa: " + bandActiva)

            nomec = f"{_nbacia}_{nyear}_GTB_col11_BND_fm-v_{self.options['version']}"

            if nyear not in lista_years:

                limitlsb      = 15
                bandas_imports = self.lst_feat_select[:limitlsb]
                print(f" numero de bandas selecionadas {len(bandas_imports)} ")
                print(bandas_imports)

                if nyear < 2026:
                    nameFeatROIs = f"rois_fromBasin_{_nbacia}_{nyear}"
                else:
                    nameFeatROIs = f"rois_fromBasin_{_nbacia}_{2024}"

                print("loading Rois with name =>>>>>> ", nameFeatROIs)

                asset_rois     = self.options['asset_joinsGrBa']
                dir_asset_rois = os.path.join(asset_rois, nameFeatROIs)
                print(f"load samples from idAsset >> {dir_asset_rois}")
                ROIs_toTrain = ee.FeatureCollection(dir_asset_rois)
                ROIs_toTrain = self.down_samples_ROIs(ROIs_toTrain, _nbacia)
                print(" saindo do processo downsamples ")

                date_inic = ee.Date.fromYMD(int(nyear), 1, 1)
                date_end  = ee.Date.fromYMD(int(nyear), 12, 31)

                if nyear < 2026:
                    lstCoef      = [0.8425, 0.8957, 0.9097, 0.3188, 0.969, 0.9578]
                    bandsCoef    = ee.Image.constant(lstCoef + lstCoef + lstCoef)
                    lstIntercept = [106.7546, 115.1553, 239.0688, 1496.4408, 392.3453, 366.57]
                    bandsIntercept = ee.Image.constant(lstIntercept + lstIntercept + lstIntercept)

                    colmosaicMapbiomas = (imagens_mosaico.filter(ee.Filter.eq('year', nyear))
                                    .median().updateMask(bacia_raster))
                    imagens_mosaicoEEv = colmosaicMapbiomas.multiply(bandsCoef).add(bandsIntercept)
                    imagens_mosaicoEEv = imagens_mosaicoEEv.divide(10000)

                    mosaicColGoogle = imagens_mosaicoEE.filter(ee.Filter.date(date_inic, date_end))
                    mosaicoBuilded  = self.make_mosaicofromIntervalo(mosaicColGoogle, nyear)
                    mosaicoBuilded  = mosaicoBuilded.updateMask(bacia_raster)
                    maskGaps        = mosaicoBuilded.unmask(-9999).eq(-9999).updateMask(bacia_raster)
                    mosaicoBuilded  = mosaicoBuilded.unmask(-9999).where(maskGaps, imagens_mosaicoEEv)
                    maskGaps        = mosaicoBuilded.neq(-9999)
                    mosaicoBuilded  = mosaicoBuilded.updateMask(maskGaps).updateMask(bacia_raster)
                else:
                    mosaicColGoogle = imagens_mosaicoEE.filter(ee.Filter.date(date_inic, date_end))
                    mosaicoBuilded  = self.make_mosaicofromIntervalo_y25(mosaicColGoogle, nyear, True)

                # Normalização min-max p01/p99 nas bandas brutas (VC4)
                # mosaicoBuilded = self.apply_percentile_scaling(mosaicoBuilded, _nbacia, nyear)
                mosaicoBuilded = mosaicoBuilded.updateMask(bacia_raster)

                print("----- calculado todos os 102 indices ---------------------")
                mosaicProcess = self.agregateBandswithSpectralIndex(mosaicoBuilded)
                mosaicProcess = ee.Image(mosaicProcess)
                print("-A----------------------------------------")
                print("calculou todas as bandas necesarias ")

                gradeExpMemo = [
                   
                ]
                if _nbacia in gradeExpMemo:
                    pmtroClass['numberOfTrees'] = 8
                    pmtroClass['shrinkage']      = 0.1
                else:
                    pmtroClass['shrinkage'] = self.dictHiperPmtTuning[_nbacia]['learning_rate']
                    lstBacias_prob = ['7541', '7544', '7592', '7612', '7615', '7712', '7721', '7741', '7746']
                    if _nbacia in lstBacias_prob:
                        numberTrees = 8
                        if self.dictHiperPmtTuning[_nbacia]["n_estimators"] < numberTrees:
                            pmtroClass['numberOfTrees'] = self.dictHiperPmtTuning[_nbacia]["n_estimators"] - 3
                        else:
                            pmtroClass['numberOfTrees'] = numberTrees

                print("pmtros Classifier ==> ", pmtroClass)

                classifierGTB = ee.Classifier.smileGradientTreeBoost(**pmtroClass).train(
                                                    ROIs_toTrain, 'class', bandas_imports)
                classifiedGTB = mosaicProcess.classify(classifierGTB, bandActiva)

                print("addicionando classification bands = ", bandActiva)
                mydict = {
                    'id_bacia':   _nbacia,
                    'version':    self.options['version'],
                    'biome':      self.options['bioma'],
                    'classifier': 'GTB',
                    'collection': '11.0',
                    'sensor':     'Landsat',
                    'source':     'geodatin',
                    'year':       nyear,
                    'bands':      'fm'
                }
                classifiedGTB = classifiedGTB.set(mydict)
                classifiedGTB = classifiedGTB.set("system:footprint", baciabuffer.coordinates())
                self.processoExportar(classifiedGTB, baciabuffer, nomec)

        else:
            print(f' bacia >>> {nomec}  <<<  foi FEITA ')

    def processoExportar(self, mapaRF, regionB, nameB):
        nomeDesc = 'BACIA_' + str(nameB)
        idasset  = os.path.join(self.options['assetOut'], nomeDesc)
        optExp   = {
            'image':           mapaRF,
            'description':     nomeDesc,
            'assetId':         idasset,
            'region':          ee.Geometry(regionB),
            'scale':           30,
            'maxPixels':       1e13,
            "pyramidingPolicy": {".default": "mode"},
        }
        task = ee.batch.Export.image.toAsset(**optExp)
        task.start()
        print("salvando ... " + nomeDesc + "..!")
        for keys, vals in dict(task.status()).items():
            print("  {} : {}".format(keys, vals))


mosaico = 'mosaico_mapbiomas'
param = {
    'bioma': "CAATINGA",
    'biomas': ["CAATINGA","CERRADO", "MATAATLANTICA"],
    'asset_bacias': "projects/mapbiomas-arida/ALERTAS/auxiliar/bacias_hidrografica_caatinga49div",
    'asset_bacias_buffer': 'projects/ee-solkancengine17/assets/shape/bacias_buffer_caatinga_49_regions',
    'asset_IBGE': 'users/SEEGMapBiomas/bioma_1milhao_uf2015_250mil_IBGE_geo_v4_revisao_pampa_lagoas',
    'assetOut': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1',
    'bnd_L': ['blue','green','red','nir','swir1','swir2'],
    'version': 3,
    'lsBandasMap': [],
    'numeroTask': 6,
    'numeroLimit': 10,
    'conta': {
        '0': 'caatinga01',
        '1': 'caatinga02',
        '2': 'caatinga03',
        '3': 'caatinga04',
        '4': 'caatinga05',
        '5': 'solkan1201',
        '6': 'solkanGeodatin',
        '7': 'superconta'
    },
    'dict_classChangeBa': arqParams.dictClassRepre
}

#============================================================
#========================METODOS=============================
#============================================================

def gerenciador(cont):
    numberofChange = [kk for kk in param['conta'].keys()]
    print(numberofChange)

    if str(cont) in numberofChange:
        print(f"inicialize in account #{cont} <> {param['conta'][str(cont)]}")
        switch_user(param['conta'][str(cont)])
        projAccount = get_project_from_account(param['conta'][str(cont)])
        try:
            ee.Initialize(project=projAccount)
            print('The Earth Engine package initialized successfully!')
        except ee.EEException as e:
            print('The Earth Engine package failed to initialize!')

        tarefas = tasks(n=param['numeroTask'], return_list=True)
        for lin in tarefas:
            print(str(lin))

    elif cont > param['numeroLimit']:
        return 0
    cont += 1
    return cont


def save_ROIs_toAsset(collection, name):
    optExp = {
        'collection': collection,
        'description': name,
        'assetId': param['outAssetROIs'] + "/" + name
    }
    task = ee.batch.Export.table.toAsset(**optExp)
    task.start()
    print("exportando ROIs da bacia $s ...!", name)


def check_dir(file_name):
    if not os.path.exists(file_name):
        arq = open(file_name, 'w+')
        arq.close()


def getPathCSV(nfolder):
    mpath      = os.getcwd()
    pathparent = str(Path(mpath).parents[0])
    roisPath   = '/dados/' + nfolder
    mpath      = pathparent + roisPath
    print("path of CSVs Rois is \n ==>", mpath)
    return mpath


def clean_lstBandas(tmplstBNDs):
    lstFails   = ['green_median_texture']
    lstbndsRed = []
    for bnd in tmplstBNDs:
        bnd = bnd.replace('_1','').replace('_2','').replace('_3','')
        if bnd not in lstbndsRed and 'min' not in bnd and bnd not in lstFails and 'stdDev' not in bnd:
            lstbndsRed.append(bnd)
    return lstbndsRed


tesauroBasin = arqParams.tesauroBasin
pathJson     = getPathCSV("regJSON/")

print("==================================================")

registros_proc = "registros/lsBaciasClassifyfeitasv_3.txt"
pathFolder     = os.getcwd()
path_MGRS      = os.path.join(pathFolder, registros_proc)
baciasFeitas   = []
check_dir(path_MGRS)

arqFeitos = open(path_MGRS, 'r')
for ii in arqFeitos.readlines():
    ii = ii[:-1]
    baciasFeitas.append(ii)
arqFeitos.close()
arqFeitos = open(path_MGRS, 'a+')

nameBacias = [
    '765', '7544', '7541', '7411', '746', '7591', '7592',
    '761111', '761112', '7612', '7613', '7614', '7615',
    '771', '7712', '772', '7721', '773', '7741', '7746',
    '7754', '7761', '7764', '7691', '7581', '7625', '7584',
    '751', '752', '7616', '745', '7424', '7618', '7561',
    '755', '7617', '7564', '7422', '76116', '7671', '757',
    '766', '753', '764', '7619', '7443', '7438', '763',
    '7622'
]

print(f"we have {len(nameBacias)} bacias")
modelo       = "GTB"
knowMapSaved = False
procMosaicEE = True

cont = 7
asset_exportar       = param['assetOut']
process_classification = ClassMosaic_indexs_Spectral()
lst_bacias_saved     = process_classification.lstIDassetS

for _nbacia in nameBacias[:]:
    if knowMapSaved:
        try:
            nameMap = 'BACIA_' + _nbacia + '_' + 'GTB_col11_BND_fm-v' + str(param['version'])
            imgtmp  = ee.Image(os.path.join(asset_exportar, nameMap))
            print(" loading ", nameMap, " ", len(imgtmp.bandNames().getInfo()), " bandas")
        except:
            pass
    else:
        print("-----------------------------------------------------------------")
        print(f"-------------    classificando bacia nova << {_nbacia} >> ---------------")
        print("-----------------------------------------------------------------")
        versao_str   = f'fm-v_{process_classification.options["version"]}'
        lst_temporal = [
            raster_bacia for raster_bacia in lst_bacias_saved
            if f'BACIA_{_nbacia}_' in raster_bacia and versao_str in raster_bacia
        ]
        lst_years = []
        if len(lst_temporal) < 41:
            for ii in lst_temporal:
                print(ii)
                lst_years.append(int(ii.split("_")[2]))
            print(f" ---- {len(lst_years)} years feitos ")
            process_classification.iterate_bacias(_nbacia, modelo, False, procMosaicEE, lst_years)

arqFeitos.close()
