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
    ee.Initialize(project= projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise
# sys.setrecursionlimit(1000000000)

#============================================================
#============== FUNCTIONS FO SPECTRAL INDEX =================


class ClassMosaic_indexs_Spectral(object):

    # default options
    options = {
        'bnd_L': ['blue','green','red','nir','swir1','swir2'],
        'bnd_fraction': ['gv','npv','soil'],
        'biomas': ['CERRADO','CAATINGA','MATAATLANTICA'],
        'bioma': "CAATINGA",
        'version': 1,
        'lsBandasMap': [],
        'asset_bacias_buffer' : 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions',
        'asset_grad': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/basegrade30KMCaatinga',
        'asset_collectionId': 'LANDSAT/COMPOSITES/C02/T1_L2_32DAY',
        'asset_joinsGrBa': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesCCred',
        'assetOut': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1',
        # Spectral bands selected
        'lsClasse': [4, 3, 12, 15, 18, 21, 22, 33],
        'lsPtos': [300, 500, 300, 350, 150, 100, 150, 300],
        'dict_classChangeBa': arqParams.dictClassRepre,
        # https://scikit-learn.org/stable/modules/ensemble.html#gradient-boosting
        'pmtGTB': {
            'numberOfTrees': 35, 
            'shrinkage': 0.1,         
            'samplingRate': 0.65, 
            'loss': "LeastSquares",#'Huber',#'LeastAbsoluteDeviation', 
            'seed': 0
        },
    }

    # TESTE: apenas bandas brutas do mosaico (sem índices espectrais)
    # para isolar se o erro vem do agregateBandswithSpectralIndex ou de outra parte
    lst_feat_select = [
        'blue_median', 'green_median', 'red_median', 'nir_median', 'swir1_median', 'swir2_median',
        'blue_median_wet', 'green_median_wet', 'red_median_wet', 'nir_median_wet', 'swir1_median_wet', 'swir2_median_wet',
        'blue_median_dry', 'green_median_dry', 'red_median_dry', 'nir_median_dry', 'swir1_median_dry', 'swir2_median_dry',
    ]
    # lst_feat_select = [  # lista original com índices espectrais
    #         'ndti_median_dry',  'brba_median_wet', 'ndti_median_wet',
    #         'slope', 'wetness_median',
    #         'awei_median', 'awei_median_wet', 'swir2_median',
    #         'brba_median_dry', 'brightness_median', 'gli_median_dry', 'spri_median_dry',
    #         'spri_median_wet', 'red_median_wet', 'ndti_median', 'awei_median_dry',
    #         'green_median_dry', 'green_median_wet', 'swir1_median_wet',
    #         'pri_median_dry', 'pri_median', 'swir2_median_dry', 'mbi_median_dry',
    #         'shape_median_dry', 'wetness_median_wet', 'brightness_median_dry', 'swir2_median_wet', 'red_median_dry', 'evi_median_dry',
    #         'pri_median_wet', 'evi_median_wet', 'evi_median', 'gcvi_median', 'avi_median', 'bsi_median', 'ui_median', 'ndvi_median', 'ndvi_median_dry', 'ndwi_median_dry',
    #         'ndwi_median', 'ndvi_median_wet', 'ndwi_median_wet',
    # ]
    # lst_properties = arqParam.allFeatures
    # MOSAIC WITH BANDA 2022 
    # https://code.earthengine.google.com/c3a096750d14a6aa5cc060053580b019
    def __init__(self, nbacia, year_act):

        print("==================================================")
        self.iterate_bacias(nbacia, year_act)



    # add bands with slope and hilshade informations 
    def addSlopeAndHilshade(self, img):
        # A digital elevation model.
        # NASADEM: NASA NASADEM Digital Elevation 30m
        dem = ee.Image('NASA/NASADEM_HGT/001').select('elevation')
        # Calculate slope. Units are degrees, range is [0,90).
        slope = ee.Terrain.slope(dem).divide(500).toFloat()
        return img.addBands(slope.rename('slope'))

    def GET_NDFIA(self, IMAGE, sufixo):
        lstBands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']
        lstBandsSuf = [bnd + sufixo for bnd in lstBands]
        lstFractions = ['gv', 'shade', 'npv', 'soil', 'cloud']
        lstFractionsSuf = [frac + sufixo for frac in lstFractions]
        
        endmembers = [            
            [0.05, 0.09, 0.04, 0.61, 0.30, 0.10], # gv
            [0.14, 0.17, 0.22, 0.30, 0.55, 0.30], # npv
            [0.20, 0.30, 0.34, 0.58, 0.60, 0.58], # soil
            [0.0 , 0.0,  0.0 , 0.0 , 0.0 , 0.0 ], # Shade
            [0.90, 0.96, 0.80, 0.78, 0.72, 0.65]  # cloud
        ];

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
        indSMA_median =  self.GET_NDFIA(img, '_median')
        indSMA_med_wet =  self.GET_NDFIA(img, '_median_wet')
        indSMA_med_dry =  self.GET_NDFIA(img, '_median_dry')
        return img.addBands(indSMA_med_dry).addBands(indSMA_median).addBands(indSMA_med_wet)

    def agregateBandsContextoEstrutural(self, img):        
        # REDUZIDO: kernel 5x5 em vez de 7x7
        kernel = ee.Kernel.square(5)
        bandas_textura = [
            'osavi_median', 
            'gcvi_median', 'avi_median', 
            'bsi_median', 
            'ui_median',  #'ndfia_median',
            'awei_median'
        ]
        
        img_base = img.select(bandas_textura)
                # Calcular média e desvio padrão separadamente
        mean_img = img_base.reduceNeighborhood(
            reducer=ee.Reducer.mean(),
            kernel=kernel
        )
        
        std_img = img_base.reduceNeighborhood(
            reducer=ee.Reducer.stdDev(),
            kernel=kernel
        )
        
        # Renomear para evitar conflito
        mean_bands = mean_img.bandNames().map(lambda b: ee.String(b).cat('_mean'))
        std_bands = std_img.bandNames().map(lambda b: ee.String(b).cat('_stdDev'))
        
        mean_img = mean_img.rename(mean_bands)
        std_img = std_img.rename(std_bands)
        
        return img.addBands(mean_img).addBands(std_img)


    # Ratio Vegetation Index # Global Environment Monitoring Index GEMI
    def agregateBandswithSpectralIndex(self, img): # lista_bands
        sufixos = ['_median', '_median_wet', '_median_dry']

        # Puxa o dicionário gigante do seu arquivo de configuração!
        formulas_base = arqParams.FORMULAS_INDICES_ESPECTRAIS
        novas_bandas_base = []

        for s in sufixos:
            print("sufixos ==> ", s)
            for nome_indice, expressao in formulas_base.items():
                nome_banda = f"{nome_indice}{s}"
                # Só monta o nó se a banda for necessária para a classificação.
                if nome_banda in self.lst_feat_select:
                    expr_formatada = expressao.format(s=s)
                    banda_calc = img.expression(f"float({expr_formatada})").rename(nome_banda)
                    novas_bandas_base.append(banda_calc)

        # spri = (pri + 1) / 2 = green / (green + blue) — calculado direto de img
        # sem passar por img_com_base para não criar profundidade no grafo
        for s in sufixos:
            if f"spri{s}" in self.lst_feat_select:
                spri = img.expression(
                    f"float(b('green{s}') / (b('green{s}') + b('blue{s}')))"
                ).rename(f"spri{s}")
                novas_bandas_base.append(spri)

        img_quase_pronta = ee.Image.cat([img] + novas_bandas_base)

        imagem_final = self.addSlopeAndHilshade(img_quase_pronta)
        # imagem_final = self.agregateBandsContextoEstrutural(imagem_final)
        print("-----ADICIONOU TODOS OS INDICES ESPTRAIS AO PASSAR POR AQUI -----------")
        return imagem_final

    
    def make_mosaicofromIntervalo(self, colMosaic, year_courrent, semetral=False):
        
        band_year = [nband + '_median' for nband in self.options['bnd_L']]            
        band_wets = [bnd + '_wet' for bnd in band_year]
        band_drys = [bnd + '_dry' for bnd in band_year]
        
        dictPer = {
            'year': {
                'start': str(str(year_courrent)) + '-01-01',
                'end': str(year_courrent) + '-12-31',
                'surf': 'year',
                'bnds': band_year
            },
            'dry': {
                'start': str(year_courrent) + '-08-01',
                'end': str(year_courrent) + '-12-31',
                'surf': 'dry',
                'bnds': band_drys
            },
            'wet': {
                'start': str(year_courrent) + '-01-01',
                'end': str(year_courrent) + '-07-31',
                'surf': 'wet',
                'bnds': band_wets
            }
        }       
        
        if semetral:
            lstPeriodo = ['year', 'wet']
        else:
            lstPeriodo = ['year', 'dry', 'wet']

        partes = []
        col_base = colMosaic.select(self.options['bnd_L'])
        for periodo in lstPeriodo:
            dateStart = dictPer[periodo]['start']
            dateEnd = dictPer[periodo]['end']
            bands_period = dictPer[periodo]['bnds']
            mosaictmp = (
                col_base
                    .filter(ee.Filter.date(dateStart, dateEnd))
                    .max()
                    .rename(bands_period)
            )
            partes.append(mosaictmp)

        if semetral:
            bands_period = dictPer['dry']['bnds']
            imgUnos = ee.Image.constant([1] * len(band_year)).rename(bands_period)
            partes.append(imgUnos)

        return ee.Image.cat(partes)


    def get_bands_mosaicos (self):
        band_year = [nband + '_median' for nband in self.options['bnd_L']]
        band_drys = [bnd + '_dry' for bnd in band_year]    
        band_wets = [bnd + '_wet' for bnd in band_year]
        return band_year + band_wets + band_drys


    def iterate_bacias(self, _nbacia, nyear):        

        # loading geometry bacim
        baciabuffer = ee.FeatureCollection(self.options['asset_bacias_buffer']).filter(
                            ee.Filter.eq('nunivotto4', _nbacia))
        print(f"know about the geometry 'nunivotto4' >>  {_nbacia} loaded < {baciabuffer.size().getInfo()} > geometry" )   
        baciabuffer = baciabuffer.geometry()
        # sys.exit()

        print("---------------------------------------------------------------")
        pmtroClass = copy.deepcopy(self.options['pmtGTB'])
        print("pmtros Classifier ==> ", pmtroClass)

        # tesauroBasin = arqParams.tesauroBasin
        lsNamesBaciasViz = arqParams.basinVizinhasNew[_nbacia]
        lstSoViz =  [kk for kk in lsNamesBaciasViz if kk != _nbacia]
        print("lista de Bacias vizinhas", lstSoViz)

        
        bandActiva = 'classification_' + str(nyear)       
        print( "banda activa: " + bandActiva)             
        nomec = f"{_nbacia}_{nyear}_GTB_col11_BND_fm-v_{self.options['version']}"
        # print("nome to export ", 'BACIA_' + nomec)


        #cria o classificador com as especificacoes definidas acima 
        limitlsb = 45
        bandas_imports = self.lst_feat_select[:limitlsb]
        print(f" numero de bandas selecionadas {len(bandas_imports)} ") 
        print(bandas_imports)
        
        # sys.exit()
        # --- CARREGAMENTO DE AMOSTRAS (ESPECÍFICO) ---
        # Se o ano for > 2024, usamos as amostras de 2024 como fallback
        ano_amostra = nyear if nyear <= 2024 else 2024
        nameFeatROIs = f"rois_fromBasin_{_nbacia}_{ano_amostra}"             

        print("loading Rois with name =>>>>>> ", nameFeatROIs)
        asset_rois = self.options['asset_joinsGrBa']
        
        # try:
        dir_asset_rois = os.path.join(asset_rois, nameFeatROIs)
        print(f"load samples from idAsset >> {dir_asset_rois}")
        ROIs_toTrain = ee.FeatureCollection(dir_asset_rois) 
        
        # --- CLASSIFICAÇÃO ---
        # Treinar o classificador apenas com os dados necessários
        # ee.Classifier.smileGradientTreeBoost(numberOfTrees, shrinkage, samplingRate, maxNodes, loss, seed)
        # print("antes de classificar ", ROIs_toTrain.first().propertyNames().getInfo())                
        classifierGTB = (ee.Classifier.smileGradientTreeBoost(**pmtroClass)
                                .train(ROIs_toTrain, 'class', bandas_imports))    

        print("************** Classificador treinado ******************")                        
        
        date_inic = ee.Date.fromYMD(int(nyear),1,1)      
        date_end = ee.Date.fromYMD(int(nyear),12,31)                   
        mosaicColGoogle = (ee.ImageCollection(self.options['asset_collectionId'])
                                .filterBounds(baciabuffer)
                                .filter(ee.Filter.date(date_inic, date_end))
                                .select(self.options['bnd_L']) 
                        )        
        mosaicoBuilded = self.make_mosaicofromIntervalo(mosaicColGoogle, nyear) 
        mosaicoBuilded = mosaicoBuilded.clip(baciabuffer)
        
        # print(f" we have {mosaicoBuilded.bandNames().getInfo()} images ")
        
        # TESTE: bypass dos índices espectrais — mosaico bruto direto
        # se o erro desaparecer aqui, o problema está no agregateBandswithSpectralIndex
        # se o erro persistir, o problema está no classifier/ROIs/export
        mosaicProcess = mosaicoBuilded.select(bandas_imports)
        # mosaicProcess = (ee.Image(self.agregateBandswithSpectralIndex(mosaicoBuilded))
        #                     .select(bandas_imports))

        # print("-A----------------------------------------")
        print(f" we have {mosaicProcess.bandNames().getInfo()} images ")
        
        print("calculou todas as bandas necesarias ")
                    
        classifiedGTB = mosaicProcess.classify(classifierGTB, bandActiva)        
        # print("classificando!!!! ")
        
        # se for o primeiro ano cria o dicionario e seta a variavel como
        # o resultado da primeira imagem classificada
        print("addicionando classification bands = " , bandActiva)                                                  
        # nomec = f"{_nbacia}_{nyear}_GTB_col10-v_{self.options['version']}"            
        mydict = {
            'id_bacia': _nbacia,
            'version': self.options['version'],
            'biome': self.options['bioma'],
            'classifier': 'GTB',
            'collection': '11.0',
            'sensor': 'Landsat',
            'source': 'geodatin',  
            'year': nyear, 
            'bands': 'fm'             
        }

        classifiedGTB = classifiedGTB.set(mydict)
        classifiedGTB = classifiedGTB.set("system:footprint", baciabuffer.coordinates())
        # exporta bacia   .coordinates()
        self.processoExportar(classifiedGTB, baciabuffer, nomec)
        sys.exit()
                 

    #exporta a imagem classificada para o asset
    def processoExportar(self, mapaRF, regionB, nameB):
        nomeDesc = 'BACIA_'+ str(nameB)
        idasset =  os.path.join(self.options['assetOut'] , nomeDesc)

        optExp = {
            'image': mapaRF, 
            'description': nomeDesc, 
            'assetId':idasset, 
            'region':ee.Geometry(regionB), #['coordinates'] .getInfo()
            'scale': 30, 
            'maxPixels': 1e13,
            "pyramidingPolicy":{".default": "mode"},
            # 'priority': 1000
        }
        task = ee.batch.Export.image.toAsset(**optExp)
        task.start() 
        print("salvando ... " + nomeDesc + "..!")
        # print(task.status())
        for keys, vals in dict(task.status()).items():
            print ( "  {} : {}".format(keys, vals))



param = {      
    'assetOut': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1',
    'version': 1,
    'lsBandasMap': [],
    'dict_classChangeBa': arqParams.dictClassRepre,
    "anoIntInit": 1985,
    "anoIntFin": 2025,
}
# print(param.keys())
# print("vai exportar em ", param['assetOut'])

#============================================================
#========================METODOS=============================
#============================================================


def check_dir(file_name):
    if not os.path.exists(file_name):
        arq = open(file_name, 'w+')
        arq.close()

def getPathCSV (nfolder):
    # get dir path of script 
    mpath = os.getcwd()
    # get dir folder before to path scripts 
    pathparent = str(Path(mpath).parents[0])
    # folder of CSVs ROIs
    roisPath = '/dados/' + nfolder
    mpath = pathparent + roisPath
    print("path of CSVs Rois is \n ==>",  mpath)
    return mpath



dictPmtroArv = {
    '35': [
            '741', '746', '753', '766', '7741', '778', 
            '7616', '7617', '7618', '7619'
    ],
    '50': [
            '7422', '745', '752', '758', '7621', 
            '776', '777',  '7612', '7615'# 
    ],
    '65':  [
            '7421','744','7492','751',
            '754','755','756','757','759','7622','763','764',
            '765','767','771','772','773', '7742','775',
            '76111','76116','7614','7613'
    ]
}

tesauroBasin = arqParams.tesauroBasin
pathJson = getPathCSV("regJSON/")


print("==================================================")

## Revisando todos as Bacias que foram feitas 
registros_proc = "registros/lsBaciasClassifyfeitasv_1.txt"
pathFolder = os.getcwd()
path_MGRS = os.path.join(pathFolder, registros_proc)
baciasFeitas = []
check_dir(path_MGRS)

arqFeitos = open(path_MGRS, 'r')
for ii in arqFeitos.readlines():    
    ii = ii[:-1]
    # print(" => " + str(ii))
    baciasFeitas.append(ii)

arqFeitos.close()
arqFeitos = open(path_MGRS, 'a+')

# lista de 49 bacias 
nameBacias = [
    '765', '7544', '7541', '7411', '746', '7591', '7592', 
    '761111', '761112', '7612', '7613', '7614', '7615', 
    '771', '7712', '772', '7721', '773', '7741', '7746', '7754', 
    '7761', '7764',   '7691', '7581', '7625', '7584', '751', 
    '752', '7616', '745', '7424', '7618', '7561', '755', '7617', 
    '7564', '7422', '76116', '7671', '757', '766', '753', '764',
    '7619', '7443', '7438', '763', '7622'
]
# nameBacias = [
    # '7422', '7424', '7438', '7443', '745', '746','751','752','753', '7564', '7581',
    # '7617', '7618', '7619', '7622', '7625', '763','765', '766', '7671', '772',
    # '7721', '7741', '7754'
# ]
pathFSJson = getPathCSV("FS_col11_json/")
print("==== path of CSVs of Features Selections ==== \n >>> ", pathFSJson)
# '7617', '7564',  '763', '7622'
print(f"we have {len(nameBacias)} bacias")
# "761112",
modelo = "GTB"
knowMapSaved = False
procMosaicEE = True
lst_year_serie = list(range(param["anoIntInit"], param["anoIntFin"] + 1))
print(f"list of year to process {len(lst_year_serie)}")

listBacFalta = []
asset_exportar = param['assetOut']

lst_bacias_saved = (ee.ImageCollection(asset_exportar)
                        .reduceColumns(ee.Reducer.toList(), ['system:index'])
                        .get('list').getInfo()
                )
print(f" ====== we have {len(lst_bacias_saved)} maps saved ====")   
print(lst_bacias_saved[:2])

for _nbacia in nameBacias[:1]:
    if knowMapSaved:
        try:
            nameMap = 'BACIA_' + _nbacia + '_' + 'GTB_col10-v' + str(param['version'])
            imgtmp = ee.Image(os.path.join(asset_exportar, nameMap))
            print(" 🚨 loading ", nameMap, " ", len(imgtmp.bandNames().getInfo()), " bandas 🚨")
        except:
            listBacFalta.append(_nbacia)
    else:        
        print("---------------------------.kmkl------------------------------")
        print(f"-------------    classificando bacia nova << {_nbacia} >> ---------------")   
        print("-----------------------------------------------------------------")     
        
        lst_temporal = [raster_bacia for raster_bacia in lst_bacias_saved if _nbacia in raster_bacia] 
        lst_years = []
        if len(lst_temporal) < 41:
            for ii in lst_temporal:
                print(ii)  
                lst_years.append(int(ii.split("_")[2]))
            print(f" ---- {len(lst_years)} years feitos ")
        lst_year_process = [nyear for nyear in lst_year_serie if nyear not in lst_years]
        for nyear in lst_year_process:
            process_classification = ClassMosaic_indexs_Spectral(_nbacia, nyear)        #     process_classification.iterate_bacias(_nbacia, modelo, False, procMosaicEE, lst_years) 

    # sys.exit()
arqFeitos.close()

