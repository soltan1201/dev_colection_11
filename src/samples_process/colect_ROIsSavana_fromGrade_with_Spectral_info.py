#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2
@author: geodatin
"""

import ee
import os
import copy
import sys
import pandas as pd
import collections
from pathlib import Path
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
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


class ClassMosaic_indexs_Spectral(object):

    # default options
    options = {
        'bnd_L': ['blue','green','red','nir','swir1','swir2'],
        'bnd_fraction': ['gv','npv','soil'],
        'biomas': ['CERRADO','CAATINGA','MATAATLANTICA'],
        'classMapB': [3, 4, 5, 9, 12, 13, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32, 33, 36, 39, 40, 41, 46, 47, 48, 49, 50, 62, 75],
        'classNew':  [3, 4, 3, 3, 12, 12, 15, 19, 19, 19, 21, 25, 25, 25, 25, 33, 29, 25, 33, 12, 33, 36, 19, 19, 19, 36, 36, 36,  4, 12, 19, 25],
        'asset_bacias_buffer' : 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions',
        'asset_grad': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/basegrade30KMCaatinga',
        'assetMapbiomas100': 'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_integration_v2',
        'asset_collectionId': 'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3',

        'asset_output_grade': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/rois_grades_cerr_caat_embeddin', 

        'asset_embedding': "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL",
        # 'asset_output': 'projects/nexgenmap/SAMPLES/Caatinga',
        # Spectral bands selected
        'lsClasse': [  4,   3,  12,  15,  19,  21,  25,  29, 33,   36],
        'lsPtos':   [300, 500, 300, 350, 150, 100, 150, 100, 200, 150],
        "anoIntInit": 1985,
        "anoIntFin": 2025,
    }

    featureBands = [
        'blue_median', 'blue_median_wet', 'blue_median_dry', 'blue_stdDev', 
        'green_median', 'green_median_dry', 'green_median_wet', 
        'green_median_texture', 'green_min', 'green_stdDev', 
        'red_median', 'red_median_dry', 'red_min', 'red_median_wet', 
        'red_stdDev', 'nir_median', 'nir_median_dry', 'nir_median_wet', 
        'nir_stdDev', 'red_edge_1_median', 'red_edge_1_median_dry', 
        'red_edge_1_median_wet', 'red_edge_1_stdDev', 'red_edge_2_median', 
        'red_edge_2_median_dry', 'red_edge_2_median_wet', 'red_edge_2_stdDev', 
        'red_edge_3_median', 'red_edge_3_median_dry', 'red_edge_3_median_wet', 
        'red_edge_3_stdDev', 'red_edge_4_median', 'red_edge_4_median_dry', 
        'red_edge_4_median_wet', 'red_edge_4_stdDev', 'swir1_median', 
        'swir1_median_dry', 'swir1_median_wet', 'swir1_stdDev', 'swir2_median', 
        'swir2_median_wet', 'swir2_median_dry', 'swir2_stdDev'
    ]
    features_extras = [
        'blue_stdDev','green_median_texture', 'green_min', 'green_stdDev',
        'red_min', 'red_stdDev','red_edge_1_median', 'red_edge_1_median_dry', 
        'red_edge_1_median_wet', 'red_edge_1_stdDev', 'red_edge_2_median', 
        'red_edge_2_median_dry', 'red_edge_2_median_wet', 'red_edge_2_stdDev', 
        'red_edge_3_median', 'red_edge_3_median_dry', 'red_edge_3_median_wet', 
        'red_edge_3_stdDev', 'red_edge_4_median', 'red_edge_4_median_dry', 
        'red_edge_4_median_wet', 'red_edge_4_stdDev','swir1_stdDev',  'swir2_stdDev'
    ]

    allbands = [
        'soil_median_dry', 'ratio_median_dry', 'gli_median_wet', 'pri_median_dry', 'osavi_median', 'npv_median_wet', 'gcvi_median_mean', 'shade_median', 'shape_median', 'soil_median', 'ndfia_median_dry', 'mbi_median_dry', 'bsi_median_stdDev', 'nbr_median', 'wetness_median_dry', 'ndfia_median_mean', 'ndfia_median', 'iia_median_wet', 'brba_median_dry', 'cloud_median', 'nir_median', 'lswi_median_wet', 'ndvi_median', 'slope', 'rvi_median', 'gcvi_median_dry', 'shape_median_dry', 'cvi_median_dry', 'blue_median_dry', 'mbi_median', 'nddi_median', 'avi_median_mean', 'ndfia_median_stdDev', 'gcvi_median_stdDev', 'swir2_median_wet', 'ui_median_wet', 'red_median_wet', 'avi_median', 'co2flux_median_wet', 'shade_median_wet', 'red_median_dry', 'gemi_median', 'osavi_median_dry', 'awei_median_mean', 'brba_median', 'cloud_median_wet', 'bsi_median_mean', 'nbr_median_dry', 'ratio_median', 'gli_median_dry', 'wetness_median', 'green_median_wet', 'brightness_median_wet', 'ndmi_median_dry', 'blue_median', 'msi_median_dry', 'evi_median', 'lswi_median_dry', 'ndti_median_dry', 'ui_median_stdDev', 'blue_median_wet', 'lai_median', 'lai_median_wet', 'cvi_median', 'spri_median_wet', 'shape_median_wet', 'dswi5_median_wet', 'afvi_median', 'ndwi_median', 'avi_median_wet', 'gli_median', 'evi_median_wet', 'nir_median_dry', 'gvmi_median', 'cvi_median_wet', 'ndvi_median_dry', 'ui_median_mean', 'iia_median', 'ndwi_median_dry', 'co2flux_median', 'system:index', 'msi_median_wet', 'osavi_median_wet', 'green_median_dry', 'pri_median', 'ui_median_dry', 'ndbi_median_wet', 'nbr_median_wet', 'nddi_median_wet', 'osavi_median_mean', 'swir1_median_wet', 'bsi_median', 'hillshade', 'swir1_median', 'swir2_median', 'gvmi_median_dry', 'rvi_median_dry', 'ndti_median', 'red_median', 'gemi_median_wet', 'lswi_median', 'brightness_median_dry', 'awei_median_wet', 'gndvi_median_wet', 'gv_median_dry', 'osavi_median_stdDev', 'ndti_median_wet', 'ndvi_median_wet', 'afvi_median_wet', 'spri_median', 'avi_median_stdDev', 'ndmi_median', 'nir_median_wet', 'evi_median_dry', 'bsi_median_dry', 'ndbi_median', 'ndwi_median_wet', 'ratio_median_wet', 'soil_median_wet', 'gcvi_median', 'ui_median', 'rvi_median_wet', 'nddi_median_dry', 'msi_median', 'npv_median', 'swir1_median_dry', 'pri_median_wet', 'ndbi_median_dry', 'avi_median_dry', 'gvmi_median_wet', 'wetness_median_wet', 'dswi5_median', 'spri_median_dry', 'awei_median', 'gndvi_median', 'lai_median_dry', 'gv_median', 'ndfia_median_wet', 'mbi_median_wet', 'gndvi_median_dry', 'brba_median_wet', 'gv_median_wet', 'ndmi_median_wet', 'npv_median_dry', 'awei_median_dry', 'gemi_median_dry', 'bsi_median_wet', 'cloud_median_dry', 'afvi_median_dry', 'gcvi_median_wet', 'iia_median_dry', 'brightness_median', 'awei_median_stdDev', 'green_median', 'co2flux_median_dry', 'swir2_median_dry'
    ]

    # lst_properties = arqParam.allFeatures
    # MOSAIC WITH BANDA 2022 
    # https://code.earthengine.google.com/c3a096750d14a6aa5cc060053580b019
    def __init__(self):
        
        self.imgMosaic = (
            ee.ImageCollection(self.options['asset_collectionId'])                            
                            .select(self.options['bnd_L'])
        )
        # print(simgMosaic.first().getInfo())
        # self.imgMosaic = simgMosaic#.map(lambda img: self.process_re_escalar_img(img))
                                      
        print("  ", self.imgMosaic.size().getInfo())
        print("see band Names the first ")
        # print(" ==== ", ee.Image(self.imgMosaic.first()).bandNames().getInfo())
        print("==================================================")
        # sys.exit()
        self.lst_year = [k for k in range(self.options['anoIntInit'], self.options['anoIntFin'] + 1)]
        print("lista de anos ", self.lst_year)
        
        # @collection90: mapas de uso e cobertura Mapbiomas ==> para extrair as areas estaveis
        self.imgMapbiomas = ee.Image(self.options['assetMapbiomas100'])

    # def process_re_escalar_img (self, imgA):
    #     imgMosaic = imgA.select('blue_median').gte(0).rename('constant');
    #     imgEscalada = imgA.divide(10000).toFloat();
    #     return imgMosaic.addBands(imgEscalada).select(self.featureBands).set('year', imgA.get('year'))

    # add bands with slope and hilshade informations 
    def addSlopeAndHilshade(self, img):
        # A digital elevation model.
        # NASADEM: NASA NASADEM Digital Elevation 30m
        dem = ee.Image('NASA/NASADEM_HGT/001').select('elevation')

        # Calculate slope. Units are degrees, range is [0,90).
        slope = ee.Terrain.slope(dem).divide(500).toFloat()

        # Use the ee.Terrain.products function to calculate slope, aspect, and
        # hillshade simultaneously. The output bands are appended to the input image.
        # Hillshade is calculated based on illumination azimuth=270, elevation=45.
        terrain = ee.Terrain.products(dem)
        hillshade = terrain.select('hillshade').divide(500).toFloat()

        return img.addBands(slope.rename('slope')).addBands(hillshade.rename('hillshade'))


    
    def GET_NDFIA(self, IMAGE, sufixo):
            
        lstBands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2']
        lstBandsSuf = [bnd + sufixo for bnd in lstBands]
        lstFractions = ['gv', 'shade', 'npv', 'soil', 'cloud']
        lstFractionsSuf = [frac + sufixo for frac in lstFractions]
        
        endmembers = [            
            [0.05, 0.09, 0.04, 0.61, 0.30, 0.10], #/*gv*/
            [0.14, 0.17, 0.22, 0.30, 0.55, 0.30], #/*npv*/
            [0.20, 0.30, 0.34, 0.58, 0.60, 0.58], #/*soil*/
            [0.0 , 0.0,  0.0 , 0.0 , 0.0 , 0.0 ], #/*Shade*/
            [0.90, 0.96, 0.80, 0.78, 0.72, 0.65]  #/*cloud*/
        ];

        fractions = (ee.Image(IMAGE).select(lstBandsSuf)
                                .unmix(endmembers= endmembers, sumToOne= True, nonNegative= True)
                                .float())
        fractions = fractions.rename(lstFractions)
        # // print(UNMIXED_IMAGE);
        # GVshade = GV /(1 - SHADE)
        # NDFIa = (GVshade - SOIL) / (GVshade + )
        NDFI_ADJUSTED = fractions.expression(
                                "float(((b('gv') / (1 - b('shade'))) - b('soil')) / ((b('gv') / (1 - b('shade'))) + b('npv') + b('soil')))"
                                ).rename('ndfia' + sufixo)

        NDFI_ADJUSTED = NDFI_ADJUSTED.toFloat()
        fractions = fractions.rename(lstFractionsSuf)
        RESULT_IMAGE = (fractions.toFloat()
                            .addBands(NDFI_ADJUSTED))

        return ee.Image(RESULT_IMAGE).toFloat()

    def agregate_Bands_SMA_NDFIa(self, img):
        
        indSMA_median =  self.GET_NDFIA(img, '_median')
        indSMA_med_wet =  self.GET_NDFIA(img, '_median_wet')
        indSMA_med_dry =  self.GET_NDFIA(img, '_median_dry')

        return img.addBands(indSMA_median).addBands(indSMA_med_wet).addBands(indSMA_med_dry)


    #endregion


 

    def agregateBandsContextoEstrutural(self, img):        
        """
        Calcula métricas de vizinhança (Média e Desvio Padrão) para ensinar ao modelo 
        o contexto espacial da paisagem (ex: separar savana esparsa de campestre).
        """
        # Kernel quadrado de raio 7 (Janela de 15x15 pixels = ~450x450m no Landsat)
        # Escala ideal para capturar aglomerados de árvores e fragmentação na Caatinga
        kernel = ee.Kernel.square(7)
        
        # Selecionamos apenas as versões anuais (_median) dos índices
        bandas_textura = [
            # Vegetação (Diferenciação de densidade de biomassa e dossel)
            'osavi_median', 
            'gcvi_median', 
            'avi_median', 
            
            # Solo Exposto / Degradação / Urbano (Captura o fundo/background da savana)
            'ndfia_median', 
            'bsi_median'
        ]
        
        # Filtra a imagem apenas com as bandas selecionadas para otimizar o cálculo
        img_base = img.select(bandas_textura)
        
        # Cria um redutor duplo: Calcula a Média e o Desvio Padrão simultaneamente
        # - Média: entende a "densidade" da região (ex: quantidade geral de verde ao redor)
        # - Desvio Padrão: entende a "heterogeneidade" (ex: árvores isoladas vs. pasto liso)
        redutor_combo = ee.Reducer.mean().combine(
            reducer2=ee.Reducer.stdDev(),
            sharedInputs=True
        )
        
        # Aplica o redutor na vizinhança usando álgebra de mapas nativa (rápido e vetorizado)
        contexto_espacial = img_base.reduceNeighborhood(
            reducer=redutor_combo,
            kernel=kernel
        )

        # O Earth Engine nomeará as bandas de saída automaticamente adicionando os sufixos.
        # Você terá 14 novas bandas no total. Exemplo: 
        # 'msavi_median_mean', 'msavi_median_stdDev', 'ndfia_mean', 'ndfia_stdDev', etc.
        
        return img.addBands(contexto_espacial)

    def CalculateIndice_otimizado(self, img):
        # 1. Sufixos para automatizar o cálculo das 3 estações (Anual, Wet, Dry)
        sufixos = ['_median', '_median_wet', '_median_dry']

        # 2. Dicionário Mestre de Expressões Base
        # O marcador {s} será substituído automaticamente pelo sufixo da estação
        formulas_base = {
            # Simple Ratio / Ratio Vegetation Index (Jordan, 1969 / Pearson & Miller, 1972)
            'ratio': "b('nir{s}') / b('red{s}')",
            
            # Inverse Ratio Vegetation Index (Richardson & Wiegand, 1977)
            'rvi': "b('red{s}') / b('nir{s}')",


            # Enhanced Vegetation Index de 2 Bandas (EVI2 adaptado - Jiang et al., 2008)
            'evi': "2.4 * (b('nir{s}') - b('red{s}')) / (1 + b('nir{s}') + b('red{s}'))",
            
            # Green Chlorophyll Vegetation Index (Gitelson et al., 1998)
            'gcvi': "(b('nir{s}') / b('green{s}')) - 1",
            
            # Global Environment Monitoring Index (Pinty & Verstraete, 1992)
            'gemi': "(2 * (b('nir{s}') * b('nir{s}') - b('red{s}') * b('red{s}')) + 1.5 * b('nir{s}') + 0.5 * b('red{s}')) / (b('nir{s}') + b('green{s}') + 0.5)",
            
            # Chlorophyll Vegetation Index (Vincini et al., 2008)
            'cvi': "b('nir{s}') * (b('green{s}') / (b('blue{s}') * b('blue{s}')))",
            
            # Green Leaf Index (Louhaichi et al., 2001)
            'gli': "(2 * b('green{s}') - b('red{s}') - b('blue{s}')) / (2 * b('green{s}') + b('red{s}') + b('blue{s}'))",
            
            # Advanced Vegetation Index (Rikimaru et al., 2002)
            'avi': "(b('nir{s}') * (1.0 - b('red{s}')) * (b('nir{s}') - b('red{s}'))) ** 0.3333", 
            
            # Bare Soil Index (Rikimaru et al., 2002)
            'bsi': "((b('swir1{s}') - b('red{s}')) - (b('nir{s}') + b('blue{s}'))) / ((b('swir1{s}') + b('red{s}')) + (b('nir{s}') + b('blue{s}')))",
            
            # Optimized Soil-Adjusted Vegetation Index (Rondeaux et al., 1996)
            'osavi': "(b('nir{s}') - b('red{s}')) / (0.16 + b('nir{s}') + b('red{s}'))",

            
            # Global Vegetation Moisture Index (Ceccato et al., 2002)
            'gvmi': "((b('nir{s}') + 0.1) - (b('swir1{s}') + 0.02)) / ((b('nir{s}') + 0.1) + (b('swir1{s}') + 0.02))",
            
            # Pseudo-Photochemical Reflectance Index (Adaptado de Gamon et al., 1992 para satélites de banda larga)
            'pri': "(b('green{s}') - b('blue{s}')) / (b('green{s}') + b('blue{s}'))",
            
            # Normalized Burn Ratio (Key & Benson, 1999)
            'nbr': "(b('nir{s}') - b('swir1{s}')) / (b('nir{s}') + b('swir1{s}'))",

        }

        novas_bandas_base = []

        # 3. Executa o loop achatando o processamento
        for s in sufixos:
            for nome_indice, expressao in formulas_base.items():
                expr_formatada = expressao.format(s=s)
                nome_banda = f"{nome_indice}{s}"
                
                # Gera a banda isolada e joga na lista (sem fazer addBands na imagem inteira ainda)
                banda_calc = img.expression(f"float({expr_formatada})").rename(nome_banda)
                novas_bandas_base.append(banda_calc)

        # Junta a imagem original com as novas bandas de uma vez só (Flattening)
        img_com_base = ee.Image.cat([img] + novas_bandas_base)

        # 4. Índices Dependentes (que usam índices calculados acima)
        bandas_dependentes = []
        for s in sufixos:
            # LAI depende de EVI
            lai = img_com_base.expression(f"float(3.618 * (b('evi{s}') - 0.118))").rename(f"lai{s}")
            # NDDI depende de NDVI e NDWI
            nddi = img_com_base.expression(f"float((b('ndvi{s}') - b('ndwi{s}')) / (b('ndvi{s}') + b('ndwi{s}')))").rename(f"nddi{s}")
            # SPRI depende de PRI
            spri = img_com_base.expression(f"float((b('pri{s}') + 1) / 2)").rename(f"spri{s}")
            bandas_dependentes.extend([lai, nddi, spri])

        img_quase_pronta = ee.Image.cat([img_com_base] + bandas_dependentes)

        # Fluxo de CO2 depende de SPRI (que acabou de ser gerado)
        bandas_co2 = []
        for s in sufixos:
            co2 = img_quase_pronta.expression(f"float(b('ndvi{s}') * b('spri{s}'))").rename(f"co2flux{s}")
            bandas_co2.append(co2)

        # 5. Concatena tudo e aplica as funções complexas finais
        imagem_final = ee.Image.cat([img_quase_pronta] + bandas_co2)

        # Adiciona Textura, Relevo e NDFIa (mantidos como métodos separados devido à complexidade)
        # imagem_final = self.agregateBandsTexturasGLCM(imagem_final)
        imagem_final = self.addSlopeAndHilshade(imagem_final)
        imagem_final = self.agregate_Bands_SMA_NDFIa(imagem_final)
        imagem_final = self.agregateBandsContextoEstrutural(imagem_final)

        return imagem_final

    def make_mosaicofromReducer(self, colMosaic):
        band_year = [nband + '_median' for nband in self.options['bnd_L']]
        band_drys = [bnd + '_dry' for bnd in band_year]    
        band_wets = [bnd + '_wet' for bnd in band_year]
        # self.bandMosaic = band_year + band_wets + band_drys
        # print("bandas principais \n ==> ", self.bandMosaic)
        # bandsDry =None
        percentilelowDry = 5
        percentileDry = 35
        percentileWet = 65

        # get dry season collection
        evilowDry = (
            colMosaic.select(['evi'])
                    .reduce(ee.Reducer.percentile([percentilelowDry]))
        )
        eviDry = (
            colMosaic.select(['evi'])
                    .reduce(ee.Reducer.percentile([percentileDry]))
        )        

        collectionDry = (
            colMosaic.map(lambda img: img.mask(img.select(['evi']).gte(evilowDry))
                                        .mask(img.select(['evi']).lte(eviDry)))
        )

        # get wet season collection
        eviWet = (
            colMosaic.select(['evi'])        
                    .reduce(ee.Reducer.percentile([percentileWet]))
        )
        collectionWet = (
            colMosaic.map(lambda img: img.mask(img.select(['evi']).gte(eviWet)))                                        
        )

        # Reduce collection to median mosaic
        mosaic = (
            colMosaic.select(self.options['bnd_L'])
                .reduce(ee.Reducer.median()).rename(band_year)
        )

        # get dry median mosaic
        mosaicDry = (
            collectionDry.select(self.options['bnd_L'])
                .reduce(ee.Reducer.median()).rename(band_drys)
        )

        # get wet median mosaic
        mosaicWet = (
            collectionWet.select(self.options['bnd_L'])
                .reduce(ee.Reducer.median()).rename(band_wets)
        )

        # get stdDev mosaic
        mosaicStdDev = (
            colMosaic.select(self.options['bnd_L'])
                        .reduce(ee.Reducer.stdDev())
        )

        mosaic = (mosaic.addBands(mosaicDry)
                        .addBands(mosaicWet)
                        .addBands(mosaicStdDev)
        )

        return mosaic

    def iterate_by_grid(self, idGrade, askSize, oneGrade):        

        # loading geometry bacim    
        oneGrade = ee.Geometry(oneGrade)
        list_of_fc_years = []

        mosaico_embedding = (ee.ImageCollection(self.options['asset_embedding'])
                                .filterBounds(oneGrade))

        for nyear in self.lst_year[:]:
            bandYear = 'classification_' + str(nyear)
            print(f" processing grid_year => {idGrade} <> {bandYear} ")     
            date_inic = ee.Date.fromYMD(nyear, 1, 1)

            imgColfiltered = (
                self.imgMosaic.filter(ee.Filter.eq('year', nyear))
                        .filterBounds(oneGrade)
            )

            print("----- calculado todos os old(102) now 123 indices ---------------------")
            img_recMosaicnewB = self.CalculateIndice_otimizado(imgColfiltered)
            bndAdd = img_recMosaicnewB.bandNames().getInfo()            
            print(f"know bands names Index {len(bndAdd)}")
            print("  ", bndAdd)
            
            embedding_years = mosaico_embedding.filterDate(date_inic, date_inic.advance(1, 'year'))
            img_recMosaicnewB = img_recMosaicnewB.addBands(embedding_years.mosaic())

            # sys.exit()
            nameBandYear = bandYear

            # shpAllFeat = ee.FeatureCollection([]) 

            layerCC = self.imgMapbiomas.select(nameBandYear).eq(4).rename('class')   # .clip(oneGrade)                      
            # print("numero de ptos controle ", roisAct.size().getInfo())
            # opcoes para o sorteio estratificadoBuffBacia
            # sample(region, scale, projection, factor, numPixels, seed, dropNulls, tileScale, geometries)
            ptosTemp = (
                img_recMosaicnewB.addBands(layerCC)
                .select(self.allbands) # Filtra a imagem brutalmente!
                .addBands(ee.Image.constant(nyear).rename('year'))
                .addBands(ee.Image.constant(idGrade).rename('GRID_ID'))
                .sample(
                    region=  oneGrade,  
                    scale= 30,   
                    numPixels= 3000,
                    dropNulls= True,
                    geometries= True,
                    tileScale= 16
                )
            )

            list_of_fc_years.append(ptosTemp)
                # sys.exit()
            # print(f"======  coleted rois from class {self.options['lsClasse']}  =======")
            # sys.exit()
        name_exp = 'rois_grade_' + str(idGrade) #  + "_" + str(nyear)# + "_cc_" + str(nclass)    

        # Achatando a lista em uma única FeatureCollection otimizada
        if list_of_fc_years:
            shpAllFeat = ee.FeatureCollection(list_of_fc_years).flatten()

            self.save_ROIs_toAsset(shpAllFeat, name_exp)
        else:
            print("Nenhuma amostra processada para esta bacia.")
                
    
    # salva ftcol para um assetindexIni
    # lstKeysFolder = ['cROIsN2manualNN', 'cROIsN2clusterNN'] 
    def save_ROIs_toAsset(self, collection, name):
        optExp = {
            'collection': collection,
            'description': name,
            'assetId': self.options['asset_output_grade'] + "/" + name
        }
        task = ee.batch.Export.table.toAsset(**optExp)
        task.start()
        print("exportando ROIs da bacia $s ...!", name)




def ask_byGrid_saved(dict_asset):
    getlstFeat = ee.data.getList(dict_asset)
    lst_temporalAsset = []
    assetbase = "projects/earthengine-legacy/assets/" + dict_asset['id']
    for idAsset in getlstFeat[:]:         
        path_ = idAsset.get('id')        
        name_feat = path_.replace( assetbase + '/', '')
        print("reading <==> " + name_feat)
        idGrade = name_feat.split('_')[2]
        # name_exp = 'rois_grade_' + str(idGrade) + "_" + str(nyear)
        if int(idGrade) not in lst_temporalAsset:
            lst_temporalAsset.append(int(idGrade))
    
    return lst_temporalAsset

asset_grid = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/basegrade30KMCaatinga'
shp_grid = ee.FeatureCollection(asset_grid)

# lstIds = shp_grid.reduceColumns(ee.Reducer.toList(), ['indice']).get('list').getInfo()
# print("   ", lstIds)



reprocessar = False
if reprocessar:
    df = pd.read_csv('lista_gride_with_failsYearSaved.csv')
    lstIdCode = df['idGrid'].tolist()
    print(f"we reprocessing {len(lstIdCode)} gride that fails to samples \n", lstIdCode)

# sys.exit()
param = {
    'anoInicial': 1985,
    'anoFinal': 2024,
    'changeCount': True,
    'numeroTask': 6,
    'numeroLimit': 110,
    'conta': {
        '0': 'caatinga01',
        '10': 'caatinga02',
        '20': 'caatinga03',
        '30': 'caatinga04',
        '40': 'caatinga05',
        '50': 'solkan1201',
        # '120': 'diegoGmail',
        '60': 'solkanGeodatin',
        '70': 'superconta'
    },
}
def gerenciador(cont):    
    #=====================================
    # gerenciador de contas para controlar 
    # processos task no gee   
    #=====================================
    numberofChange = [kk for kk in param['conta'].keys()]    
    print(numberofChange)
    
    if str(cont) in numberofChange:
        print(f"inicialize in account #{cont} <> {param['conta'][str(cont)]}")
        switch_user(param['conta'][str(cont)])
        projAccount = get_project_from_account(param['conta'][str(cont)])
        try:
            ee.Initialize(project= projAccount) # project='ee-cartassol'
            print('The Earth Engine package initialized successfully!')
        except ee.EEException as e:
            print('The Earth Engine package failed to initialize!') 
        
        # relatorios.write("Conta de: " + param['conta'][str(cont)] + '\n')

        tarefas = tasks(
            n= param['numeroTask'],
            return_list= True)
        
        # for lin in tarefas:   
        #     print(str(lin))         
            # relatorios.write(str(lin) + '\n')
    
    elif cont > param['numeroLimit']:
        return 0
    cont += 1    
    return cont

askingbySizeFC = False
searchFeatSaved = False
cont = 70
if param['changeCount']:
    cont = gerenciador(cont)


# asset_grades_area de pesquisa
asset_pesquisa = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/grades_area_pesquisa_caatinga_cerrado'
# asset_grades_cerrado coletas savana
asset_coleta_cerrado = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/grades_area_coleta_cerrado'
# asset_grades_caatinga coletas savanas 
asset_coleta_caatinga = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/grades_area_coleta_caatinga'

grades_coleta = ee.FeatureCollection(asset_coleta_cerrado).merge(ee.FeatureCollection(asset_coleta_caatinga))
lstIdCode = grades_coleta.reduceColumns(ee.Reducer.toList(), ['indice']).get('list').getInfo()

objetoMosaic_exportROI = ClassMosaic_indexs_Spectral()
print("saida ==> ", objetoMosaic_exportROI.options['asset_output_grade'])
# sys.exit()
if searchFeatSaved: 
    lstFeatAsset = ask_byGrid_saved({'id': objetoMosaic_exportROI.options['asset_output_grade']})
    print("   lista de feat ", lstFeatAsset[:5] )
    print("  == size ", len(lstFeatAsset))
    askingbySizeFC = False
else:
    lstFeatAsset = []
print("size of grade geral >> ", len(lstIdCode))
# sys.exit()
inicP = 0 # 0, 100
endP = 100   # 100, 200, 300, 600
for cc, item in enumerate(lstIdCode[inicP:endP]):
    print(f"# {cc + 1 + inicP} loading geometry grade {item}")   
    if item not in lstFeatAsset:
        grade_geom = grades_coleta.filter(ee.Filter.eq('indice', item))
        size = grade_geom.size().getInfo()
        if size > 0:
            objetoMosaic_exportROI.iterate_by_grid(item, askingbySizeFC, grade_geom.geometry() )
        # cont = gerenciador(cont)
    # sys.exit()

