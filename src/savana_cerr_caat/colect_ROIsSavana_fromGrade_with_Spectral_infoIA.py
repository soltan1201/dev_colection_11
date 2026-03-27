#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2
@author: geodatin
"""

import ee
import os
import sys
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
    print('✅ The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('❌ The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise


class ClassMosaic_indexs_Spectral(object):

    # default options
    options = {
        'bnd_L': ['blue','green','red','nir','swir1','swir2'],
        'bnd_fraction': ['gv','npv','soil'],
        'biomas': ['CERRADO','CAATINGA','MATAATLANTICA'],
        'assetMapbiomas100': 'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_integration_v2',
        'asset_collectionId': 'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3',
        'asset_mosaic_sentinelp2': 'projects/nexgenmap/MapBiomas2/SENTINEL/mosaics-3',
        'asset_mosaic_sentinelp1': 'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3',
        'asset_output_grade': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/rois_grades_cerr_caat_embeddin', 
        'asset_embedding': "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL",
        
        'lsClasse': [  4,   3,  12,  15,  19,  21,  25,  29, 33,   36],
        'lsPtos':   [300, 500, 300, 350, 150, 100, 150, 100, 200, 150],
        "anoIntInit": 2021, # ATUALIZADO: 2022
        "anoIntFin": 2024,  # ATUALIZADO: 2025
    }

    allbands = [
        'ratio_median_dry', 'gli_median_wet', 'pri_median_dry', 'osavi_median', 
        'gcvi_median_mean', 'bsi_median_stdDev', 'nbr_median', 'rvi_median', 
        'gcvi_median_dry', 'cvi_median_dry', 'avi_median_mean', 'gcvi_median_stdDev', 
        'avi_median', 'gemi_median', 'osavi_median_dry', 'bsi_median_mean', 
        'nbr_median_dry', 'ratio_median', 'gli_median_dry', 'evi_median', 
        'cvi_median', 'avi_median_wet', 'gli_median', 'evi_median_wet', 
        'gvmi_median', 'cvi_median_wet', 'osavi_median_wet', 'pri_median', 
        'nbr_median_wet', 'osavi_median_mean', 'bsi_median', 'gvmi_median_dry', 
        'rvi_median_dry', 'gemi_median_wet', 'osavi_median_stdDev', 'avi_median_stdDev', 
        'evi_median_dry', 'bsi_median_dry', 'ratio_median_wet', 'gcvi_median', 
        'rvi_median_wet', 'pri_median_wet', 'avi_median_dry', 'gvmi_median_wet', 
        'gemi_median_dry', 'bsi_median_wet', 'gcvi_median_wet'
    ] # system:index removido propositalmente para evitar quebras

    

    def __init__(self):
        
        # Carrega a coleção do Sentinel, mas NÃO seleciona as bandas aqui.
        # Assim mantemos nir_median, nir_median_wet, dry originais intactas.
        self.imgMosaic = ee.ImageCollection(self.options['asset_mosaic_sentinelp1']).merge(
                                    ee.ImageCollection(self.options['asset_mosaic_sentinelp2']))
                                      
        print("==================================================")
        self.lst_year = [k for k in range(self.options['anoIntInit'], self.options['anoIntFin'] + 1)]
        print("lista de anos ", self.lst_year)
        
        self.imgMapbiomas = ee.Image(self.options['assetMapbiomas100'])

        self.mosaico_embedding = ee.ImageCollection(self.options['asset_embedding'])

    # ... [MANTENHA OS SEUS MÉTODOS addSlopeAndHilshade, GET_NDFIA, agregate_Bands_SMA_NDFIa AQUI] ...
    
    def addSlopeAndHilshade(self, img):
        dem = ee.Image('NASA/NASADEM_HGT/001').select('elevation')
        slope = ee.Terrain.slope(dem).divide(500).toFloat()
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


    def agregateBandsContextoEstrutural(self, img):        
        kernel = ee.Kernel.square(7)
        bandas_textura = ['osavi_median', 'gcvi_median', 'avi_median', 'ndfia_median', 'bsi_median']
        img_base = img.select(bandas_textura)
        redutor_combo = ee.Reducer.mean().combine(reducer2= ee.Reducer.stdDev(), sharedInputs=True)
        contexto_espacial = img_base.reduceNeighborhood(reducer=redutor_combo, kernel=kernel)
        return img.addBands(contexto_espacial)

    def CalculateIndice_otimizado(self, img):
        sufixos = ['_median', '_median_wet', '_median_dry']
        formulas_base = {
            'ratio': "b('nir{s}') / b('red{s}')",
            'rvi': "b('red{s}') / b('nir{s}')",
            'evi': "2.4 * (b('nir{s}') - b('red{s}')) / (1 + b('nir{s}') + b('red{s}'))",
            'gcvi': "(b('nir{s}') / b('green{s}')) - 1",
            'gemi': "(2 * (b('nir{s}') * b('nir{s}') - b('red{s}') * b('red{s}')) + 1.5 * b('nir{s}') + 0.5 * b('red{s}')) / (b('nir{s}') + b('green{s}') + 0.5)",
            'cvi': "b('nir{s}') * (b('green{s}') / (b('blue{s}') * b('blue{s}')))",
            'gli': "(2 * b('green{s}') - b('red{s}') - b('blue{s}')) / (2 * b('green{s}') + b('red{s}') + b('blue{s}'))",
            'avi': "(b('nir{s}') * (1.0 - b('red{s}')) * (b('nir{s}') - b('red{s}'))) ** 0.3333", 
            'bsi': "((b('swir1{s}') - b('red{s}')) - (b('nir{s}') + b('blue{s}'))) / ((b('swir1{s}') + b('red{s}')) + (b('nir{s}') + b('blue{s}')))",
            'osavi': "(b('nir{s}') - b('red{s}')) / (0.16 + b('nir{s}') + b('red{s}'))",
            'gvmi': "((b('nir{s}') + 0.1) - (b('swir1{s}') + 0.02)) / ((b('nir{s}') + 0.1) + (b('swir1{s}') + 0.02))",
            'pri': "(b('green{s}') - b('blue{s}')) / (b('green{s}') + b('blue{s}'))",
            'nbr': "(b('nir{s}') - b('swir1{s}')) / (b('nir{s}') + b('swir1{s}'))",
        }

        novas_bandas_base = []
        for s in sufixos:
            for nome_indice, expressao in formulas_base.items():
                expr_formatada = expressao.format(s=s)
                nome_banda = f"{nome_indice}{s}"
                banda_calc = img.expression(f"float({expr_formatada})").rename(nome_banda)
                novas_bandas_base.append(banda_calc)

        img_com_base = ee.Image.cat([img] + novas_bandas_base)
        bandas_dependentes = []
        for s in sufixos:
            lai = img_com_base.expression(f"float(3.618 * (b('evi{s}') - 0.118))").rename(f"lai{s}")
            # nddi = img_com_base.expression(f"float((b('ndvi{s}') - b('ndwi{s}')) / (b('ndvi{s}') + b('ndwi{s}')))").rename(f"nddi{s}")
            spri = img_com_base.expression(f"float((b('pri{s}') + 1) / 2)").rename(f"spri{s}")
            bandas_dependentes.extend([lai,  spri]) # nddi,
          
        imagem_final = ee.Image.cat([img_com_base] + bandas_dependentes) 
        imagem_final = self.addSlopeAndHilshade(imagem_final)
        imagem_final = self.agregate_Bands_SMA_NDFIa(imagem_final)
        imagem_final = self.agregateBandsContextoEstrutural(imagem_final)

        return imagem_final

    # =========================================================================
    # FUNÇÃO DE ITERAÇÃO OTIMIZADA PARA O NOVO FLUXO
    # =========================================================================
    def iterate_by_grid(self, idGrade, askSize, grade_feat):        
        
        # A Geometria e o Valor de Região vêm diretamente da Feature injetada pelo Loop
        oneGrade = ee.Feature(grade_feat).geometry()
        val_regiao = ee.Number(ee.Feature(grade_feat).get('regiao_val')) # 1 = Cerrado, 2 = Caatinga

        list_of_fc_years = []        

        for nyear in self.lst_year[:]:
            bandYear = 'classification_' + str(nyear)
            print(f" Processing grid_year => {idGrade} <> {bandYear} ")     
            
            date_inic = ee.Date.fromYMD(nyear, 1, 1)

            # 1. Filtra a Coleção Mosaico pelo Ano (o Mosaico já vem empacotado do GEE)
            imgColfiltered = (self.imgMosaic
                                .filter(ee.Filter.eq('year', nyear))
                                .filterBounds(oneGrade))
                                
            # Achata a coleção num mosaico pra podermos injetar na sua função CalculateIndice
            img_base = imgColfiltered.mosaic()

            print("----- Calculando todos os índices nativos do Sentinel -----")
            img_recMosaicnewB = self.CalculateIndice_otimizado(img_base)
            
            # 2. Seleciona APENAS as bandas espectrais e índices gerados no passo acima
            img_spectral = img_recMosaicnewB.select(self.allbands)

            # 3. Adiciona as Bandas Embedding com Segurança
            # Se não houver embedding (ex: ano de 2025), ele injeta uma matriz de zeros mascarada
            embedding_years = (self.mosaico_embedding
                                    .filterBounds(oneGrade)
                                        .filterDate(date_inic, date_inic.advance(1, 'year'))
                            )
            
            img_embed = ee.Image(ee.Algorithms.If(
                embedding_years.size().gt(0),
                embedding_years.mosaic(),
                ee.Image.constant([0]*64).rename([f'embedding_{i}' for i in range(64)]).updateMask(0)
            ))

            # 4. Construção da Classe Dinâmica (0, 1 ou 2)
            # layerCC vira booleano (1=Savana, 0=Background) e depois multiplicamos pela região
            layerCC = self.imgMapbiomas.select(bandYear).eq(4).multiply(val_regiao).rename('class')   
            
            # 5. Empilha e Amostra
            ptosTemp = (
                img_spectral
                .addBands(img_embed)
                .addBands(layerCC)
                .addBands(ee.Image.constant(nyear).rename('year'))
                .addBands(ee.Image.constant(idGrade).rename('GRID_ID'))
                # .sample(
                #     region= oneGrade,  
                #     scale= 10,   
                #     numPixels= 500,
                #     dropNulls= True,
                #     geometries= True,
                #     tileScale= 16
                # )
                .stratifiedSample(
                    numPoints= 250,      # Solicita 250 do background e 250 da savana
                    classBand= 'class',
                    region= oneGrade,  
                    scale= 10,   
                    dropNulls= True,
                    geometries= True,
                    tileScale= 16
                )
            )
            list_of_fc_years.append(ptosTemp)

        name_exp = 'rois_grade_' + str(idGrade) 

        # Achatando a lista em uma única FeatureCollection otimizada
        if list_of_fc_years:
            shpAllFeat = ee.FeatureCollection(list_of_fc_years).flatten()
            # Validação: Só agenda a exportação se a feature collection realmente extraiu dados válidos
            # total_amostras = shpAllFeat.size().getInfo()
            # if total_amostras > 0:
            print("Sucesso! amostras coletadas. Agendando exportação...")
            self.save_ROIs_toAsset(shpAllFeat, name_exp)
            # else:
            #     print(f"⚠️ A grade {idGrade} não gerou amostras válidas. Exportação cancelada.")
        else:
            print("Nenhuma amostra processada para esta bacia.")
                
    
    def save_ROIs_toAsset(self, collection, name):
        optExp = {
            'collection': collection,
            'description': name,
            'assetId': self.options['asset_output_grade'] + "/" + name
        }
        task = ee.batch.Export.table.toAsset(**optExp)
        task.start()
        print("exportando ROIs da grade %s ...!", name)


def ask_byGrid_saved(dict_asset):
    # Usando listAssets que é o padrão atual que não quebra no GEE
    getlstFeat = ee.data.listAssets(dict_asset)
    lst_temporalAsset = []
    
    for item in getlstFeat.get('assets', []):         
        name_feat = item['name'].split('/')[-1]
        print("reading <==> " + name_feat)
        idGrade = name_feat.split('_')[2]
        if int(idGrade) not in lst_temporalAsset:
            lst_temporalAsset.append(int(idGrade))

    return lst_temporalAsset

# =========================================================================
# FLUXO DE EXECUÇÃO
# =========================================================================

# asset_grades_area de pesquisa
asset_pesquisa = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/grades_area_pesquisa_caatinga_cerrado_v2'
# asset_grades_cerrado coletas savana
asset_coleta_cerrado = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/grades_area_coleta_cerrado_v2'
# asset_grades_caatinga coletas savanas 
asset_coleta_caatinga = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/grades_area_coleta_caatinga_v2'

# ATRIBUIÇÃO DOS VALORES DE REGIÃO DIRETAMENTE NA COLEÇÃO ANTES DO MERGE
# Cerrado = 1 | Caatinga = 2
fc_cerrado = ee.FeatureCollection(asset_coleta_cerrado).map(lambda f: f.set('regiao_val', 1))
fc_caatinga = ee.FeatureCollection(asset_coleta_caatinga).map(lambda f: f.set('regiao_val', 2))

grades_coleta = fc_cerrado.merge(fc_caatinga)

lstIdCode = grades_coleta.reduceColumns(ee.Reducer.toList(), ['indice']).get('list').getInfo()

objetoMosaic_exportROI = ClassMosaic_indexs_Spectral()
print("saida ==> ", objetoMosaic_exportROI.options['asset_output_grade'])

searchFeatSaved = False # Defina para True se precisar evitar retrabalho

if searchFeatSaved: 
    lstFeatAsset = ask_byGrid_saved({'parent': objetoMosaic_exportROI.options['asset_output_grade']})
    print("  == size das prontas: ", len(lstFeatAsset))
    askingbySizeFC = False
else:
    lstFeatAsset = []
print("size of grade geral >> ", len(lstIdCode))

inicP = 0
endP = 800 
for cc, item in enumerate(lstIdCode[inicP:endP]):
    print(f"# {cc + 1 + inicP} loading geometry grade {item}")   
    if item not in lstFeatAsset:
        
        # Filtra a grade E extrai o Feature (não apenas a geometria)
        # Isso nos permite acessar o 'regiao_val' dentro da função iterate_by_grid
        grade_feat = grades_coleta.filter(ee.Filter.eq('indice', item)).first()
        
        # Como o .first() no GEE Python API não checa nulos, verificamos via size() pra não quebrar
        size = grades_coleta.filter(ee.Filter.eq('indice', item)).size().getInfo()
        
        if size > 0:
            objetoMosaic_exportROI.iterate_by_grid(item, False, grade_feat)