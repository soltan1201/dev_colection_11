#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2
Classificação em 3 Macro-Classes usando Gradient Tree Boost
"""

import ee
import os
import sys
from pathlib import Path

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account

projAccount = get_current_account()
print(f"Projeto selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project=projAccount)
    print('✅ Earth Engine inicializado com sucesso!')
except Exception as e:
    print(f'❌ Erro ao inicializar o GEE: {e}')
    sys.exit()


class Classificador_GTB_Caatinga(object):

    options = {
        'bnd_L': ['blue','green','red','nir','swir1','swir2'],
        'assetMapbiomas100': 'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_integration_v2',
        'asset_mosaic_sentinelp2': 'projects/nexgenmap/MapBiomas2/SENTINEL/mosaics-3',
        'asset_mosaic_sentinelp1': 'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3',
        'asset_embedding': "GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL",
        'asset_bacias_geom': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions',
        
        # ASSET DA SUA FEATURE COLLECTION ÚNICA COM AS AMOSTRAS:
        'asset_amostras_treino': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_consolidado_cerr_caat_embeddin_ALL',
        
        # PASTA ONDE OS MAPAS CLASSIFICADOS SERÃO SALVOS:
        'asset_output_maps': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classificacao_3Classes', 
        
        "anoIntInit": 2022, 
        "anoIntFin": 2024,  
        
        # Parâmetros do Gradient Tree Boost
        'gtb_trees': 50,
        'gtb_learning_rate': 0.1
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
    ] 

    def __init__(self):
        self.imgMosaic = ee.ImageCollection(self.options['asset_mosaic_sentinelp1']).merge(
                                    ee.ImageCollection(self.options['asset_mosaic_sentinelp2']))
                                      
        print("==================================================")
        self.lst_year = list(range(self.options['anoIntInit'], self.options['anoIntFin'] + 1))
        
        self.mosaico_embedding = ee.ImageCollection(self.options['asset_embedding'])
        self.bacias_geom = ee.FeatureCollection(self.options['asset_bacias_geom'])

    # =========================================================================
    # BLOCO DE CONSTRUÇÃO DA IMAGEM (Idêntico ao da extração para bater perfeitamente)
    # =========================================================================
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
            [0.05, 0.09, 0.04, 0.61, 0.30, 0.10], [0.14, 0.17, 0.22, 0.30, 0.55, 0.30], 
            [0.20, 0.30, 0.34, 0.58, 0.60, 0.58], [0.0 , 0.0,  0.0 , 0.0 , 0.0 , 0.0 ], 
            [0.90, 0.96, 0.80, 0.78, 0.72, 0.65]  
        ]
        fractions = ee.Image(IMAGE).select(lstBandsSuf).unmix(endmembers=endmembers, sumToOne=True, nonNegative=True).float()
        fractions = fractions.rename(lstFractions)
        NDFI_ADJUSTED = fractions.expression(
            "float(((b('gv') / (1 - b('shade'))) - b('soil')) / ((b('gv') / (1 - b('shade'))) + b('npv') + b('soil')))"
        ).rename('ndfia' + sufixo).toFloat()
        
        fractions = fractions.rename(lstFractionsSuf)
        return ee.Image(fractions.toFloat().addBands(NDFI_ADJUSTED)).toFloat()

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
            spri = img_com_base.expression(f"float((b('pri{s}') + 1) / 2)").rename(f"spri{s}")
            bandas_dependentes.extend([lai, spri]) 
          
        imagem_final = ee.Image.cat([img_com_base] + bandas_dependentes) 
        imagem_final = self.addSlopeAndHilshade(imagem_final)
        imagem_final = self.agregate_Bands_SMA_NDFIa(imagem_final)
        imagem_final = self.agregateBandsContextoEstrutural(imagem_final)

        return imagem_final

    # =========================================================================
    # LÓGICA DE TREINAMENTO E CLASSIFICAÇÃO (3 CLASSES)
    # =========================================================================
    
    def treinar_modelo_anual(self, ano):
        """
        Carrega a FeatureCollection única, filtra pelo ano, 
        remapeia para 3 classes e treina o GTB.
        """
        print(f"🌲 Treinando modelo GTB para o ano {ano}...")
        fc_amostras = ee.FeatureCollection(self.options['asset_amostras_treino'])
        
        # Filtra os pontos de treinamento do ano específico
        amostras_ano = fc_amostras.filter(ee.Filter.eq('year', ano))
        
        # DEFINIÇÃO DAS 3 MACRO-CLASSES:
        # 1: Vegetação Nativa (3, 4, 12)
        # 2: Agropecuária / Antrópico (15, 18, 19, 21)
        # 3: Outros / Solo / Água / Não-vegetado (22, 25, 29, 33, 36)
        classes_originais = [3, 4, 12, 15, 18, 19, 21, 22, 25, 29, 33, 36]
        classes_novas     = [1, 1,  1,  2,  2,  2,  2,  3,  3,  3,  3,  3]
        
        # Cria uma nova coluna 'macro_class' com os valores 1, 2 e 3
        amostras_remapeadas = amostras_ano.remap(classes_originais, classes_novas, 'class', 'macro_class')
        
        # Monta a lista de features (Bandas selecionadas + Embedding)
        features_para_treino = self.allbands + [f'embedding_{i}' for i in range(64)]
        
        # Instancia e treina o Gradient Tree Boost
        classificador = ee.Classifier.smileGradientTreeBoost(
            numberOfTrees=self.options['gtb_trees'],
            shrinkage=self.options['gtb_learning_rate'],
            seed=42
        ).train(
            features=amostras_remapeadas,
            classProperty='macro_class',
            inputProperties=features_para_treino
        )
        
        return classificador

    def classificar_bacia(self, bacia_id, ano, classificador):
        """
        Gera a imagem da bacia, classifica com o modelo treinado e exporta.
        """
        # Pega a geometria da bacia para recortar o mapa
        geometria_bacia = self.bacias_geom.filter(ee.Filter.eq('nunivotto4', bacia_id)).geometry()
        date_inic = ee.Date.fromYMD(ano, 1, 1)

        # 1. Reconstrói o mosaico e índices espectrais
        imgColfiltered = (self.imgMosaic
                            .filter(ee.Filter.eq('year', ano))
                            .filterBounds(geometria_bacia))
        
        img_base = imgColfiltered.mosaic()
        img_indices = self.CalculateIndice_otimizado(img_base).select(self.allbands)

        # 2. Adiciona as Bandas Embedding
        embedding_years = (self.mosaico_embedding
                                .filterBounds(geometria_bacia)
                                .filterDate(date_inic, date_inic.advance(1, 'year')))
        
        img_embed = ee.Image(ee.Algorithms.If(
            embedding_years.size().gt(0),
            embedding_years.mosaic(),
            ee.Image.constant([0]*64).rename([f'embedding_{i}' for i in range(64)]).updateMask(0)
        ))

        # 3. Junta as bandas e aplica o classificador GTB
        imagem_pronta = img_indices.addBands(img_embed)
        
        # O resultado será uma imagem com uma banda chamada 'classification' contendo 1, 2 ou 3
        mapa_classificado = imagem_pronta.classify(classificador).rename(f'classification_{ano}')
        
        # Converte para Byte (Economiza 90% do espaço de armazenamento no GEE)
        mapa_classificado = mapa_classificado.toByte()

        # 4. Agendamento da Exportação
        nome_arquivo = f"Classificacao_{bacia_id}_{ano}_3Classes"
        asset_id_out = f"{self.options['asset_output_maps']}/{nome_arquivo}"

        optExp = {
            'image': mapa_classificado,
            'description': nome_arquivo,
            'assetId': asset_id_out,
            'region': geometria_bacia,
            'scale': 10,  # Resolução do Sentinel
            'maxPixels': 1e13,
            # Importante: Como são classes categóricas, usamos 'mode' nas pirâmides de visualização
            'pyramidingPolicy': {'.default': 'mode'} 
        }

        try: ee.data.deleteAsset(asset_id_out)
        except: pass

        task = ee.batch.Export.image.toAsset(**optExp)
        task.start()
        print(f"   ⏳ Exportando Mapa -> {nome_arquivo}")

# =========================================================================
# FLUXO DE EXECUÇÃO DA CLASSIFICAÇÃO
# =========================================================================

nameBacias = [
    '7411', '7754', '7691', '7581', '7625', '7584', '751', '7614', 
    '752', '7616', '745', '7424', '773', '7612', '7613', 
    '7618', '7561', '755', '7617', '7564', '761111', '761112', 
    '7741', '7422', '76116', '7761', '7671', '7615',  
    '7764', '757', '771', '7712', '766', '7746', '753', '764', 
    '7721', '772', '7619', '765', '7438', '7591', '7592', '7622',
    '746', '7541', '7443', '7544', '763'    
]

classificador_app = Classificador_GTB_Caatinga()

# Itera sobre cada ano (treina 1 modelo por ano para garantir precisão temporal)
for ano in classificador_app.lst_year:
    print(f"\n{'='*50}\nIniciando Ciclo para o ANO: {ano}\n{'='*50}")
    
    # 1. Treina o Modelo GTB para este ano usando TODAS as amostras consolidadas
    modelo_gtb_ano = classificador_app.treinar_modelo_anual(ano)
    
    # 2. Aplica o modelo já treinado sobre cada bacia e exporta
    for cc, bacia in enumerate(nameBacias):
        print(f"[{cc+1}/{len(nameBacias)}] Classificando bacia {bacia}...")
        classificador_app.classificar_bacia(bacia, ano, modelo_gtb_ano)

print("\n🚀 Todas as tarefas de classificação foram enfileiradas!")