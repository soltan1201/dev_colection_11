#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
# SCRIPT DE RE SAMPLING POR BACIA
# Produzido por Geodatin - Dados e Geoinformacao
# DISTRIBUIDO COM GPLv2
'''

import ee 
import sys
import os
import json
from tqdm import tqdm
from pathlib import Path
import collections
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
print("ver >> ", pathparent)
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
projAccount = get_current_account()
from gee_tools import *
print(f"projetos selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project= projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise


# =========================================================================
# PARÂMETROS E ASSETS
# =========================================================================
assetMapbiomas100 = 'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_integration_v2'
asset_bacias = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions'

# Asset de saída (Ajuste o caminho da pasta onde quer salvar a tabela)
asset_saida = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/estatisticas_bacias_1985_2024'

classMapB = [3, 4, 5, 9, 12, 13, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32, 33, 36, 39, 40, 41, 46, 47, 48, 49, 50, 62, 75]
classNew  = [3, 4, 3, 3, 12, 12, 15, 19, 19, 19, 21, 25, 25, 25, 25, 33, 29, 25, 33, 12, 33, 36, 19, 19, 19, 36, 36, 36,  4, 12, 19, 25]

ano_inicial = 1985
ano_final = 2024

# =========================================================================
# LÓGICA 100% SERVER-SIDE
# =========================================================================
bacias = ee.FeatureCollection(asset_bacias)
mapbiomas = ee.Image(assetMapbiomas100)
area_img = ee.Image.pixelArea().divide(10000) # Hectares

# Cria uma lista de anos no Lado do Servidor
anos_lista = ee.List.sequence(ano_inicial, ano_final)

# Função para processar um ano específico
def processar_ano(ano_ee):
    ano_ee = ee.Number(ano_ee).toInt()
    band_name = ee.String('classification_').cat(ano_ee.format('%d'))
    
    # Prepara a imagem do ano
    img_ano = mapbiomas.select([band_name]).rename('class')  # .remap(classMapB, classNew)
    img_calc = area_img.addBands(img_ano)
    
    # Reducer em grupo
    stats = img_calc.reduceRegions(
        collection=bacias,
        reducer=ee.Reducer.sum().group(groupField=1, groupName='class'),
        scale=300,
        tileScale=16 
    )
    
    # Função para reformatar a saída de cada bacia e extrair os grupos para colunas planas
    def formatar_bacia(feat):
        grupos = ee.List(feat.get('groups'))
        
        # Calcula a área total da bacia neste ano
        area_total = ee.Number(grupos.map(lambda g: ee.Dictionary(g).getNumber('sum')).reduce(ee.Reducer.sum()))
        
        # Função iterativa para transformar a lista de grupos em um único dicionário de colunas
        def achatar_grupos(g, dict_acumulado):
            g = ee.Dictionary(g)
            classe = ee.Number(g.get('class')).format('%d')
            area = ee.Number(g.get('sum'))
            pct = area.divide(area_total).multiply(100)
            
            nome_col_area = ee.String('class_').cat(classe).cat('_ha')
            nome_col_pct = ee.String('class_').cat(classe).cat('_pct')
            
            return ee.Dictionary(dict_acumulado).set(nome_col_area, area).set(nome_col_pct, pct)
        
        # Constrói o dicionário com todas as classes para esta bacia
        dicionario_classes = ee.Dictionary(grupos.iterate(achatar_grupos, ee.Dictionary()))
        
        # Adiciona o ID da bacia e o ano
        propriedades_finais = dicionario_classes.set('nunivotto4', feat.get('nunivotto4')).set('year', ano_ee)
        
        # Retorna uma Feature nula (sem geometria) para criar uma tabela extremamente leve!
        return ee.Feature(None, propriedades_finais)
        
    return stats.map(formatar_bacia)

# Mapeia a função sobre todos os anos e achata o resultado final
colecao_estatisticas = ee.FeatureCollection(anos_lista.map(processar_ano)).flatten()

# =========================================================================
# EXPORTAÇÃO PARA ASSET (TABLE)
# =========================================================================
nome_tarefa = f'Export_Stats_Bacias_{ano_inicial}_{ano_final}'

tarefa = ee.batch.Export.table.toAsset(
    collection=colecao_estatisticas,
    description=nome_tarefa,
    assetId=asset_saida
)

tarefa.start()

print("\n" + "="*60)
print(f"🚀 Tarefa '{nome_tarefa}' iniciada com sucesso!")
print(f"📂 Destino: {asset_saida}")
print("Acompanhe o progresso na aba 'Tasks' do Code Editor.")
print("="*60)