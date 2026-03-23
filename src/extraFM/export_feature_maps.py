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
import time
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

asset_ROIs = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_byBasinInd/rois_fromGrade_'
asset_bacias = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions'
asset_output = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/rois_resample_featmaps'

bacias = ee.FeatureCollection(asset_bacias)

# Define a list of years to export
nyear = '1990'

# Função para exportar as amostras para o asset
def processoExportar_toAsset(areaFeat, nameT):      
    optExp = {
        'collection': ee.FeatureCollection(areaFeat), 
        'description': nameT, 
        'assetId': f"{asset_output}/{nameT}"
    }    
    task = ee.batch.Export.table.toAsset(**optExp)
    task.start() # No Python, é obrigatório dar .start() na tarefa
    print(f"Enviando tarefa para o servidor: salvando ... {nameT} ..!")

nameBacias = [
    # '7411','7754', '7691', '7581', '7625', '7584', '751', '7614', 
    # '752', '7616', '745', '7424', '773', '7612', '7613', 
    # '7618', '7561', '755', '7617', '7564', '761111','761112', 
    # '7741', '7422', '76116', '7761', '7671', '7615',  
    # '7764', '757', '771', '7712', '766', '7746', '753', '764', 
    # '7721', '772', '7619',  '765', '7438', '7591', '7592', '7622', '746'
    '7541',  '7443',  '7544',  '763'    
]

print(f"We have bacias: {len(nameBacias)}")

# O loop nativo do Python substitui o forEach
for cc, nbacias in enumerate(nameBacias):
    print(f"Processando bacia {nbacias}...")
    
    temporal_basin = bacias.filter(ee.Filter.eq('nunivotto4', nbacias))
    
    featMaps = (ee.ImageCollection(f'projects/solvedltda/assets/MB11_FM/{nyear}')
                .filterBounds(temporal_basin.geometry())
                .mosaic())
    
    rois_temporal = (ee.FeatureCollection(f"{asset_ROIs}{nbacias}")
                     .filter(ee.Filter.eq('year', int(nyear))))
    
    # IMPORTANTE: Descomente as linhas abaixo apenas para debugar uma bacia específica.
    # Usar .getInfo() em loop vai deixar o script extremamente lento.
    # print(f"show metadados Feature Maps year {nyear}:", featMaps.bandNames().getInfo())
    # print("we load features samples with size:", rois_temporal.size().getInfo())

    # Adiciona uma coluna de números aleatórios de 0 a 1
    rois_temporal = rois_temporal.randomColumn('rand')
    
    # Filtra mantendo apenas 20% dos pontos da bacia (reduz a carga em 80%)
    # Ajuste o 0.2 para mais ou para menos conforme a necessidade do seu modelo
    rois_temporal_reduzido = rois_temporal.filter(ee.Filter.lt('rand', 0.2))
    
    # sampleRegions extrai os pixels de featMaps para os pontos da FeatureCollection
    read_temporal = featMaps.sampleRegions(
        collection=rois_temporal_reduzido, 
        # properties=rois_temporal.first().propertyNames(), 
        scale=30, 
        tileScale=16, 
        geometries=True
    )
    
    name_export = f'samples_{nbacias}_{nyear}' # Adicionei o ano ao nome para não sobrescrever exportações futuras
    
    processoExportar_toAsset(read_temporal, name_export)

# 📡 Bônus: Monitor de Tarefas (Task Tracker) no Python
print("\n=================================================")
print("Todas as tarefas foram enviadas! Iniciando monitoramento...")
print("=================================================")

def monitorar_tarefas():
    while True:
        # Busca todas as tarefas do usuário logado
        tarefas = ee.batch.Task.list()
        
        # Filtra apenas as tarefas que estão ativas (rodando ou na fila)
        tarefas_ativas = [t for t in tarefas if t.state in ['READY', 'RUNNING']]
        
        if not tarefas_ativas:
            print("\n✅ TODAS AS TAREFAS FORAM CONCLUÍDAS (Ou falharam). Verifique seu Asset!")
            break
            
        print(f"\n[{time.strftime('%X')}] Status atual:")
        for t in tarefas_ativas:
            # Imprime o nome da tarefa, o status e o tempo decorrido
            print(f" -> {t.config['description']}: {t.state}")
            
        # Espera 30 segundos antes de checar novamente (para não bombardear o servidor)
        time.sleep(30)

# Inicia o monitoramento
monitorar_tarefas()