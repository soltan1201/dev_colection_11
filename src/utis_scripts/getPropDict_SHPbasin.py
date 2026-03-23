#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
#SCRIPT DE CLASSIFICACAO POR BACIA
#Produzido por Geodatin - Dados e Geoinformacao
#DISTRIBUIDO COM GPLv2
'''

import ee
import os 
import sys
from pathlib import Path
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

param = {
    'asset_bacias_buffer' : 'projects/ee-solkancengine17/assets/shape/bacias_buffer_caatinga_49_regions',
}
lstIdCod = [20,28,41,49,43,37,46,47,21,12,13,14,27,31,32,39,38,11,22,34,1,25,29]
print(" dados definidos ")
baciabuffer = (ee.FeatureCollection(param['asset_bacias_buffer'])
                        .filter(ee.Filter.inList('id_codigo', lstIdCod)))
                        # ee.Filter.eq('nunivotto4', _nbacia))
print(f"know about the geometry 'nunivotto4' >>  loaded < {baciabuffer.size().getInfo()} > geometry" ) 

lstCod = baciabuffer.reduceColumns(ee.Reducer.toList(), ['nunivotto4']).get('list').getInfo()
print("lista de bacias a processar primeiramente ", lstCod)