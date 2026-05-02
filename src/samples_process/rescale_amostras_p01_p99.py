#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformacao
DISTRIBUIDO COM GPLv2

Aplica normalização min-max p01/p99 (por bacia/ano) nas propriedades de bandas
brutas de cada FeatureCollection de amostras, exportando para um novo folder.

Fonte dos percentis : src/dados/dict_percentis_p01_p99_bacia_ano.json
                      (gerado por gerar_percentis_p01_p99_bacia_ano.py)
Asset de entrada    : ROIs_clean_downsamplesCCred/rois_fromBasin_{bacia}_{ano}
Asset de saída      : ROIs_clean_downsamplesCCred_scaled/rois_fromBasin_{bacia}_{ano}

Nota: as propriedades de índices espectrais (ndvi, ndwi, etc.) não são
recomputadas — permanecem com os valores originais do mosaico max.
Apenas as 18 bandas brutas (annual/wet/dry × 6 bandas) são normalizadas.
"""

import ee
import os
import sys
import json
from pathlib import Path
import collections
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
from gee_tools import switch_user, tasks

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

# ==== Configuração ====
ASSET_ROIS_IN  = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesCCredv2'
ASSET_ROIS_OUT = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesCCred_scaled'

LST_BAND_IMP = [
    'blue_median',     'green_median',     'red_median',     'nir_median',     'swir1_median',     'swir2_median',
    'blue_median_dry', 'green_median_dry', 'red_median_dry', 'nir_median_dry', 'swir1_median_dry', 'swir2_median_dry',
    'blue_median_wet', 'green_median_wet', 'red_median_wet', 'nir_median_wet', 'swir1_median_wet', 'swir2_median_wet',
]

P_LOW  = 1
P_HIGH = 99

NAME_BACIAS = [
    '765', '7544', '7541', '7411', '746', '7591', '7592',
    '761111', '761112', '7612', '7613', '7614', '7615',
    '771', '7712', '772', '7721', '773', '7741', '7746',
    '7754', '7761', '7764', '7691', '7581', '7625', '7584',
    '751', '752', '7616', '745', '7424', '7618', '7561',
    '755', '7617', '7564', '7422', '76116', '7671', '757',
    '766', '753', '764', '7619', '7443', '7438', '763', '7622'
]

# Amostras disponíveis até 2024 (2025 usa proxy de 2024)
ANOS = list(range(1985, 2025))

JSON_PERCENTIS = os.path.join(pathparent, 'dados', 'dict_percentis_p01_p99_bacia_ano.json')

param = {
    'numeroTask':  6,
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
}


def load_json(path):
    with open(path, 'r') as f:
        return json.load(f)


def get_assets_saved(asset_folder):
    """Retorna set com nomes dos assets já exportados no folder de saída."""
    try:
        lst  = ee.data.getList({'id': asset_folder})
        base = f"projects/earthengine-legacy/assets/{asset_folder}/"
        return {item['id'].replace(base, '') for item in lst}
    except Exception as e:
        print(f"  WARN: não foi possível listar assets em {asset_folder}: {e}")
        return set()


def make_rescale_fn(bacia_stats, bands, p_low_suffix, p_high_suffix):
    """
    Retorna uma função GEE-side (para .map()) que normaliza apenas as bandas
    brutas (LST_BAND_IMP) de cada feature, preservando geometry e todas as
    demais propriedades (índices espectrais, class, year, GRID_ID, etc.).

    Estratégia: constrói um ee.Dictionary com os 18 valores normalizados e
    aplica feat.set(dict) em uma única chamada — mais eficiente e garante que
    o feature original (geometria + outras colunas) é mantido intacto.
    """
    # Pré-computa escalares Python para evitar getInfo() dentro do map
    scale_params = {}
    for band in bands:
        p_low  = float(bacia_stats.get(f"{band}_{p_low_suffix}",  0)     or 0)
        p_high = float(bacia_stats.get(f"{band}_{p_high_suffix}", 10000) or 10000)
        scale_params[band] = (p_low, max(p_high - p_low, 1.0))

    def rescale_feat(feat):
        # Calcula cada valor normalizado e acumula num dict GEE
        scaled = {}
        for band, (p_low, range_) in scale_params.items():
            scaled[band] = (
                ee.Number(feat.get(band))
                  .subtract(p_low)
                  .divide(range_)
                  .clamp(0, 1)
                  .multiply(10000)
            )
        # set(dict) preserva a geometria e todas as demais propriedades do feature
        return feat.set(scaled)

    return rescale_feat


def gerenciador(cont):
    numberofChange = [kk for kk in param['conta'].keys()]
    print(numberofChange)
    if str(cont) in numberofChange:
        print(f"inicialize in account #{cont} <> {param['conta'][str(cont)]}")
        switch_user(param['conta'][str(cont)])
        projAccount_new = get_project_from_account(param['conta'][str(cont)])
        try:
            ee.Initialize(project=projAccount_new)
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


# ==== Principal ====
dict_percentis = load_json(JSON_PERCENTIS)
print(f"JSON de percentis carregado: {len(dict_percentis)} bacias.")

assets_saved = get_assets_saved(ASSET_ROIS_OUT)
print(f"Assets já exportados no destino: {len(assets_saved)}\n")

p_low_suffix  = f"p{P_LOW}"
p_high_suffix = f"p{P_HIGH}"

inicP = 0   # fatia de bacias a processar (ajuste conforme necessário)
endP  = len(NAME_BACIAS)

for nbacia in NAME_BACIAS[inicP:endP]:
    bacia_dict = dict_percentis.get(str(nbacia), {})
    if not bacia_dict:
        print(f"[{nbacia}] sem percentis no JSON. Pulando bacia inteira.")
        continue

    for nyear in ANOS:
        name_fc = f"rois_fromBasin_{nbacia}_{nyear}"

        if name_fc in assets_saved:
            print(f"  skip {name_fc} (já existe)")
            continue

        bacia_stats = bacia_dict.get(str(nyear), {})
        if not bacia_stats:
            print(f"  WARN: sem percentis para {nbacia}/{nyear}. Pulando.")
            continue

        print(f"  exportando {name_fc} ...", end=' ', flush=True)
        try:
            fc = ee.FeatureCollection(f"{ASSET_ROIS_IN}/{name_fc}")

            rescale_fn = make_rescale_fn(bacia_stats, LST_BAND_IMP, p_low_suffix, p_high_suffix)
            fc_scaled  = fc.map(rescale_fn)

            task = ee.batch.Export.table.toAsset(
                collection=fc_scaled,
                description=name_fc,
                assetId=f"{ASSET_ROIS_OUT}/{name_fc}"
            )
            task.start()
            print("task iniciada.")
        except Exception as e:
            print(f"ERRO: {e}")

print("\nTodas as tasks lançadas.")
