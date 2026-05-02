#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformação
DISTRIBUIDO COM GPLv2
@author: geodatin

Lê o asset GEE de amostras col11 e conta quantas amostras existem
por classe, por bacia e por ano (rois_fromBasin_{bacia}_{ano}).

Como o dictQtLimit é aplicado por ano, a contagem por ano é necessária
para saber se o limite proposto é viável em cada ano individualmente.

Assets lidos: ROIs_clean_downsamplesCCred/rois_fromBasin_{bacia}_{ano}
Saídas:
  src/dados/reprocess_v2/amostras_por_classe_bacia_ano.csv   (bacia × ano × classe)
  src/dados/reprocess_v2/amostras_por_classe_bacia.csv       (bacia × classe: min/mean/max entre anos)
  src/dados/reprocess_v2/amostras_por_classe_bacia.json      (bacia → classe → min entre anos)

Uso:
  python get_amostras_asset_gee.py             # todos os anos 1985-2024
  python get_amostras_asset_gee.py 2010 2024   # intervalo específico
"""

import ee
import os
import sys
import json
import pandas as pd
from pathlib import Path

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
pathparent = str(Path(os.getcwd()).parents[1])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account
from gee_tools import *

projAccount = get_current_account()
try:
    ee.Initialize(project=projAccount)
    print(f'GEE inicializado: {projAccount}')
except Exception as exc:
    print(f'Falha ao inicializar GEE: {exc}')
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

ASSET_ROOT = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesCCred'

BACIAS_ALVO = [
    '7411', '745', '751', '752', '753', '7541', '755', '7561', '757',
    '7581', '7584', '7591', '7592', '761111', '761112', '76116', '7614',
    '7615', '7616', '7617', '7618', '7619', '7622', '763', '764', '765',
    '766', '7671', '771', '7712', '773', '7741', '7746', '7754', '7761', '7764'
]

CLASSES_COL11 = [3, 4, 12, 15, 19, 21, 22, 29, 33, 36]

# Intervalo de anos: por padrão todos os anos da col11
ANO_INI = int(sys.argv[1]) if len(sys.argv) > 2 else 1985
ANO_FIM = int(sys.argv[2]) if len(sys.argv) > 2 else 2024
YEARS   = list(range(ANO_INI, ANO_FIM + 1))

# ---------------------------------------------------------------------------


def get_base_path():
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    return str(script_dir.parents[1])


def count_samples_bacia_ano(nbacia, year):
    """
    Retorna dict {classe: contagem} para rois_fromBasin_{nbacia}_{year}.
    Retorna None se o asset não existir ou estiver vazio.
    """
    asset_path = f"{ASSET_ROOT}/rois_fromBasin_{nbacia}_{year}"
    try:
        fc   = ee.FeatureCollection(asset_path)
        size = fc.size().getInfo()
        if size == 0:
            return None
        hist = fc.aggregate_histogram('class').getInfo()
        return {int(float(k)): int(v) for k, v in hist.items()}
    except Exception as exc:
        print(f"      Erro [{nbacia}/{year}]: {exc}")
        return None


def main():
    base_path = get_base_path()
    path_out  = os.path.join(base_path, 'dados', 'reprocess_v2')
    os.makedirs(path_out, exist_ok=True)

    print(f"\n{'='*65}")
    print(" CONTAGEM DE AMOSTRAS POR BACIA E ANO — col11")
    print(f"{'='*65}")
    print(f"  Asset raiz : {ASSET_ROOT}")
    print(f"  Anos       : {ANO_INI} – {ANO_FIM}  ({len(YEARS)} anos)")
    print(f"  Bacias     : {len(BACIAS_ALVO)}")
    print(f"  Chamadas GEE estimadas: {len(BACIAS_ALVO) * len(YEARS)}\n")

    rows_ano = []

    for nbacia in BACIAS_ALVO:
        print(f"\n  Bacia {nbacia}")
        for year in YEARS:
            hist = count_samples_bacia_ano(nbacia, year)
            if hist is None:
                print(f"    {year}: sem asset")
                continue

            total = sum(hist.values())
            row   = {'bacia': nbacia, 'ano': year, 'total': total}
            for cls in CLASSES_COL11:
                row[f'cls_{cls}'] = hist.get(cls, 0)
            rows_ano.append(row)
            classes_presentes = [c for c in CLASSES_COL11 if hist.get(c, 0) > 0]
            print(f"    {year}: total={total:5d}  classes={classes_presentes}")

    if not rows_ano:
        print("\nNenhuma bacia/ano processada. Verifique a conta GEE e o asset.")
        sys.exit(1)

    df_ano = pd.DataFrame(rows_ano)
    cols_order = ['bacia', 'ano', 'total'] + [f'cls_{c}' for c in CLASSES_COL11]
    df_ano = df_ano[cols_order]

    # ---- Salva tabela bacia × ano × classe ----
    path_csv_ano = os.path.join(path_out, 'amostras_por_classe_bacia_ano.csv')
    df_ano.to_csv(path_csv_ano, index=False)
    print(f"\n  Salvo: {path_csv_ano}  ({len(df_ano)} registros)")

    # ---- Agrega por bacia: min / mean / max entre anos ----
    cls_cols = [f'cls_{c}' for c in CLASSES_COL11]
    agg_dict = {col: ['min', 'mean', 'max'] for col in cls_cols}
    agg_dict['total'] = ['min', 'mean', 'max']

    df_agg = df_ano.groupby('bacia').agg(agg_dict)
    df_agg.columns = ['_'.join(c) for c in df_agg.columns]
    df_agg = df_agg.reset_index()

    path_csv_agg = os.path.join(path_out, 'amostras_por_classe_bacia.csv')
    df_agg.to_csv(path_csv_agg, index=False)
    print(f"  Salvo: {path_csv_agg}  ({len(df_agg)} bacias)")

    # ---- Resumo: min entre anos por classe (pior caso para definir limite viável) ----
    print(f"\n{'='*65}")
    print(" RESUMO — mínimo de amostras entre anos (pior caso por classe)")
    print(f"{'='*65}")
    for cls in CLASSES_COL11:
        col_min = f'cls_{cls}_min'
        col_mean = f'cls_{cls}_mean'
        if col_min in df_agg.columns:
            global_min  = int(df_agg[col_min].min())
            global_mean = round(df_agg[col_mean].mean(), 1)
            print(f"  Classe {cls:2d}: min_global={global_min:5d} | mean_global={global_mean:6.1f}")

    # ---- JSON com mínimo por bacia/classe (usado pelo script de balanceamento) ----
    path_json = os.path.join(path_out, 'amostras_por_classe_bacia.json')
    dict_out = {}
    for _, row in df_agg.iterrows():
        b = str(row['bacia'])
        dict_out[b] = {str(cls): int(row[f'cls_{cls}_min']) for cls in CLASSES_COL11}
    with open(path_json, 'w') as f:
        json.dump(dict_out, f, indent=2)
    print(f"\n  JSON salvo (min por ano): {path_json}")
    print(f"  Bacias processadas: {len(df_agg)}/{len(BACIAS_ALVO)}")


if __name__ == '__main__':
    main()
