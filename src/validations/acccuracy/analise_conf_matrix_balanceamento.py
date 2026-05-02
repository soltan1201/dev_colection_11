#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformação
DISTRIBUIDO COM GPLv2
@author: geodatin

Calcula matrizes de confusão por bacia (nc10 e nc7) para col11.
Extrai erros de omissão e comissão por classe — insumo para balanceamento v2.

Saída: src/dados/reprocess_v2/erros_omissao_comissao.csv
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.metrics import confusion_matrix
from tabulate import tabulate

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------

BACIAS_ALVO = [
    '7411', '745', '751', '752', '753', '7541', '755', '7561', '757',
    '7581', '7584', '7591', '7592', '761111', '761112', '76116', '7614',
    '7615', '7616', '7617', '7618', '7619', '7622', '763', '764', '765',
    '766', '7671', '771', '7712', '773', '7741', '7746', '7754', '7761', '7764'
]

YEARS = list(range(1985, 2025))

CLASSES_EXCLUDE = {0, 27}

# Mapeamento texto → código numérico (nc10: 10 classes, alinhado com dictQtLimit)
# Classe 36 não aparece nas labels de referência LAPIG → fica apenas em predições
REMAP_REF_NC10 = {
    "FORMAÇÃO FLORESTAL":             3,
    "FORMAÇÃO SAVÂNICA":              4,
    "MANGUE":                         3,
    "RESTINGA HERBÁCEA":              3,
    "FLORESTA PLANTADA":             21,
    "FLORESTA INUNDÁVEL":             3,
    "CAMPO ALAGADO E ÁREA PANTANOSA": 12,
    "APICUM":                        12,
    "FORMAÇÃO CAMPESTRE":            12,
    "OUTRA FORMAÇÃO NÃO FLORESTAL":  12,
    "AFLORAMENTO ROCHOSO":           29,
    "PASTAGEM":                      15,
    "CANA":                          19,
    "LAVOURA TEMPORÁRIA":            19,
    "LAVOURA PERENE":                21,
    "MINERAÇÃO":                     25,
    "PRAIA E DUNA":                  25,
    "INFRAESTRUTURA URBANA":         25,
    "VEGETAÇÃO URBANA":              25,
    "OUTRA ÁREA NÃO VEGETADA":       25,
    "RIO, LAGO E OCEANO":            33,
    "AQUICULTURA":                   33,
    "NÃO OBSERVADO":                 27,
}

# Mapeamento texto → código (nc7: 7 classes — pastagem e lavoura → 21; afloramento → 25)
REMAP_REF_NC7 = {
    "FORMAÇÃO FLORESTAL":             3,
    "FORMAÇÃO SAVÂNICA":              4,
    "MANGUE":                         3,
    "RESTINGA HERBÁCEA":              3,
    "FLORESTA PLANTADA":             21,
    "FLORESTA INUNDÁVEL":             3,
    "CAMPO ALAGADO E ÁREA PANTANOSA": 12,
    "APICUM":                        12,
    "FORMAÇÃO CAMPESTRE":            12,
    "OUTRA FORMAÇÃO NÃO FLORESTAL":  12,
    "AFLORAMENTO ROCHOSO":           29,
    "PASTAGEM":                      21,
    "CANA":                          21,
    "LAVOURA TEMPORÁRIA":            21,
    "LAVOURA PERENE":                21,
    "MINERAÇÃO":                     25,
    "PRAIA E DUNA":                  25,
    "INFRAESTRUTURA URBANA":         25,
    "VEGETAÇÃO URBANA":              25,
    "OUTRA ÁREA NÃO VEGETADA":       25,
    "RIO, LAGO E OCEANO":            33,
    "AQUICULTURA":                   33,
    "NÃO OBSERVADO":                 27,
}

# Remap predições nc10: classificador emite código 22 para Área Não Vegetada,
# referência usa 25 → alinha para comparação
REMAP_PRED_NC10 = {3: 3, 4: 4, 12: 12, 15: 15, 19: 19, 21: 21, 22: 25, 29: 29, 33: 33, 36: 36}

# Remap predições nc7: colapsa pastagem/lavoura/36 → 21 e afloramento/22 → 25
REMAP_PRED_NC7 = {3: 3, 4: 4, 12: 12, 15: 21, 19: 21, 21: 21, 22: 25, 25: 25, 29: 25, 33: 33, 36: 21}

NOME_CLASSES = {
    3:  'Floresta',
    4:  'Savana',
    12: 'Campo/Campestre',
    15: 'Pastagem',
    19: 'Lavoura temporaria',
    21: 'Agropasture/Mosaic',
    25: 'Outra Área Não Vegetada',
    29: 'Afloramento Rochoso',
    33: 'Água',
    36: 'Lavoura perene',
}


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------

def get_base_path():
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    return str(script_dir.parents[1])


def calc_user_producer(cm, classes):
    rows = []
    for i, cls in enumerate(classes):
        tp       = cm[i, i]
        row_sum  = cm[i, :].sum()
        col_sum  = cm[:, i].sum()
        prod_acc = round((tp / row_sum  * 100), 2) if row_sum  > 0 else np.nan
        user_acc = round((tp / col_sum  * 100), 2) if col_sum  > 0 else np.nan
        omissao  = round(100 - prod_acc, 2)         if not np.isnan(prod_acc) else np.nan
        comissao = round(100 - user_acc, 2)         if not np.isnan(user_acc) else np.nan
        rows.append({
            'classe':     cls,
            'nome':       NOME_CLASSES.get(cls, str(cls)),
            'TP':         int(tp),
            'total_ref':  int(row_sum),
            'total_pred': int(col_sum),
            'prod_acc':   prod_acc,
            'user_acc':   user_acc,
            'omissao':    omissao,
            'comissao':   comissao,
        })
    return pd.DataFrame(rows)


def confusion_for_subset(ref_series, pred_series, classes, exclude):
    mask   = (~ref_series.isin(exclude)) & (~pred_series.isin(exclude)) & ref_series.notna() & pred_series.notna()
    y_true = ref_series[mask].astype(int)
    y_pred = pred_series[mask].astype(int)
    if len(y_true) < 10:
        return None, None
    cm         = confusion_matrix(y_true, y_pred, labels=classes)
    metrics_df = calc_user_producer(cm, classes)
    return cm, metrics_df


def build_series(df_sub, col_list, remap):
    parts = [df_sub[c].map(remap) if remap else df_sub[c] for c in col_list]
    return pd.concat(parts, ignore_index=True)


# ---------------------------------------------------------------------------
# Análise por versão (nc10 / nc7)
# ---------------------------------------------------------------------------

def analyze_version(df, remap_ref, remap_pred, nc_label, classes_eval):
    df = df.copy()
    df['bacia'] = df['bacia'].astype(str)

    col_refs  = [f"CLASS_{y}"          for y in YEARS if f"CLASS_{y}"          in df.columns]
    col_preds = [f"classification_{y}" for y in YEARS if f"classification_{y}" in df.columns]

    lst_rows = []

    print(f"\n{'='*65}")
    print(f"  ANÁLISE {nc_label.upper()} — classes: {classes_eval}")
    print(f"{'='*65}")

    # ---- Loop bacia por bacia ----
    for nbacia in BACIAS_ALVO:
        sub = df[df['bacia'] == nbacia]
        if sub.empty:
            print(f"\n  Bacia {nbacia}: sem dados no CSV")
            continue

        ref_b  = build_series(sub, col_refs,  remap_ref)
        pred_b = build_series(sub, col_preds, remap_pred)

        _, metrics_b = confusion_for_subset(ref_b, pred_b, classes_eval, CLASSES_EXCLUDE)
        if metrics_b is None:
            print(f"\n  Bacia {nbacia}: pontos insuficientes (< 10)")
            continue

        n_pts = int(ref_b.notna().sum())
        metrics_b['bacia'] = nbacia
        metrics_b['nc']    = nc_label
        metrics_b['n_pts'] = n_pts
        lst_rows.append(metrics_b)

        print(f"\n  Bacia {nbacia}  [{nc_label}]  — {n_pts} obs")
        print(tabulate(
            metrics_b[['nome', 'TP', 'total_ref', 'total_pred', 'prod_acc', 'user_acc', 'omissao', 'comissao']],
            headers='keys', tablefmt='psql', showindex=False,
            floatfmt='.2f'
        ))

    # ---- Global (resumo ao final) ----
    print(f"\n{'='*65}")
    print(f"  GLOBAL [{nc_label}]")
    print(f"{'='*65}")
    ref_all  = build_series(df, col_refs,  remap_ref)
    pred_all = build_series(df, col_preds, remap_pred)

    _, metrics_global = confusion_for_subset(ref_all, pred_all, classes_eval, CLASSES_EXCLUDE)
    if metrics_global is not None:
        n_pts_g = int(ref_all.notna().sum())
        metrics_global['bacia'] = 'GLOBAL'
        metrics_global['nc']    = nc_label
        metrics_global['n_pts'] = n_pts_g
        lst_rows.append(metrics_global)
        print(f"  {n_pts_g} observações totais")
        print(tabulate(
            metrics_global[['nome', 'TP', 'total_ref', 'total_pred', 'prod_acc', 'user_acc', 'omissao', 'comissao']],
            headers='keys', tablefmt='psql', showindex=False,
            floatfmt='.2f'
        ))

    return pd.concat(lst_rows, ignore_index=True) if lst_rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    base_path = get_base_path()
    path_acc  = os.path.join(base_path, 'dados', 'ptosAccCol11')
    path_out  = os.path.join(base_path, 'dados', 'reprocess_v2')
    os.makedirs(path_out, exist_ok=True)

    print(f"\n{'='*65}")
    print(" MATRIZES DE CONFUSÃO POR BACIA — Col11 | Balanceamento v2")
    print(f"{'='*65}")
    print(f"  Entrada : {path_acc}")
    print(f"  Saída   : {path_out}")
    print(f"  Bacias  : {len(BACIAS_ALVO)}")

    configs = [
        {
            'label':      'nc10',
            'file':       'acc_col11_spatial_all_nc10_vers_1.csv',
            'remap_ref':  REMAP_REF_NC10,
            'remap_pred': REMAP_PRED_NC10,
            'classes':    [3, 4, 12, 15, 19, 21, 25, 29, 33],
        },
        {
            'label':      'nc7',
            'file':       'acc_col11_spatial_all_nc7_vers_1.csv',
            'remap_ref':  REMAP_REF_NC7,
            'remap_pred': REMAP_PRED_NC7,
            'classes':    [3, 4, 12, 21, 25, 33],
        },
    ]

    all_results = []

    for cfg in configs:
        fpath = os.path.join(path_acc, cfg['file'])
        print(f"\n>>> Carregando {cfg['file']}")
        df = pd.read_csv(fpath, low_memory=False)
        print(f"    {len(df)} pontos | {len(df.columns)} colunas")

        df_res = analyze_version(
            df,
            remap_ref    = cfg['remap_ref'],
            remap_pred   = cfg['remap_pred'],
            nc_label     = cfg['label'],
            classes_eval = cfg['classes'],
        )
        all_results.append(df_res)

    df_final = pd.concat(all_results, ignore_index=True)

    path_out_csv = os.path.join(path_out, 'erros_omissao_comissao.csv')
    df_final.to_csv(path_out_csv, index=False)

    print(f"\n{'='*65}")
    print(f"  Arquivo salvo : {path_out_csv}")
    print(f"  Registros     : {len(df_final)}")
    print(f"  Bacias        : {df_final[df_final['bacia'] != 'GLOBAL']['bacia'].nunique()}")
    print(f"{'='*65}")


if __name__ == '__main__':
    main()
