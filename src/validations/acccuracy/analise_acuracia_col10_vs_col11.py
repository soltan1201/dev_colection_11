#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformação
DISTRIBUIDO COM GPLv2
@author: geodatin

Analisa acuracia por bacia comparando Coleção 10 (Map100) vs Coleção 11 (spatial_all).
Gera lista de bacias candidatas a reprocessamento (versão 2) quando a acuracia da
Col11 é inferior à da Col10 em qualquer número de classes (nc10 ou nc7).
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
tqdm.pandas()


# ---------------------------------------------------------------------------
# Mapeamento de rótulos de referência (CLASS_YYYY) para código numérico
# nc10: 10 classes  |  nc7: 7 classes
# ---------------------------------------------------------------------------
DICT_REMAP_NC10 = {
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

DICT_REMAP_NC7 = {
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
    "AFLORAMENTO ROCHOSO":           25,
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

# classes a excluir da análise (sem dados ou não observado)
CLASSES_EXCLUDE = {0, 27}

# anos comuns entre col10 (1985-2024) e col11 (1985-2025)
YEARS = list(range(1985, 2025))


def get_base_path():
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    return str(script_dir.parents[1])   # sobe 2 níveis: acccuracy → validations → src


def remap_reference_cols(df, dict_remap, years):
    """Converte colunas CLASS_YYYY de texto para código numérico in-place."""
    for y in years:
        col = f"CLASS_{y}"
        if col in df.columns:
            df[col] = df[col].map(dict_remap)
    return df


def accuracy_per_basin_year(df_ref_col10, df_ref_col11, years, label):
    """
    Calcula acurácia por bacia e ano para col10 e col11.
    df_ref_col10 e df_ref_col11 já têm CLASS_YYYY como numérico.
    Retorna DataFrame com colunas [bacia, year, acc_col10, acc_col11, delta_acc, nc].
    """
    # garante que 'bacia' é string para consistência
    df_ref_col10 = df_ref_col10.copy()
    df_ref_col11 = df_ref_col11.copy()
    df_ref_col10["bacia"] = df_ref_col10["bacia"].astype(str)
    df_ref_col11["bacia"] = df_ref_col11["bacia"].astype(str)

    lst_bacias = sorted(df_ref_col10["bacia"].unique())
    rows = []

    for nbacia in tqdm(lst_bacias, desc=f"  Acurácia por bacia [{label}]"):
        sub10 = df_ref_col10[df_ref_col10["bacia"] == nbacia]
        sub11 = df_ref_col11[df_ref_col11["bacia"] == nbacia]

        for year in years:
            col_ref  = f"CLASS_{year}"
            col_pred = f"classification_{year}"

            if col_ref not in sub10.columns or col_pred not in sub10.columns:
                continue

            # col10
            tmp10 = sub10[[col_ref, col_pred]].dropna()
            tmp10 = tmp10[~tmp10[col_pred].isin(CLASSES_EXCLUDE)]
            tmp10 = tmp10[~tmp10[col_ref].isin(CLASSES_EXCLUDE)]
            acc10 = (tmp10[col_ref] == tmp10[col_pred]).mean() * 100 if len(tmp10) > 0 else np.nan

            # col11 (mesmos pontos de referência, predições diferentes)
            if col_pred not in sub11.columns:
                acc11 = np.nan
            else:
                tmp11 = sub11[[col_ref, col_pred]].dropna()
                tmp11 = tmp11[~tmp11[col_pred].isin(CLASSES_EXCLUDE)]
                tmp11 = tmp11[~tmp11[col_ref].isin(CLASSES_EXCLUDE)]
                acc11 = (tmp11[col_ref] == tmp11[col_pred]).mean() * 100 if len(tmp11) > 0 else np.nan

            rows.append({
                "bacia":    nbacia,
                "year":     year,
                "acc_col10": round(acc10, 3) if not np.isnan(acc10) else np.nan,
                "acc_col11": round(acc11, 3) if not np.isnan(acc11) else np.nan,
                "delta_acc": round(acc11 - acc10, 3) if not (np.isnan(acc10) or np.isnan(acc11)) else np.nan,
                "nc":        label,
            })

    return pd.DataFrame(rows)


def flag_basins_by_accuracy(df_acc_years):
    """
    Agrega por bacia e sinaliza as que perderam acurácia na Col11 vs Col10.
    Critério: média da acurácia Col11 < média da acurácia Col10 na série temporal.
    """
    grp = df_acc_years.groupby(["bacia", "nc"]).agg(
        mean_acc_col10 = ("acc_col10", "mean"),
        mean_acc_col11 = ("acc_col11", "mean"),
        min_delta      = ("delta_acc", "min"),
        mean_delta     = ("delta_acc", "mean"),
        n_years_worse  = ("delta_acc", lambda x: (x < 0).sum()),
        n_years_total  = ("delta_acc", "count"),
    ).reset_index()

    grp["mean_acc_col10"] = grp["mean_acc_col10"].round(3)
    grp["mean_acc_col11"] = grp["mean_acc_col11"].round(3)
    grp["mean_delta"]     = grp["mean_delta"].round(3)
    grp["min_delta"]      = grp["min_delta"].round(3)
    grp["flag_reprocess"] = grp["mean_acc_col11"] < grp["mean_acc_col10"]

    return grp


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    base_path = get_base_path()
    path_acc  = os.path.join(base_path, "dados", "ptosAccCol11")
    path_out  = os.path.join(base_path, "dados", "reprocess_v2")
    os.makedirs(path_out, exist_ok=True)

    print(f"\n{'='*60}")
    print(" ANÁLISE DE ACURÁCIA: Coleção 10 vs Coleção 11 por Bacia")
    print(f"{'='*60}")
    print(f"  Dados de entrada : {path_acc}")
    print(f"  Saída            : {path_out}\n")

    configs = [
        {
            "label":     "nc10",
            "file_col10": "acc_col11_Map100_nc10_vers_1.csv",
            "file_col11": "acc_col11_spatial_all_nc10_vers_1.csv",
            "remap":      DICT_REMAP_NC10,
        },
        {
            "label":     "nc7",
            "file_col10": "acc_col11_Map100_nc7_vers_10.csv",
            "file_col11": "acc_col11_spatial_all_nc7_vers_1.csv",
            "remap":      DICT_REMAP_NC7,
        },
    ]

    all_acc_years  = []
    all_flags      = []

    for cfg in configs:
        label = cfg["label"]
        print(f"\n>>> Processando {label}")

        path10 = os.path.join(path_acc, cfg["file_col10"])
        path11 = os.path.join(path_acc, cfg["file_col11"])

        print(f"  Carregando Col10: {cfg['file_col10']}")
        df10 = pd.read_csv(path10)
        print(f"  Carregando Col11: {cfg['file_col11']}")
        df11 = pd.read_csv(path11)

        print(f"  Pontos: {len(df10)} (col10) | {len(df11)} (col11)")

        # converte referência textual → numérica
        print("  Remapeando classes de referência (texto → numérico) ...")
        df10 = remap_reference_cols(df10, cfg["remap"], YEARS)
        df11 = remap_reference_cols(df11, cfg["remap"], YEARS)

        # calcula acurácia por bacia/ano
        df_acc = accuracy_per_basin_year(df10, df11, YEARS, label)
        all_acc_years.append(df_acc)

        # sinaliza bacias com regressão de acurácia
        df_flag = flag_basins_by_accuracy(df_acc)
        all_flags.append(df_flag)

        n_flagged = df_flag["flag_reprocess"].sum()
        print(f"  Bacias com perda de acurácia [{label}]: {n_flagged}/{len(df_flag)}")
        if n_flagged > 0:
            bad = df_flag[df_flag["flag_reprocess"]].sort_values("mean_delta")
            print(bad[["bacia","mean_acc_col10","mean_acc_col11","mean_delta","n_years_worse"]].to_string(index=False))

    # junta todos os resultados
    df_all_acc   = pd.concat(all_acc_years,  ignore_index=True)
    df_all_flags = pd.concat(all_flags,      ignore_index=True)

    # lista unificada: bacia é candidata se foi sinalizada em qualquer nc
    flagged_union = df_all_flags[df_all_flags["flag_reprocess"]]["bacia"].unique()
    df_all_flags["flag_any_nc"] = df_all_flags["bacia"].isin(flagged_union)

    # gera lista final de bacias para reprocessar (versão 2)
    df_reprocess = (
        df_all_flags[df_all_flags["flag_reprocess"]][["bacia", "nc", "mean_acc_col10",
                                                       "mean_acc_col11", "mean_delta",
                                                       "n_years_worse", "n_years_total"]]
        .drop_duplicates(subset="bacia")
        .sort_values("mean_delta")
    )

    # salva tabelas
    path_acc_detail  = os.path.join(path_out, "acuracia_col10_vs_col11_por_bacia_ano.csv")
    path_acc_summary = os.path.join(path_out, "acuracia_col10_vs_col11_por_bacia.csv")
    path_reprocess   = os.path.join(path_out, "bacias_reprocess_v2_acuracia.csv")

    df_all_acc.to_csv(path_acc_detail,  index=False)
    df_all_flags.to_csv(path_acc_summary, index=False)
    df_reprocess.to_csv(path_reprocess,  index=False)

    print(f"\n{'='*60}")
    print(f" RESULTADO FINAL — ANÁLISE DE ACURÁCIA")
    print(f"{'='*60}")
    print(f"  Bacias candidatas a reprocessamento (acurácia): {len(df_reprocess)}")
    print(f"  Lista: {sorted(df_reprocess['bacia'].tolist())}")
    print(f"\n  Arquivos salvos:")
    print(f"    {path_acc_detail}")
    print(f"    {path_acc_summary}")
    print(f"    {path_reprocess}")
