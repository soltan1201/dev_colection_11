#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformação
DISTRIBUIDO COM GPLv2
@author: geodatin

Analisa erros de área por bacia comparando Coleção 10 (Map100) vs Coleção 11 (spatial_all).
O erro é calculado como a SOMA das diferenças absolutas POR CLASSE em cada (bacia, ano),
normalizada pela área total da Col10 naquele ano e bacia ("erro de reclassificação").

Gera lista de bacias candidatas a reprocessamento (versão 2) com base em:
  1. Algum ano com erro_reclass > erro_reclass médio da série DESTA bacia
     AND erro_reclass > THRESHOLD_MIN_PERC (mínimo absoluto p/ evitar ruído numérico)
  2. Algum ano com erro_reclass > 10% da área total da Col10 naquele ano

O resultado final é a união com a lista gerada pela análise de acurácia (se disponível).
"""

import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm
tqdm.pandas()


THRESHOLD_10PCT      = 10.0    # 10% da área col10 como limiar de erro
THRESHOLD_MIN_PERC   = 0.5     # mínimo relevante: 0.5% da área col10 para acionar critério 1


def get_base_path():
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    return str(script_dir.parents[1])   # sobe 2 níveis: areas → validations → src


def load_area_df(path_csv):
    """Carrega CSV de área por classe, bacia e ano."""
    df = pd.read_csv(path_csv, usecols=["area", "classe", "id_bacia", "year"])
    df["id_bacia"] = df["id_bacia"].astype(str)
    df["classe"]   = df["classe"].astype(int)
    return df


def compute_reclassification_error(df_col10, df_col11, label):
    """
    Para cada (bacia, year), calcula:
      - area total col10
      - soma das abs(area_col10_classe - area_col11_classe) para classes comuns
      - erro_reclass_perc = erro / total_col10 * 100

    Retorna DataFrame detalhado e resumo por bacia.
    """
    # pivota: colunas = classes, linhas = (bacia, year)
    piv10 = df_col10.pivot_table(
        index=["id_bacia", "year"], columns="classe", values="area",
        aggfunc="sum", fill_value=0.0
    ).reset_index()

    piv11 = df_col11.pivot_table(
        index=["id_bacia", "year"], columns="classe", values="area",
        aggfunc="sum", fill_value=0.0
    ).reset_index()

    # classes presentes em cada coleção
    classes_col10 = set(df_col10["classe"].unique())
    classes_col11 = set(df_col11["classe"].unique())
    classes_comum = classes_col10 & classes_col11

    # --- área total col10 por bacia/ano ---
    total_col10 = (
        df_col10.groupby(["id_bacia", "year"])["area"]
        .sum().rename("area_total_col10").reset_index()
    )

    # --- merge piv10 e piv11 nos mesmos (bacia, year) ---
    merged = pd.merge(
        piv10, piv11,
        on=["id_bacia", "year"],
        how="inner",
        suffixes=("_c10", "_c11"),
    )

    # diferença absoluta somada por classe comum
    diff_cols = []
    for cls in sorted(classes_comum):
        c10_col = cls if (cls in piv10.columns and cls not in piv11.columns) else f"{cls}_c10"
        c11_col = cls if (cls in piv11.columns and cls not in piv10.columns) else f"{cls}_c11"
        # após merge com suffixes os nomes são <cls>_c10 e <cls>_c11 apenas quando há conflito
        # quando a coluna existe em só um DF, o merge preserva o nome original
        col_c10 = f"{cls}_c10" if f"{cls}_c10" in merged.columns else cls
        col_c11 = f"{cls}_c11" if f"{cls}_c11" in merged.columns else cls
        dcol = f"diff_cls{cls}"
        merged[dcol] = (merged[col_c10] - merged[col_c11]).abs()
        diff_cols.append(dcol)

    merged["erro_reclass_km2"] = merged[diff_cols].sum(axis=1)

    # junta área total col10
    merged = merged.merge(total_col10, on=["id_bacia", "year"])
    merged["erro_reclass_perc"] = (
        (merged["erro_reclass_km2"] / merged["area_total_col10"].replace(0, np.nan)) * 100
    ).round(4)

    # --- erro médio por bacia ---
    mean_err = (
        merged.groupby("id_bacia")["erro_reclass_perc"]
        .mean().rename("erro_medio_perc").reset_index()
    )
    merged = merged.merge(mean_err, on="id_bacia")

    # critério 1: erro > erro médio E erro > limiar mínimo de 0.5%
    merged["flag_maior_media"] = (
        (merged["erro_reclass_perc"] > merged["erro_medio_perc"]) &
        (merged["erro_reclass_perc"] > THRESHOLD_MIN_PERC)
    )
    # critério 2: erro > 10% da área col10
    merged["flag_maior_10pct"] = merged["erro_reclass_perc"] > THRESHOLD_10PCT
    merged["flag_ano"]         = merged["flag_maior_media"] | merged["flag_maior_10pct"]
    merged["nc"]               = label

    # --- resumo por bacia ---
    def sumariza(grp):
        return pd.Series({
            "n_anos_total":           len(grp),
            "n_anos_flag":            grp["flag_ano"].sum(),
            "n_anos_maior_media":     grp["flag_maior_media"].sum(),
            "n_anos_maior_10pct":     grp["flag_maior_10pct"].sum(),
            "erro_medio_perc":        round(grp["erro_reclass_perc"].mean(), 4),
            "erro_max_perc":          round(grp["erro_reclass_perc"].max(), 4),
            "erro_max_km2":           round(grp["erro_reclass_km2"].max(), 2),
            "flag_reprocess":         grp["flag_ano"].any(),
        })

    df_summary = (
        merged.groupby("id_bacia")[
            ["erro_reclass_perc", "erro_reclass_km2",
             "flag_ano", "flag_maior_media", "flag_maior_10pct"]
        ]
        .apply(sumariza, include_groups=False)
        .reset_index()
        .rename(columns={"id_bacia": "bacia"})
    )
    df_summary["nc"] = label

    detail_cols = [
        "id_bacia", "year", "area_total_col10", "erro_reclass_km2",
        "erro_reclass_perc", "erro_medio_perc",
        "flag_maior_media", "flag_maior_10pct", "flag_ano", "nc"
    ]
    df_detail = merged[[c for c in detail_cols if c in merged.columns]].rename(
        columns={"id_bacia": "bacia"}
    )

    return df_detail, df_summary


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    base_path  = get_base_path()
    path_areas = os.path.join(base_path, "dados", "areasCol11")
    path_out   = os.path.join(base_path, "dados", "reprocess_v2")
    os.makedirs(path_out, exist_ok=True)

    print(f"\n{'='*60}")
    print(" ANÁLISE DE ERROS DE ÁREA: Coleção 10 vs Coleção 11 por Bacia")
    print(f"{'='*60}")
    print(f"  Dados de entrada      : {path_areas}")
    print(f"  Saída                 : {path_out}")
    print(f"  Limiar erro 10%       : {THRESHOLD_10PCT}% da área Col10")
    print(f"  Limiar mín. relevante : {THRESHOLD_MIN_PERC}% da área Col10")
    print(f"  Método                : soma de dif. absolutas por classe (reclassificação)\n")

    configs = [
        {
            "label":     "nc10",
            "file_col10": "areaXclasse_CAATINGA_Col11.0_Map100_nc10_vers_10_remap.csv",
            "file_col11": "areaXclasse_CAATINGA_Col11.0_spatial_all_nc10_vers_1.csv",
        },
        {
            "label":     "nc7",
            "file_col10": "areaXclasse_CAATINGA_Col11.0_Map100_nc7_vers_10_remap.csv",
            "file_col11": "areaXclasse_CAATINGA_Col11.0_spatial_all_nc7_vers_1.csv",
        },
    ]

    all_details   = []
    all_summaries = []

    for cfg in configs:
        label = cfg["label"]
        print(f"\n>>> Processando {label}")

        path10 = os.path.join(path_areas, cfg["file_col10"])
        path11 = os.path.join(path_areas, cfg["file_col11"])

        print(f"  Carregando Col10: {cfg['file_col10']}")
        df10 = load_area_df(path10)

        print(f"  Carregando Col11: {cfg['file_col11']}")
        df11 = load_area_df(path11)

        # usa apenas anos comuns (col10 vai até 2024, col11 até 2025)
        years_common = set(df10["year"].unique()) & set(df11["year"].unique())
        df10 = df10[df10["year"].isin(years_common)]
        df11 = df11[df11["year"].isin(years_common)]

        print(f"  Anos comuns: {min(years_common)}–{max(years_common)} ({len(years_common)} anos)")
        print(f"  Bacias: {df10['id_bacia'].nunique()}")
        print(f"  Classes Col10: {sorted(df10['classe'].unique())}")
        print(f"  Classes Col11: {sorted(df11['classe'].unique())}")

        df_detail, df_summary = compute_reclassification_error(df10, df11, label)
        all_details.append(df_detail)
        all_summaries.append(df_summary)

        n_flag = df_summary["flag_reprocess"].sum()
        print(f"\n  Bacias com erro de área [{label}]: {n_flag}/{len(df_summary)}")
        if n_flag > 0:
            bad = df_summary[df_summary["flag_reprocess"]].sort_values("erro_max_perc", ascending=False)
            print(bad[["bacia","n_anos_flag","erro_medio_perc","erro_max_perc","erro_max_km2",
                        "n_anos_maior_media","n_anos_maior_10pct"]].to_string(index=False))
        else:
            top = df_summary.sort_values("erro_max_perc", ascending=False).head(5)
            print("  (nenhuma bacia atingiu o limiar — top 5 maiores erros:)")
            print(top[["bacia","erro_medio_perc","erro_max_perc","erro_max_km2"]].to_string(index=False))

    # consolida resultados
    df_all_detail  = pd.concat(all_details,   ignore_index=True)
    df_all_summary = pd.concat(all_summaries, ignore_index=True)

    flagged_area = df_all_summary[df_all_summary["flag_reprocess"]]["bacia"].unique()
    df_all_summary["flag_any_nc"] = df_all_summary["bacia"].isin(flagged_area)

    df_reprocess_area = (
        df_all_summary[df_all_summary["flag_reprocess"]]
        .drop_duplicates(subset="bacia")
        .sort_values("erro_max_perc", ascending=False)
        [["bacia","nc","n_anos_flag","erro_medio_perc","erro_max_perc",
          "erro_max_km2","n_anos_maior_media","n_anos_maior_10pct"]]
    )

    # --- tenta combinar com lista de acurácia (se existir) ---
    path_acc_list = os.path.join(path_out, "bacias_reprocess_v2_acuracia.csv")
    if os.path.exists(path_acc_list):
        df_acc_list      = pd.read_csv(path_acc_list)
        flagged_acc      = set(df_acc_list["bacia"].astype(str).unique())
        flagged_area_set = set(df_reprocess_area["bacia"].astype(str).unique())
        flagged_total    = flagged_acc | flagged_area_set

        print(f"\n  Bacias por acurácia    : {len(flagged_acc)}")
        print(f"  Bacias por área        : {len(flagged_area_set)}")
        print(f"  União total (únicos)   : {len(flagged_total)}")

        df_final = pd.DataFrame({
            "bacia":          sorted(flagged_total),
            "flag_acuracia":  [b in flagged_acc      for b in sorted(flagged_total)],
            "flag_area":      [b in flagged_area_set for b in sorted(flagged_total)],
        })
        path_final = os.path.join(path_out, "bacias_reprocess_v2_FINAL.csv")
        df_final.to_csv(path_final, index=False)
        print(f"\n  Lista final salva em  : {path_final}")
        print(df_final.to_string(index=False))
    else:
        print(f"\n  (Lista de acurácia não encontrada — execute analise_acuracia_col10_vs_col11.py primeiro)")

    # salva tabelas de área
    path_area_detail  = os.path.join(path_out, "erros_area_col10_vs_col11_por_bacia_ano.csv")
    path_area_summary = os.path.join(path_out, "erros_area_col10_vs_col11_por_bacia.csv")
    path_area_list    = os.path.join(path_out, "bacias_reprocess_v2_area.csv")

    df_all_detail.to_csv(path_area_detail,   index=False)
    df_all_summary.to_csv(path_area_summary, index=False)
    df_reprocess_area.to_csv(path_area_list, index=False)

    print(f"\n{'='*60}")
    print(f" RESULTADO FINAL — ANÁLISE DE ERRO DE ÁREA")
    print(f"{'='*60}")
    print(f"  Bacias candidatas a reprocessamento (área): {len(df_reprocess_area)}")
    if len(df_reprocess_area) > 0:
        print(f"  Lista: {sorted(df_reprocess_area['bacia'].tolist())}")
    print(f"\n  Arquivos salvos:")
    print(f"    {path_area_detail}")
    print(f"    {path_area_summary}")
    print(f"    {path_area_list}")
