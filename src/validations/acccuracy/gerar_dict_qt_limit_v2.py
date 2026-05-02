#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Produzido por Geodatin - Dados e Geoinformação
DISTRIBUIDO COM GPLv2
@author: geodatin

Combina erros de omissão/comissão (Script 1) e contagem real de amostras
(Script 2) para gerar dictQtLimit otimizado por bacia — versão 2.

Entradas:
  src/dados/reprocess_v2/erros_omissao_comissao.csv
  src/dados/reprocess_v2/amostras_por_classe_bacia.csv  (opcional)

Saída:
  src/dados/reprocess_v2/dict_qt_limit_v2_por_bacia.json
  src/dados/reprocess_v2/resumo_balanceamento_v2.csv

Lógica de ajuste:
  erro_medio = (omissao + comissao) / 2
  fator = 1.0 + ALPHA * (erro_medio - PIVOT) / PIVOT
  fator = clip(fator, FATOR_MIN, FATOR_MAX)
  novo_limite = round(limite_base * fator / ROUND_STEP) * ROUND_STEP
  novo_limite = clip(novo_limite, MIN_POR_CLASSE[c], MAX_POR_CLASSE[c])
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from pathlib import Path
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

# Limites de referência atuais (v1) — baseline do ajuste
DICT_QT_LIMIT_V1 = {
    3:  520,
    4:  1200,
    12: 300,
    15: 970,
    19: 180,
    21: 200,
    22: 400,
    29: 200,
    33: 100,
    36: 260,
}

# Limites absolutos por classe: (min, max)
LIMITES_ABS = {
    3:  (300,  1800),
    4:  (600,  2400),
    12: (150,   600),
    15: (500,  2000),
    19: ( 80,   400),
    21: (150,  1500),
    22: (200,   800),
    29: (100,   450),
    33: ( 50,   250),
    36: (100,   500),
}

# Parâmetros da fórmula de ajuste
ALPHA      = 0.6    # magnitude do ajuste (0.6 = ±60% quando erro dobra/zera em relação ao pivot)
PIVOT      = 20.0   # erro médio (%) que não provoca ajuste
FATOR_MIN  = 0.60   # faz reduzir no máximo 40%
FATOR_MAX  = 1.80   # faz aumentar no máximo 80%
ROUND_STEP = 50     # arredondamento para múltiplos de 50

# Mínimo de pontos de validação por classe/bacia para usar análise per-basin
MIN_PTS_PER_CLASS_BACIA = 30

# Nome legível das classes
NOME_CLASSES = {
    3: 'Floresta',  4: 'Savana',  12: 'Campo', 15: 'Pastagem',
    19: 'L.Anual', 21: 'Agropast', 22: 'N.Veget', 29: 'Aflor.Roch',
    33: 'Água',    36: 'Outra.Cult',
}


# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------

def get_base_path():
    script_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    return str(script_dir.parents[1])


def calc_novo_limite(limite_base, omissao, comissao):
    """Aplica fórmula de ajuste e retorna novo limite arredondado."""
    if np.isnan(omissao) or np.isnan(comissao):
        return limite_base
    erro_medio = (omissao + comissao) / 2.0
    fator = 1.0 + ALPHA * (erro_medio - PIVOT) / PIVOT
    fator = np.clip(fator, FATOR_MIN, FATOR_MAX)
    novo  = round(limite_base * fator / ROUND_STEP) * ROUND_STEP
    return int(novo)


def apply_limites_abs(classe, valor):
    lo, hi = LIMITES_ABS.get(classe, (50, 3000))
    return int(np.clip(valor, lo, hi))


def build_dict_bacia(nbacia, df_errors, df_samples):
    """
    Constrói dictQtLimit para uma bacia específica.

    Prioridade:
    1. Se bacia tem dados de validação com pts >= MIN_PTS_PER_CLASS_BACIA → usa per-basin errors
    2. Caso contrário, usa erros GLOBAL

    Se há dados de amostras reais (df_samples), verifica se o limite proposto
    é viável (não pode exceder o que existe no asset × margem de segurança 0.9).
    """
    result = {}
    resumo = []

    # erros globais (nc10 como principal)
    df_global = df_errors[(df_errors['bacia'] == 'GLOBAL') & (df_errors['nc'] == 'nc10')]
    # erros por bacia (nc10)
    df_bacia  = df_errors[(df_errors['bacia'] == str(nbacia)) & (df_errors['nc'] == 'nc10')]

    amostras_bacia = {}
    if df_samples is not None and str(nbacia) in df_samples.index:
        amostras_bacia = df_samples.loc[str(nbacia)].to_dict()

    for cls in sorted(DICT_QT_LIMIT_V1.keys()):
        limite_base = DICT_QT_LIMIT_V1[cls]

        # Tenta usar erros per-basin se tiver dados suficientes
        row_b = df_bacia[df_bacia['classe'] == cls]
        usar_bacia = (
            not row_b.empty and
            row_b.iloc[0]['total_ref'] >= MIN_PTS_PER_CLASS_BACIA
        )

        if usar_bacia:
            omissao  = row_b.iloc[0]['omissao']
            comissao = row_b.iloc[0]['comissao']
            fonte    = 'bacia'
        else:
            row_g = df_global[df_global['classe'] == cls]
            if row_g.empty:
                omissao  = np.nan
                comissao = np.nan
            else:
                omissao  = row_g.iloc[0]['omissao']
                comissao = row_g.iloc[0]['comissao']
            fonte = 'global'

        novo_limite = calc_novo_limite(limite_base, omissao, comissao)
        novo_limite = apply_limites_abs(cls, novo_limite)

        # Restringe ao máximo disponível no asset (com margem de 10%)
        qt_asset = amostras_bacia.get(str(cls), 0)
        if qt_asset > 0:
            max_asset = int(qt_asset * 0.90)
            if novo_limite > max_asset:
                novo_limite = max(apply_limites_abs(cls, max_asset), LIMITES_ABS[cls][0])

        result[str(cls)] = novo_limite
        resumo.append({
            'bacia':        nbacia,
            'classe':       cls,
            'nome':         NOME_CLASSES.get(cls, str(cls)),
            'limite_v1':    limite_base,
            'omissao':      round(omissao, 2) if not np.isnan(omissao) else np.nan,
            'comissao':     round(comissao, 2) if not np.isnan(comissao) else np.nan,
            'fator_usado':  fonte,
            'qt_asset':     qt_asset,
            'limite_v2':    novo_limite,
            'delta_pct':    round((novo_limite / limite_base - 1) * 100, 1),
        })

    return result, pd.DataFrame(resumo)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    base_path = get_base_path()
    path_rv2  = os.path.join(base_path, 'dados', 'reprocess_v2')
    os.makedirs(path_rv2, exist_ok=True)

    print(f"\n{'='*65}")
    print(" GERAÇÃO dictQtLimit v2 por bacia — Balanceamento de Amostras")
    print(f"{'='*65}")

    # Carrega erros de omissão/comissão
    path_erros = os.path.join(path_rv2, 'erros_omissao_comissao.csv')
    if not os.path.exists(path_erros):
        print(f"\n[ERRO] Arquivo não encontrado: {path_erros}")
        print("  Execute primeiro: python analise_conf_matrix_balanceamento.py")
        sys.exit(1)
    df_errors = pd.read_csv(path_erros)
    print(f"\n  Erros carregados: {path_erros}  ({len(df_errors)} registros)")

    # Carrega amostras reais (opcional)
    path_amostras = os.path.join(path_rv2, 'amostras_por_classe_bacia.csv')
    df_samples = None
    if os.path.exists(path_amostras):
        df_raw = pd.read_csv(path_amostras)
        df_raw['bacia'] = df_raw['bacia'].astype(str)
        df_raw = df_raw.set_index('bacia')
        # Renomeia colunas cls_3 → '3' etc para acesso uniforme
        rename_map = {f'cls_{c}': str(c) for c in DICT_QT_LIMIT_V1.keys()}
        df_samples = df_raw.rename(columns=rename_map)
        print(f"  Amostras carregadas: {path_amostras}  ({len(df_samples)} bacias)")
    else:
        print(f"  [AVISO] Amostras GEE não encontradas ({path_amostras})")
        print("           Execute get_amostras_asset_gee.py para dados mais precisos.")
        print("           Continuando apenas com os erros de omissão/comissão...\n")

    # Exibe resumo global antes do ajuste
    df_global_nc10 = df_errors[(df_errors['bacia'] == 'GLOBAL') & (df_errors['nc'] == 'nc10')]
    if not df_global_nc10.empty:
        print(f"\n{'='*65}")
        print(" ERROS GLOBAIS nc10 (base do ajuste para bacias sem dados suficientes)")
        print(f"{'='*65}")
        print(tabulate(
            df_global_nc10[['nome', 'total_ref', 'prod_acc', 'user_acc', 'omissao', 'comissao']],
            headers='keys', tablefmt='psql', showindex=False
        ))

    # Gera dicionário por bacia
    dict_v2     = {}
    all_resumo  = []

    for nbacia in BACIAS_ALVO:
        dict_bacia, df_resumo_b = build_dict_bacia(nbacia, df_errors, df_samples)
        dict_v2[nbacia] = dict_bacia
        all_resumo.append(df_resumo_b)

    df_resumo_final = pd.concat(all_resumo, ignore_index=True)

    # Salva JSON
    path_json = os.path.join(path_rv2, 'dict_qt_limit_v2_por_bacia.json')
    with open(path_json, 'w') as f:
        json.dump(dict_v2, f, indent=2, ensure_ascii=False)
    print(f"\n  JSON salvo: {path_json}")

    # Salva resumo CSV
    path_res_csv = os.path.join(path_rv2, 'resumo_balanceamento_v2.csv')
    df_resumo_final.to_csv(path_res_csv, index=False)
    print(f"  Resumo salvo: {path_res_csv}")

    # Imprime comparativo global (médias entre bacias)
    print(f"\n{'='*65}")
    print(" COMPARATIVO v1 vs v2 — médias entre bacias-alvo")
    print(f"{'='*65}")
    comp = df_resumo_final.groupby('classe').agg(
        nome        = ('nome',       'first'),
        limite_v1   = ('limite_v1',  'first'),
        limite_v2   = ('limite_v2',  'mean'),
        omissao_med = ('omissao',    'mean'),
        comissao_med= ('comissao',   'mean'),
        delta_pct   = ('delta_pct',  'mean'),
    ).reset_index()
    comp['limite_v2']    = comp['limite_v2'].round(0).astype(int)
    comp['omissao_med']  = comp['omissao_med'].round(1)
    comp['comissao_med'] = comp['comissao_med'].round(1)
    comp['delta_pct']    = comp['delta_pct'].round(1)
    print(tabulate(comp, headers='keys', tablefmt='psql', showindex=False))

    # Imprime dicionário v2 por bacia (amostra das primeiras 5)
    print(f"\n{'='*65}")
    print(" DICT_QT_LIMIT_V2 — primeiras 5 bacias")
    print(f"{'='*65}")
    for b in BACIAS_ALVO[:5]:
        print(f"  '{b}': {dict_v2[b]}")

    print(f"\n  Total de bacias: {len(dict_v2)}")
    print(f"  Parâmetros: ALPHA={ALPHA} | PIVOT={PIVOT}% | ROUND={ROUND_STEP}")


if __name__ == '__main__':
    main()
