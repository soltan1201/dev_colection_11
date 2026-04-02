# CLAUDE.md — MapBiomas Coleção 11 / Caatinga

Contexto permanente para o assistente IA neste repositório.

---

## Objetivo do projeto

Pipeline de classificação de uso e cobertura do solo (LULC) para o **bioma Caatinga**, coleção 11 do MapBiomas. Cobre **49 bacias hidrográficas** e **41 anos** (1985–2025) usando Google Earth Engine (GEE) + scikit-learn local.

---

## Estrutura de diretórios

```
src/
├── classification_process/   # Scripts principais de classificação GEE
├── samples_process/          # Coleta e exportação de ROIs (amostras)
├── features_process/         # Seleção e limpeza de features (sklearn local)
├── extraFM/                  # Export de feature maps
├── savana_cerr_caat/         # Scripts específicos para separação Savana/Cerrado/Caatinga
├── validations/areas/        # Métricas de validação e análise de área
├── utis_scripts/             # Utilitários: mover assets, deletar, monitorar tasks
├── show_mapas/               # Scripts JS de visualização no GEE Code Editor
├── destaques/                # Scripts JS de destaques visuais
├── dados/                    # JSONs de configuração (hiperparâmetros, feature selection)
├── gee_tools.py              # Funções utilitárias GEE (tasks, assets, contas)
└── configure_account_projects_ee.py  # Mapeamento de contas GEE
```

---

## Contas GEE disponíveis

| Alias | Projeto GEE |
|---|---|
| `caatinga01` | `mapbiomas-caatinga-cloud` |
| `caatinga02` | `mapbiomas-caatinga-cloud02` |
| `caatinga03` | `mapbiomas-caatinga-cloud03` |
| `caatinga04` | `mapbiomas-caatinga-cloud04` |
| `caatinga05` | `mapbiomas-caatinga-cloud05` |
| `solkanGeodatin` | `geo-data-s` |
| `superconta` | `mapbiomas-brazil` |

---

## Assets GEE principais

```
# Bacias (geometria)
projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions

# Grade base
projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/basegrade30KMCaatinga

# ROIs col11 (amostras de treinamento)
projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesCCred
projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_clean_downsamplesv1CC

# Saída das classificações col11
projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1

# Coleção Landsat
LANDSAT/COMPOSITES/C02/T1_L2_32DAY
```

---

## Convenção de nomes dos assets de saída

```
BACIA_{nbacia}_{nyear}_GTB_col11_BND_fm-v_{VERSION}
# Exemplo: BACIA_751_2020_GTB_col11_BND_fm-v_1
```

Os ROIs seguem:
```
rois_fromBasin_{nbacia}_{ano}    # amostras por bacia
rois_fromGrade_{bacia}_{year}    # amostras por grade
```

---

## Parâmetros do classificador (GEE)

```python
PMT_GTB = {
    'numberOfTrees': 35,
    'shrinkage':     0.1,
    'samplingRate':  0.65,
    'loss':          'LeastSquares',
    'seed':          0,
}
# Classificador: ee.Classifier.smileGradientTreeBoost
# Parâmetros por bacia em: src/dados/dictBetterModelpmtCol10v1.json
```

---

## Features ativas (LST_FEAT_SELECT — versão atual VC4)

```python
# Bandas espectrais brutas (períodos: median, wet=jan-jul, dry=ago-dez)
'green_median_dry',  'green_median_wet',
'red_median_dry',    'red_median_wet',    'swir1_median_wet',
'swir2_median_wet',  'swir2_median',      'swir2_median_dry',

# Índices espectrais
'ndti_median_dry',   'ndti_median_wet',   'ndti_median',
'gli_median_dry',
'ndvi_median',       'ndvi_median_dry',   'ndvi_median_wet',
'ndwi_median_dry',   'ndwi_median',       'ndwi_median_wet',
'awei_median',       'awei_median_wet',   'awei_median_dry',

# Comentadas/desativadas: brba, mbi, shape, gcvi, bsi, ui
```

Fórmulas completas de todos os índices: `src/classification_process/arqParametros_class.py` → `FORMULAS_INDICES_ESPECTRAIS`

---

## Período de dados

- Anos: **1985 a 2025**
- Para ano > 2024, usa amostras de 2024 (`ano_amostra = min(nyear, 2024)`)

---

## Bacias (49 total)

```python
nameBacias = [
    '765', '7544', '7541', '7411', '746', '7591', '7592',
    '761111', '761112', '7612', '7613', '7614', '7615',
    '771', '7712', '772', '7721', '773', '7741', '7746', 
    '7754', '7761', '7764', '7691', '7581', '7625', '7584', 
    '751','752', '7616', '745', '7424', '7618', '7561', 
    '755', '7617', '7564', '7422', '76116', '7671', '757', 
    '766', '753', '764', '7619', '7443', '7438', '763', 
    '7622'
]
```

---

## Fluxo do pipeline

```
1. samples_process/   → Coletar ROIs por grade/bacia no GEE → exportar para assets
2. features_process/  → Limpeza, downsampling, seleção RFECV (sklearn local)
                        → gera feat_sel_{bacia}_{ano}.json em FS_col11_json/
3. classification_process/ → Mosaico Landsat → índices → treinar GTB → exportar classificação
4. validations/       → Métricas de área, concordância, análise de incidentes
```

---

## Arquivos de configuração importantes

| Arquivo | Conteúdo |
|---|---|
| `src/dados/dictBetterModelpmtCol10v1.json` | Hiperparâmetros GTB por bacia |
| `src/features_process/FS_col11_json/*.json` | Features selecionadas por bacia/ano |
| `src/features_process/dict_basin_neigbor.json` | Bacias vizinhas (para ROIs extras) |
| `src/samples_process/dict_basin_49_lista_grades.json` | Grades por bacia |
| `src/classification_process/arqParametros_class.py` | Fórmulas de índices, classes, amostras |

---

## Padrões e convenções do código

- Scripts Python rodam via CLI: `python script.py pos_inicio pos_fim` (fatiamento da lista de bacias)
- Scripts JS (`.js`) são para teste/visualização no **GEE Code Editor**
- `knowMapSaved = False` → modo que varre `ASSET_OUT` para detectar anos faltantes
- Para identificar assets de uma bacia em `lst_bacias_saved`, usar `f'BACIA_{nbacia}_'` (com underscore no final) para evitar match parcial entre bacias como `765` e `7651`
- `arqParametros_class.py` é o módulo central de parâmetros para classificação

---

## Dependências Python

```
earthengine-api
pandas
numpy
scikit-learn (RFECV, GradientBoosting, StratifiedKFold)
tqdm
tabulate
matplotlib
```
