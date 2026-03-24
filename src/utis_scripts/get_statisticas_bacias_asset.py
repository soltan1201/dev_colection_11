import ee
import os
import sys
from pathlib import Path

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account

projAccount = get_current_account()
print(f"Projeto selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project=projAccount)
    print('✅ Earth Engine inicializado com sucesso!')
except Exception as e:
    print(f"❌ Erro ao inicializar o GEE: {e}")
    sys.exit()

# =========================================================================
# PARÂMETROS E ASSETS
# =========================================================================
assetMapbiomas100 = 'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_integration_v2'
asset_bacias = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions'

# Asset de saída
asset_saida = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/estatisticas_bacias_1985_2024'

classMapB = [3, 4, 5, 9, 12, 13, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32, 33, 36, 39, 40, 41, 46, 47, 48, 49, 50, 62, 75]
classNew  = [3, 4, 3, 3, 12, 12, 15, 19, 19, 19, 21, 25, 25, 25, 25, 33, 29, 25, 33, 12, 33, 36, 19, 19, 19, 36, 36, 36,  4, 12, 19, 25]

# Extrai a lista de classes únicas que esperamos no resultado
classes_unicas = sorted(list(set(classNew)))

ano_inicial = 1985
ano_final = 2024

# =========================================================================
# LÓGICA 100% SERVER-SIDE E VETORIZADA
# =========================================================================
bacias = ee.FeatureCollection(asset_bacias)
mapbiomas = ee.Image(assetMapbiomas100)

# A imagem de área se adapta automaticamente à escala solicitada no reduceRegions
area_img = ee.Image.pixelArea().divide(10000) # Hectares

# 1. Função para criar uma imagem Multi-Banda por ano
def criar_imagem_anual(ano_ee):
    ano_ee = ee.Number(ano_ee).toInt()
    band_name = ee.String('classification_').cat(ano_ee.format('%d'))
    
    img_ano = mapbiomas.select([band_name]).remap(classMapB, classNew).rename('class')
    
    bandas_list = []
    for c in classes_unicas:
        banda_classe = img_ano.eq(c).multiply(area_img).rename(f'class_{c}_ha')
        bandas_list.append(banda_classe)
        
    banda_total = img_ano.gt(0).multiply(area_img).rename('total_ha')
    bandas_list.append(banda_total)
    
    return ee.Image.cat(bandas_list).set('year', ano_ee)

anos_lista = ee.List.sequence(ano_inicial, ano_final)
img_col = ee.ImageCollection(anos_lista.map(criar_imagem_anual))

# 2. Função para extrair estatísticas e calcular porcentagem
def extrair_estatisticas(img):
    ano = img.get('year')
    
    # =========================================================
    # OTIMIZAÇÃO AQUI: scale=300 para redução de 99% do esforço
    # =========================================================
    stats = img.reduceRegions(
        collection=bacias,
        reducer=ee.Reducer.sum(),
        scale=300, # <--- Escala grosseira para cálculo rápido de proporções
        tileScale=4 # Podemos até reduzir o tileScale já que a escala subiu tanto
    )
    
    def calcular_pct(feat):
        feat = feat.set('year', ano)
        total_ha = ee.Number(feat.get('total_ha'))
        
        total_seguro = ee.Algorithms.If(total_ha.eq(0), 1, total_ha)
        
        for c in classes_unicas:
            area_ha = ee.Number(feat.get(f'class_{c}_ha'))
            pct = area_ha.divide(total_seguro).multiply(100)
            feat = feat.set(f'class_{c}_pct', pct)
            
        return feat
        
    return stats.map(calcular_pct)

colecao_final = img_col.map(extrair_estatisticas).flatten()

# =========================================================================
# EXPORTAÇÃO PARA ASSET (TABLE)
# =========================================================================
nome_tarefa = f'Export_Stats_Bacias_Fast_300m_{ano_inicial}_{ano_final}'

try:
    ee.data.deleteAsset(asset_saida)
except:
    pass

tarefa = ee.batch.Export.table.toAsset(
    collection=colecao_final,
    description=nome_tarefa,
    assetId=asset_saida
)

tarefa.start()

print("\n" + "="*60)
print(f"🚀 Tarefa '{nome_tarefa}' iniciada com escala otimizada (300m)!")
print(f"📂 Destino: {asset_saida}")
print("Acompanhe na aba 'Tasks'.")
print("="*60)