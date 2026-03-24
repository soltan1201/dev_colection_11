import ee
import sys

# =========================================================================
# 1. INICIALIZAÇÃO DO EARTH ENGINE
# =========================================================================
try:
    ee.Initialize(project= 'mapbiomas-brazil')
    print('✅ Earth Engine inicializado com sucesso!')
except Exception as e:
    print(f"❌ Erro ao inicializar o GEE: {e}")
    sys.exit()

# =========================================================================
# 2. CONFIGURAÇÃO DOS DIRETÓRIOS
# =========================================================================
# Pasta onde estão as amostras separadas
pasta_origem = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/rois_grades_cerr_caat_embeddin'

# Nome do NOVO asset consolidado (Ajuste o nome final como preferir)
asset_destino = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/rois_grades_cerr_caat_embeddins'

# =========================================================================
# 3. FUNÇÃO PARA LISTAR TODOS OS ASSETS (COM PAGINAÇÃO)
# =========================================================================
def listar_assets_pasta(folder_id):
    """
    Lê todos os assets de uma pasta, ignorando o limite padrão de 1000 itens
    da API do Earth Engine, usando paginação (pageToken).
    """
    assets = []
    page_token = None
    
    while True:
        params = {'parent': folder_id, 'pageSize': 1000}
        if page_token:
            params['pageToken'] = page_token
        
        response = ee.data.listAssets(params)
        
        # Pega apenas os caminhos (IDs) dos arquivos
        assets.extend([item['name'] for item in response.get('assets', [])])
        
        page_token = response.get('nextPageToken')
        if not page_token:
            break
            
    return assets

# =========================================================================
# 4. EXECUÇÃO DO MERGE E EXPORTAÇÃO
# =========================================================================
print(f"🔎 Lendo arquivos da pasta:\n{pasta_origem}...\n")
lista_assets = listar_assets_pasta(pasta_origem)

print(f"📊 Total de arquivos encontrados: {len(lista_assets)}")

if len(lista_assets) == 0:
    print("⚠️ Nenhum arquivo encontrado para mesclar. Verifique o caminho da pasta.")
    sys.exit()

# OTIMIZAÇÃO: Cria uma lista de objetos ee.FeatureCollection
colecoes = [ee.FeatureCollection(asset) for asset in lista_assets]

# TRUQUE DE MESTRE: Usa o .flatten() em vez de loop com .merge()
# Isso instrui o Google a empilhar todas as tabelas em uma única operação paralela
fc_consolidada = ee.FeatureCollection(colecoes).flatten()

# Dispara a exportação para o novo Asset
nome_tarefa = 'Merge_Amostras_Embeddin_Col11'

print(f"\n⏳ Agendando a exportação para o asset:\n{asset_destino}...\n")

try:
    # Tenta deletar o asset de destino caso ele já exista de uma tentativa anterior
    ee.data.deleteAsset(asset_destino)
except:
    pass

tarefa = ee.batch.Export.table.toAsset(
    collection=fc_consolidada,
    description=nome_tarefa,
    assetId=asset_destino
)

tarefa.start()

print("=======================================================================")
print(f"🚀 Tarefa '{nome_tarefa}' enviada com sucesso!")
print("Acompanhe o progresso na aba 'Tasks' do Code Editor ou no terminal de tarefas.")
print("=======================================================================")