// Definindo o caminho da pasta
var folderPath = "projects/mapbiomas-public/assets/brazil/lulc";

// Como é uma pasta de Coleções de Imagens (ImageCollections),
// vamos listar o conteúdo.
var assets = ee.data.listAssets(folderPath);

print("Lista de Assets na Pasta:", assets.assets);


var lst_folders = [
      "projects/mapbiomas-public/assets/brazil/lulc/collection7_1",
      "projects/mapbiomas-public/assets/brazil/lulc/collection8",
      "projects/mapbiomas-public/assets/brazil/lulc/collection9",
      "projects/mapbiomas-public/assets/brazil/lulc/collection10"
]

lst_folders.forEach(function(asset_ids){
    print(" >> " + asset_ids);
    // Como é uma pasta de Coleções de Imagens (ImageCollections),
    // vamos listar o conteúdo.
    var assets = ee.data.listAssets(asset_ids);
    print("Lista de Assets na Pasta:", assets.assets);
    
})

var lst_asset_inter = [
    'projects/mapbiomas-public/assets/brazil/lulc/collection7_1/mapbiomas_collection71_integration_v1',
    'projects/mapbiomas-public/assets/brazil/lulc/collection8/mapbiomas_collection80_integration_v1',
    'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1',
    'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_coverage_v2',
    'projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_deforestation_secondary_vegetation_v2',
    
]