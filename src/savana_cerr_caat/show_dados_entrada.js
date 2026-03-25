// ==============================================================================
// CONFIGURAÇÕES INICIAIS
// ==============================================================================
var ano_alvo = 2022;
var palettes = require('users/mapbiomas/modules:Palettes.js');
var palette = palettes.get('brazil');
// Parâmetros de visualização
var vis = {
    class_cobert : { min: 0,  max: 75,   palette: palette,  format: 'png'},
    FalsaCor: {bands: ['swir1_median_dry', 'nir_median_dry', 'red_median_dry'], min: 300, max: 4000, gamma: 1.5},
    CorVerdadeira : {bands: ['red_median_dry', 'green_median_dry', 'blue_median_dry'], min: 200, max: 1500, gamma: 1.2},
    Embedding: {  bands: ['A00', 'A01', 'A02'],  min: -1.0,  max: 1.0 }
};
// 1. Carrega a área de pesquisa (Grades) e centraliza o mapa
var grades_pesquisa = ee.FeatureCollection('projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/grades_area_pesquisa_caatinga_cerrado');
// Map.centerObject(grades_pesquisa, 6);

// Adiciona o contorno das grades no mapa (transparente por dentro, borda vermelha)
Map.addLayer(grades_pesquisa.style({color: 'FF0000', fillColor: '00000000', width: 1}), {}, 'Grades de Pesquisa (Contorno)', true);


var asset_vetor_biomas_250 = 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil';
var biomas = ee.FeatureCollection(asset_vetor_biomas_250);

// ==============================================================================
// MAPBIOMAS COLEÇÃO 10 (Ano 2022)
// ==============================================================================
var mapbiomas = ee.Image('projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_integration_v2');
var mapbiomas_ano = mapbiomas.select('classification_' + ano_alvo).clip(grades_pesquisa);



// ==============================================================================
// MOSAICOS SENTINEL (Merge P1 e P2)
// ==============================================================================
var sentinel_p1 = ee.ImageCollection('projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3');
var sentinel_p2 = ee.ImageCollection('projects/nexgenmap/MapBiomas2/SENTINEL/mosaics-3');
var sentinel_full = sentinel_p1.merge(sentinel_p2);

// Filtra pelo ano, converte para imagem (mosaic) e recorta para a área de interesse
var sentinel_ano = sentinel_full
                        .filter(ee.Filter.eq('year', ano_alvo))
                        .filterBounds(grades_pesquisa)
                        .mosaic()
                        .clip(grades_pesquisa);

Map.addLayer(sentinel_ano, vis.FalsaCor, 'Sentinel Falsa Cor (SWIR/NIR/RED) - ' + ano_alvo, true);
Map.addLayer(sentinel_ano, vis.CorVerdadeira, 'Sentinel Cor Verdadeira (RGB) - ' + ano_alvo, false);

// ==============================================================================
// GOOGLE SATELLITE EMBEDDING
// ==============================================================================
var embedding_col = ee.ImageCollection('GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL');

var date_start = ee.Date.fromYMD(ano_alvo, 1, 1);
var date_end = date_start.advance(1, 'year');

var embedding_ano = embedding_col
        .filterBounds(grades_pesquisa)
        .filterDate(date_start, date_end)
        .mosaic()
        .clip(grades_pesquisa);

// O embedding possui 64 bandas de representação latente (A00 a A63).
// Como elas não têm correspondência de cor física, escolhemos 3 bandas iniciais 
// apenas para observar a textura e os recortes que a rede neural identificou.
Map.addLayer(embedding_ano, vis.Embedding, 'Google Embedding (A00, A01, A02) - ' + ano_alvo, false);
// O MapBiomas já tem a paleta de cores embutida no Asset, não precisamos declarar
Map.addLayer(mapbiomas_ano, vis.class_cobert, 'MapBiomas Col 10 (' + ano_alvo + ')', false);
Map.addLayer(biomas, {}, 'biomas ', false);
