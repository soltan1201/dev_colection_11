var palettes = require('users/mapbiomas/modules:Palettes.js');
var palette = palettes.get('brazil');
var vis = {
    savana: {min:0, max: 2, palette: ['#F7F6E5', '#44A194','#A0D585']},
    class_cobert : { min: 0,  max: 75,   palette: palette,  format: 'png'},
    embedding_img1: { min: -0.3, max: 0.3,   bands: ['A01', 'A16', 'A09'] },
    embedding_img2: {  min: -0.3, max: 0.3,  bands: ['A01', 'A25', 'A50'] },
    FalsaCor: {bands: ['swir1_median_dry', 'nir_median_dry', 'red_median_dry'], min: 300, max: 4000, gamma: 1.5},
    CorVerdadeira : {bands: ['red_median_dry', 'green_median_dry', 'blue_median_dry'], min: 200, max: 1500, gamma: 1.2},
}; 
var lst_year = [2022, 2023, 2024];  //
var asset_cobertura = "projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_coverage_v2";
var asset_savana = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_Savana';
// 1. Carrega a área de pesquisa (Grades) e centraliza o mapa
var asset_area_estudo = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/grades_area_pesquisa_caatinga_cerrado';
var grades_pesquisa = ee.FeatureCollection(asset_area_estudo);

var asset_vetor_biomas_250 = 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil';
var biomas = ee.FeatureCollection(asset_vetor_biomas_250);

var col_savana = ee.ImageCollection(asset_savana);
var raster_cob = ee.Image(asset_cobertura);
print('show metadatas of colection savana ', col_savana);
print(col_savana.aggregate_histogram('year'));
var map_biomas = ee.FeatureCollection(asset_vetor_biomas_250);

// ==============================================================================
// MOSAICOS SENTINEL (Merge P1 e P2)
// ==============================================================================
var sentinel_p1 = ee.ImageCollection('projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3');
var sentinel_p2 = ee.ImageCollection('projects/nexgenmap/MapBiomas2/SENTINEL/mosaics-3');
var sentinel_full = sentinel_p1.merge(sentinel_p2);



// ==============================================================================
// GOOGLE SATELLITE EMBEDDING
// ==============================================================================
var embedding_col = ee.ImageCollection('GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL');




lst_year.forEach(function(nyear){
    var savana_year = col_savana.filter(ee.Filter.eq('year', nyear));
    var raste_cob_year = raster_cob.select('classification_' + String(nyear));

    // Filtra pelo ano, converte para imagem (mosaic) e recorta para a área de interesse
    var sentinel_ano = sentinel_full
                        .filter(ee.Filter.eq('year', nyear))
                        .filterBounds(grades_pesquisa)
                        .mosaic()
                        .clip(grades_pesquisa);

    var date_start = ee.Date.fromYMD(nyear, 1, 1);
    var date_end = date_start.advance(1, 'year');

    var embedding_ano = embedding_col
            .filterBounds(grades_pesquisa)
            .filterDate(date_start, date_end)
            .mosaic()
            .clip(grades_pesquisa);
    // O embedding possui 64 bandas de representação latente (A00 a A63).
    // Como elas não têm correspondência de cor física, escolhemos 3 bandas iniciais 
    // apenas para observar a textura e os recortes que a rede neural identificou.
    Map.addLayer(sentinel_ano, vis.FalsaCor, 'Sentinel Falsa Cor (SWIR/NIR/RED) - ' + nyear, true);
    Map.addLayer(sentinel_ano, vis.CorVerdadeira, 'Sentinel Cor Verdadeira (RGB) - ' + nyear, false);
    Map.addLayer(embedding_ano, vis.embedding_img1, 'G.Embedding (A01, A16, A09) - ' + nyear, false);
    Map.addLayer(embedding_ano, vis.embedding_img2, 'G.Embedding (A01, A25, A50) - ' + nyear, false);
    Map.addLayer(raste_cob_year, vis.class_cobert, 'Mapbiomas ' + String(nyear), false);
    Map.addLayer(savana_year, vis.savana, 'Savana ' + String(nyear), false);
})


var palette = ['#78d532', '#b58145', '#e5a628', '#497d1e', '#d402f2', '#cdf439'];
Map.addLayer(map_biomas, {palette: palette, min: 1, max: 6}, 'biomas ');





