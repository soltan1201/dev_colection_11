var palettes = require('users/mapbiomas/modules:Palettes.js');
var palette = palettes.get('brazil');
var vis = {
    savana: {min:0, max: 2, palette: ['#F7F6E5', '#44A194','#A0D585']},
    class_cobert : { min: 0,  max: 75,   palette: palette,  format: 'png'}
};
var lst_year = [2022, 2023, 2024];
var asset_cobertura = "projects/mapbiomas-public/assets/brazil/lulc/collection10/mapbiomas_brazil_collection10_coverage_v2";
var asset_savana = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_Savana';
var asset_vetor_biomas_250 = 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil';

var col_savana = ee.ImageCollection(asset_savana);
var raster_cob = ee.Image(asset_cobertura);
print('show metadatas of colection savana ', col_savana);
print(col_savana.aggregate_histogram('year'));
var biomas = ee.FeatureCollection(asset_vetor_biomas_250);


lst_year.forEach(function(nyear){
    var savana_year = col_savana.filter(ee.Filter.eq('year', nyear));
    var raste_cob_year = raster_cob.select('classification_' + String(nyear));
    Map.addLayer(raste_cob_year, vis.class_cobert, 'Mapbiomas ' + String(nyear), false);
    Map.addLayer(savana_year, vis.savana, 'Savana ' + String(nyear), false);
})


var palette = ['#78d532', '#b58145', '#e5a628', '#497d1e', '#d402f2', '#cdf439'];
Map.addLayer(map_biomas, {palette: palette, min: 1, max: 6}, 'biomas ');





