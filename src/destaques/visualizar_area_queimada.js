
var palettes = require('users/mapbiomas/modules:Palettes.js');
var vis = {
    layer_fire: {
        min: 1, max: 1,
        palette: "000000,ff0000 ",
        opacity: 0.65
    },
    visclassCC: {
                "min": 0, 
                "max": 69,
                "palette":  palettes.get('classification9'),
                "format": "png"
    }
};

var param = {
    'asset_raster_biomas': 'projects/mapbiomas-workspace/AUXILIAR/biomas-raster-41',
    'assetBiomas': 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
    'asset_veg': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/municipios_statisticVeg',
    'asset_semiarido': 'projects/mapbiomas-workspace/AUXILIAR/SemiArido_2024',
    'asset_semiarido_old': 'projects/mapbiomas-workspace/AUXILIAR/semiarido',
    'asset_matopiba': 'projects/mapbiomas-fogo/assets/territories/matopiba',
    'asset_fire_anual_cov': 'projects/mapbiomas-public/assets/brazil/fire/collection3_1/mapbiomas_fire_collection31_annual_burned_coverage_v1',
};
var shpSemiarido = ee.FeatureCollection(param.asset_semiarido);
var shpSemiarido_old = ee.FeatureCollection(param.asset_semiarido_old);
var shp_matopiba = ee.FeatureCollection(param.asset_matopiba);
var masc_recorte = (ee.FeatureCollection([ee.Feature(shpSemiarido.geometry(), {'valor': 1})])
                        .reduceToImage(['valor'], ee.Reducer.first()));

var imFire = ee.Image(param.asset_fire_anual_cov)
                      .select("burned_coverage_2023")
                      .updateMask(masc_recorte);
                            // .updateMask(mascara);  
print("Show metadatas of image Collection Water ", imFire);


var limite = ee.Image().byte().paint({
                featureCollection: shpSemiarido,
                color: 1,
                width: 2.5
        });
var limiteOld = ee.Image().byte().paint({
            featureCollection: shpSemiarido_old,
            color: 1,
            width: 1.5
        });
var limiteMato = ee.Image().byte().paint({
            featureCollection: shp_matopiba,
            color: 1,
            width: 2
        });

Map.addLayer(ee.Image.constant(1), {min: 0, max:1},  'base');

Map.addLayer(imFire, vis.layer_fire, 'Fire');
Map.addLayer(limite, {palette: '#484030'}, 'semiarido');
Map.addLayer(limiteOld, {palette: '#003434'}, 'semiarido_old');
Map.addLayer(limiteMato, {palette: '#484030'}, 'matopiba');








