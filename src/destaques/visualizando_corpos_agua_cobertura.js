var dictRes = {
    1: 'Natural',
    2: 'Reservatorios',
    3: 'Hidroelectricas',
    4: 'Mineração',
    6: 'Outras classes'
};
var palettes = require('users/mapbiomas/modules:Palettes.js');
var vis = {
    layer_agua: {
        min: 1, max: 6,
        // palette: ['#0000FE','#00C4DA','#E5538C','#AC0024','#ffe4b5']
        palette: "0000FE,00C4DA,E5538C,AC0024,ffe4b5,0000FF"
    },
    visclassCC: {
                "min": 0, 
                "max": 69,
                "palette":  palettes.get('classification9'),
                "format": "png"
    },
    layerNatAnt: {
        min: 1, max: 3,
        palette: ["#1f8d49", "#FFFFB2", "#0000FF"]
    }
};

var param = {
    'asset_raster_biomas': 'projects/mapbiomas-workspace/AUXILIAR/biomas-raster-41',
    'assetBiomas': 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
    'asset_veg': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/municipios_statisticVeg',
    'asset_semiarido': 'projects/mapbiomas-workspace/AUXILIAR/SemiArido_2024',
    'asset_semiarido_old': 'projects/mapbiomas-workspace/AUXILIAR/semiarido',
    'asset_corpos_agua': 'projects/mapbiomas-workspace/AMOSTRAS/GTAGUA/OBJETOS/CLASSIFICADOS/TESTE_1_raster',
    'asset_cobertura_col90': "projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1",
    'classMapB': [3, 4, 5, 9,12,13,15,18,19,20,21,22,23,24,25,26,29,30,31,32,33,36,39,40,41,46,47,48,49,50,62],
    'classNew':  [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 3, 2, 2, 2, 2, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 2],
};
var shpSemiarido = ee.FeatureCollection(param.asset_semiarido);
var shpSemiarido_old = ee.FeatureCollection(param.asset_semiarido_old);
var masc_recorte = (ee.FeatureCollection([ee.Feature(shpSemiarido.geometry(), {'valor': 1})])
                        .reduceToImage(['valor'], ee.Reducer.first()));

var mascara = ee.ImageCollection(param.asset_corpos_agua)
                        .filter(ee.Filter.eq('year', 2023))
                        .mosaic().neq(2);

var imColWater = ee.ImageCollection(param.asset_corpos_agua)
                            .filter(ee.Filter.eq('year', 2023))
                            .mosaic().updateMask(masc_recorte)
                            .updateMask(mascara);  
print("Show metadatas of image Collection Water ", imColWater);

var cobertura23 = ee.Image(param.asset_cobertura_col90)
                    .select('classification_2023')                    
                    .updateMask(masc_recorte);

var naturalAnt23 = cobertura23.remap(param.classMapB, param.classNew);
var cobertura85 = ee.Image(param.asset_cobertura_col90)
                    .select('classification_1985')                    
                    .updateMask(masc_recorte);

var naturalAnt85 = cobertura85.remap(param.classMapB, param.classNew);
var limite = ee.Image().byte().paint({
                featureCollection: shpSemiarido,
                color: 1,
                width: 1.5
        });
var limiteOld = ee.Image().byte().paint({
            featureCollection: shpSemiarido_old,
            color: 1,
            width: 1.5
        });
Map.addLayer(ee.Image.constant(1), {min: 0, max:1},  'base');
Map.addLayer(limite, {palette: '#484030'}, 'semiarido');
Map.addLayer(imColWater, vis.layer_agua, 'water');
Map.addLayer(cobertura85, vis.visclassCC, 'uso e cobertura 1985')
Map.addLayer(naturalAnt85, vis.layerNatAnt, 'Natural Antropico 1985');
Map.addLayer(cobertura23, vis.visclassCC, 'uso e cobertura 2023')
Map.addLayer(naturalAnt23, vis.layerNatAnt, 'Natural Antropico 023');
Map.addLayer(limiteOld, {palette: '#003434'}, 'semiarido_old');








