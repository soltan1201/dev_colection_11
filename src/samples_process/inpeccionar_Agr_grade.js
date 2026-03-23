
var palettes = require('users/mapbiomas/modules:Palettes.js');
var vis = {
    visMosaic: {
        min: 0,
        max: 2500,
        bands: ['red_median', 'green_median', 'blue_median']
    },
    props: {  
        textColor: 'ff0000', 
        outlineColor: 'ffffff', 
        outlineWidth: 1.5, 
        outlineOpacity: 0.2
    },
    visclassCC: {
        "min": 0, 
        "max": 67,
        "palette":  palettes.get('classification9'),
        "format": "png"
    },
    mosaicnew: {
        'bands': ['swir1', 'nir', 'red'],
        // 'gain': '0.08,0.06,0.2',
        'min': 0.001, 'max': 0.4, 
        'gamma': 0.5
      },
};
var param = {
    'asset_bacias_buffer' : 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions',
    'asset_grad': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/basegrade30KMCaatinga',
    'classMapB': [3, 4, 5, 9, 12, 13, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32, 33,
                    36, 39, 40, 41, 46, 47, 48, 49, 50, 62],
    'classNew':  [3, 4, 3, 3, 12, 12, 15, 18, 18, 18, 21, 22, 22, 22, 22, 33, 29, 22, 33, 12, 33,
                    18, 18, 18, 18, 18, 18, 18,  4,  12, 18],
    'asset_bacias_buffer' : 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions',
    'asset_grad': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/basegrade30KMCaatinga',
    'assetMapbiomas90': 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1', 
    'asset_mask_toSamples': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/masks/mask_pixels_toSample', 
    'asset_collectionId': 'LANDSAT/COMPOSITES/C02/T1_L2_32DAY',

};
var year_courrent = '2020';
var dictPer = {
    'year': {
        start: year_courrent + '-01-01',
        end: year_courrent + '-12-31',
        surf: 'year',
    },
    'dry': {
        start: year_courrent + '-08-01',
        end: year_courrent + '-12-31',
        surf: 'dry',
    },
    'wet': {
        start: year_courrent + '-01-01',
        end: year_courrent + '-07-31',
        surf: 'wet',
    }

}
var lstPeriodo = ['year', 'dry', 'wet'];


var oneGrade = ee.FeatureCollection(param.asset_grad);//.filter(
    // ee.Filter.eq('indice', int(idGrade)))

var shpBasin = ee.FeatureCollection(param.asset_bacias_buffer);
var gradeSelect = oneGrade.filter(ee.Filter.inList('indice', [4109, 4110]));

var imgMap = ee.Image(param.assetMapbiomas90).select('classification_' + year_courrent)
                    .clip(gradeSelect.geometry());
var imgMapre = imgMap.remap(param.classMapB, param.classNew);

var imgColMask = ee.ImageCollection(param.asset_mask_toSamples)
                        .filterBounds(gradeSelect.geometry())
                        .mosaic().select('mask_sample_' + year_courrent);
                        
print(" dado de mascara ", imgColMask);
Map.addLayer(shpBasin, {color: 'green'}, 'basin buffer', false);
Map.addLayer(oneGrade, {color: 'blue'}, 'grades', false);
Map.addLayer(gradeSelect, {color: 'red'}, 'grade Select');

lstPeriodo.forEach(function(periodo){
    var dateStart =  dictPer[periodo].start;
    var dateEnd = dictPer[periodo].end;

    var colMosaic = ee.ImageCollection(param.asset_collectionId)
                    .filter(ee.Filter.date(dateStart, dateEnd))
                    .filter(ee.Filter.bounds(gradeSelect.geometry()))
                    .mosaic().clip(gradeSelect.geometry());
                    
    var colMosaicMax = ee.ImageCollection(param.asset_collectionId)
                    .filter(ee.Filter.date(dateStart, dateEnd))
                    .filter(ee.Filter.bounds(gradeSelect.geometry()))
                    .max().clip(gradeSelect.geometry());

    Map.addLayer(colMosaic, vis.mosaicnew, 'mosaic_' + dictPer[periodo].surf);
    Map.addLayer(colMosaicMax, vis.mosaicnew, 'max_' + dictPer[periodo].surf);
})

Map.addLayer(imgMapre, vis.visclassCC, 'mapbiomas_remap', false);
Map.addLayer(imgMap, vis.visclassCC, 'mapbiomas', false);
Map.addLayer(imgColMask.eq(1).selfMask(), {min: 0, max: 1, palette: '#000068'}, 'mascara', false);