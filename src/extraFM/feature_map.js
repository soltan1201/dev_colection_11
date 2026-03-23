var asset_ROIs = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_byBasinInd/rois_fromGrade_';
var asset_bacias = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions';
var asset_output = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/rois_resample_featmaps';
var bacias = ee.FeatureCollection(asset_bacias);

// Define a list of years to export
var nyear = '1990';

//exporta a amostras antes coletadas para o asset
function processoExportar_toAsset(areaFeat, nameT){      
    var optExp = {
          'collection': ee.FeatureCollection(areaFeat), 
          'description': nameT, 
          'assetId': asset_output + '/' + nameT
        };    
    Export.table.toAsset(optExp) ;
    print(" salvando ... " + nameT + "..!")      ;
}


var nameBacias = [
    '7411','7754', '7691', '7581', '7625', '7584', '751', '7614', 
    '752', '7616', '745', '7424', '773', '7612', '7613', 
    '7618', '7561', '755', '7617', '7564', '761111','761112', 
    '7741', '7422', '76116', '7761', '7671', '7615',  
    '7764', '757', '771', '7712', '766', '7746', '753', '764', 
    '7541', '7721', '772', '7619', '7443', '765', '7544', '7438', 
    '763', '7591', '7592', '7622', '746'
]
print("we have  bacias ", nameBacias.length);
var cc = 0;
nameBacias.forEach(function(nbacias){
    var temporal_basin = bacias.filter(ee.Filter.eq('nunivotto4', nbacias))
    var featMaps = ee.ImageCollection('projects/solvedltda/assets/MB11_FM/' + nyear)
                      .filterBounds(temporal_basin.geometry())
                      .mosaic();
    
    print("show metadados Feature Maps year " + nyear, featMaps);

    var rois_temporal = ee.FeatureCollection(asset_ROIs + nbacias)
                            .filter(ee.Filter.eq('year', parseInt(nyear)));
    print("we load features samples with size ", rois_temporal.size());
    print(rois_temporal.first())
    // sampleRegions(collection, properties, scale, projection, tileScale, geometries)
    var read_temporal = featMaps.sampleRegions({
                            collection: rois_temporal, 
                            properties: rois_temporal.first().propertyNames(), 
                            scale: 30, 
                            tileScale: 16, 
                            geometries: true
                    })
    
    if (cc < 6){
        Map.addLayer(featMaps, {}, 'year ' + nyear);
    }
    print('know size final ', read_temporal.size());
    print(read_temporal.first());
    var name_export = 'samples_' + nbacias;
    processoExportar_toAsset(read_temporal, name_export);    
})

Map.addLayer(bacias, {color: 'yellow'}, 'basin');