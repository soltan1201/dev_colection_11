var functionExports = function(cc_image, nameEXp, geomSHP, nassetId){
    var pmtExpo = {
        image: cc_image,
        description: nameEXp,
        scale: param.scale, // Escolha a escala de acordo com sua necessidade
        region: geomSHP,
        assetId: nassetId + nameEXp, // Substitua pelo nome da sua pasta no Google Drive
        maxPixels: 1e13 // Escolha o valor adequado para o número máximo de pixels permitidos
    };
    Export.image.toAsset(pmtExpo);
    print("maps salvo " + nameEXp + " ...");
};

var param = { 
    assetIm: 'projects/nexgenmap/MapBiomas2/LANDSAT/BRAZIL/mosaics-2',  
    asset_collectionId: 'LANDSAT/COMPOSITES/C02/T1_L2_32DAY',
    assetBacia: 'projects/ee-solkancengine17/assets/shape/bacias_buffer_caatinga_49_regions',     
    asset_output: 'projects/ee-solkancengine17/assets/shape',
    listaNameBacias: [
        '7754', '7691', '7581', '7625', '7584', '751', '7614', 
        '752', '7616', '745', '7424', '773', '7612', '7613', 
        '7618', '7561', '755', '7617', '7564', '761111','761112', 
        '7741', '7422', '76116', '7761', '7671', '7615', '7411', 
        '7764', '757', '771', '7712', '766', '7746', '753', '764', 
        '7541', '7721', '772', '7619', '7443','765','7544','7438', 
        '763', '7591', '7592', '7622', '746'
    ],
    scale: 10
}

print(param.listaNameBacias.length)

var FeatColbacia = ee.FeatureCollection(param.assetBacia);
print("know the metadata from featcollections of Bacias ", FeatColbacia);
var lstIdCod = FeatColbacia.reduceColumns(ee.Reducer.toList(), ['id_codigo']).get('list');

print("lista de codigos de bacias ", lstIdCod);
var pmtred = {
    properties: ['id_codigo'], 
    reducer: ee.Reducer.first()
};
var rasterBacias = FeatColbacia.reduceToImage(pmtred).toByte();
var name_exp = 'bacias_buffer_caatinga_49_regions_raster' + String(param.scale) + 'M'
functionExports(rasterBacias, name_exp, FeatColbacia.geometry(), param.asset_output);