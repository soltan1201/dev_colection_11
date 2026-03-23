var functionExports = function(cc_image, nameEXp, geomSHP, nassetId){
    var pmtExpo = {
        image: cc_image,
        description: nameEXp,
        scale: 30, // Escolha a escala de acordo com sua necessidade
        region: geomSHP,
        assetId: nassetId + nameEXp, // Substitua pelo nome da sua pasta no Google Drive
        maxPixels: 1e13 // Escolha o valor adequado para o número máximo de pixels permitidos
    };
    Export.image.toAsset(pmtExpo);
    print("maps salvo " + nameEXp + " ...");
};

var param = {
    asset_fotovoltaicos: 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/solar-panel-br-30m_2016_2024_v2',
    asset_fotoV_amaz: 'projects/geo-data-s/assets/fotovoltaica/solar-panel-br-30m_2020_2024_amaz',
    asset_biomas: 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
    asset_output: 'projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10/SOLAR-PANELS/classification/',
    nyears: [
        '2016','2017','2018','2019','2020','2021','2022','2023','2024'
    ],
}
var geom = ee.FeatureCollection(param.asset_biomas).geometry().bounds();
var raster_FV = ee.Image(param.asset_fotovoltaicos);
var  raster_FV_amaz = ee.Image(param.asset_fotoV_amaz);
print("ver raster ", raster_FV);

var img_FT = ee.Image().byte();
param.nyears.forEach(function(nyear){
    print(ee.String(nyear).cat(" < FV >  ") );
    var band_fv_act = 'Panel_' + nyear;
    var fvYY = raster_FV.select(band_fv_act).unmask(0);
    if (parseInt(nyear) > 2019){
        fvYY = fvYY.add(raster_FV_amaz.select(band_fv_act).unmask(0));
    }
    print(fvYY);
    Map.addLayer(fvYY.selfMask(), {min:0, max: 1, palette: "#000038"}, 'FV ' + nyear);
    // img_FT = img_FT.addBands(fvYY);
    fvYY = fvYY.set('biome', null)
                .set('year', parseInt(nyear))
                .set('version', '1')
                .set('collection_id', 10.0)
                .set('description', 'versão beta dos empreendimentos fotovoltaicos')
                .set('source', 'geodatin')
                .set('theme', 'SOLAR-PANELS')
                .set('territory', 'BRAZIL')
                .set('system:footprint', geom) 
    var name_export = nyear + '-1';
    functionExports(fvYY.rename('classification_' + nyear).selfMask(), name_export, geom, param.asset_output)
    
})

Map.setCenter(-62.026922, -9.454815, 12)








