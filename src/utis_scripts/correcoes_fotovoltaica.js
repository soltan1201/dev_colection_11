// https://code.earthengine.google.com/45dbc146fab932657ba13ed9f2f638fe
var palettes = require('users/mapbiomas/modules:Palettes.js');
var vis = {
    fotovoltaica: {min: 0, max: 1, palette: ['#2E3B4E']},
    vismosaicoGEE: {
        'min': 0.001, 'max': 0.15,
        bands: ['red', 'green', 'blue']
    },
    map_class: {
        min: 0,
        max: 69,
        palette: palettes.get('classification9')
    }
}
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
    asset_painel_solar: 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/solar-panel-br-30m_2016_2024',
    asset_mapbiomas: 'projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10/INTEGRATION/classification',
    asset_bioma_shp: 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
    asset_output_Polygons: 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/Energias/polygons/buffer_5km_fotovolt_final_id',
    asset_biomas_raster : 'projects/mapbiomas-workspace/AUXILIAR/biomas-raster-41',
    asset_fotovoltaicaV2: 'projects/geo-data-s/assets/fotovoltaica/version_2_clean',
    asset_fotovoltaicaV1: 'projects/geo-data-s/assets/fotovoltaica/versao1',
    asset_fotovoltaicaV3: 'projects/geo-data-s/assets/fotovoltaica/version_3',
    asset_fotovoltaicaV4: 'projects/geo-data-s/assets/fotovoltaica/version_4',
    asset_pontos_ANEEL: 'projects/mapbiomas-arida/buffer_5km_fotovolt_final',
    asset_fotoVoltaica_Am: 'projects/mapbiomas-arida/usinas_comple_amz',  // 2021
    asset_collectionId: 'LANDSAT/COMPOSITES/C02/T1_L2_32DAY',
    outputVersion: '0-5',
    nyears: [
        '2010','2011','2012','2013','2014','2015','2016','2017',
        '2018','2019','2020','2021','2022','2023','2024'
    ],
}
 var bioma_activo = 'Amazônia';
// var bioma_activo = 'Caatinga';
//var bioma_activo = 'Cerrado';
// var bioma_activo = 'Mata Atlântica';
// var bioma_activo = 'Pampa';
// var bioma_activo = 'Pantanal';
var geom = ee.FeatureCollection(param.asset_bioma_shp).geometry().bounds();
var shp_biomas = ee.FeatureCollection(param.asset_bioma_shp).filter(ee.Filter.eq('Bioma', bioma_activo));
print("shw metadata shp biomas ", shp_biomas);
var mask_raster_bioma = ee.Image(param.asset_biomas_raster).gt(0);
var mosaicEE = ee.ImageCollection(param.asset_collectionId);
var shp_areas_fv = ee.FeatureCollection(param.asset_output_Polygons);
var raster_FV = ee.Image(param.asset_painel_solar);
var raster_mapbiomas = ee.ImageCollection(param.asset_mapbiomas)
                            .filter(ee.Filter.eq('version', param.outputVersion))
                            .mosaic();
var raster_FV_v4 = ee.ImageCollection(param.asset_fotovoltaicaV4).filter(ee.Filter.eq('year', 2024));
var raster_FV_v3 = ee.ImageCollection(param.asset_fotovoltaicaV3).filter(ee.Filter.eq('year', 2024));
var raster_FV_v2 = ee.ImageCollection(param.asset_fotovoltaicaV2).filter(ee.Filter.eq('year', 2024));
var raster_FV_v1 = ee.ImageCollection(param.asset_fotovoltaicaV1)//.filter(ee.Filter.eq('year', 2024));
var shp_pontos = ee.FeatureCollection(param.asset_pontos_ANEEL);
print("raster Foto Voltaica ", raster_FV_v1);
var feat_fv_amaz = ee.FeatureCollection(param.asset_fotoVoltaica_Am)
                    .map(function(feat){return feat.set('class', 1)});
var raster_pto1_fv_amaz = feat_fv_amaz.reduceToImage(['class'], ee.Reducer.first())
var raster_pto2_fv_amaz = foto_pto2_amz.reduceToImage(['class'], ee.Reducer.first())
Map.addLayer(raster_pto2_fv_amaz, {min:0, max: 1}, 'extra FV');
var joinFV = null;
var layer_FV_extra = ee.Image().byte();

var feat_FV_AMA = ee.FeatureCollection([]);
param.nyears.forEach(function(nyear){
    var band_activa = 'classification_' + nyear;
    var band_fv_act = 'Panel_' + nyear;
    var raster_map_year = raster_mapbiomas.select(band_activa);
    var dateStart = ee.Date.fromYMD(parseInt(nyear), 1, 1);
    var dateEnd = ee.Date.fromYMD(parseInt(nyear), 12, 31);
    var mosGEEyy = mosaicEE.filter(ee.Filter.date(dateStart, dateEnd))
                        .median().updateMask(mask_raster_bioma);
    var layerZero = raster_map_year.gt(100).rename(band_fv_act);
    if (nyear === '2024'){
        joinFV = layerZero.where(raster_pto2_fv_amaz.eq(1), 1);
        joinFV = joinFV.where(raster_pto1_fv_amaz.eq(1), 1);
        print(band_fv_act, joinFV.bandNames());
        layer_FV_extra = layer_FV_extra.addBands(joinFV);
        Map.addLayer(joinFV.selfMask(), {min:0, max: 1, palette: "#000038"}, 'extra FV 2024');
        feat_FV_AMA = feat_FV_AMA.merger(feat_fv_amaz.merge(foto_pto2_amz).map(function(feat){return feat.set('year', 2024)}));
    }else{
        if (parseInt(nyear) > 2020){
            joinFV = layerZero.where(raster_pto1_fv_amaz.eq(1), 1);
            print( band_fv_act, joinFV.bandNames());
            layer_FV_extra = layer_FV_extra.addBands(joinFV);
            Map.addLayer(joinFV.selfMask(), {min:0, max: 1, palette: "#000038"}, 'extra FV');
            feat_FV_AMA = feat_FV_AMA.merger(feat_fv_amaz.map(function(feat){return feat.set('year', parseInt(nyear))}))
        }else{
            if(nyear === '2020'){
                var raster_ponto1_fv_amaz = feat_fv_amaz.filterBounds(omissao).reduceToImage(['class'], ee.Reducer.first());
                joinFV = layerZero.where(raster_ponto1_fv_amaz.eq(1), 1);
                layer_FV_extra = layer_FV_extra.addBands(joinFV);
                Map.addLayer(joinFV.selfMask(), {min:0, max: 1, palette: "#000038"}, 'extra FV 2020');
                feat_FV_AMA = feat_FV_AMA.merger(feat_fv_amaz.filterBounds(omissao).map(function(feat){return feat.set('year', parseInt(nyear))}))
            }
        }
    }
    
    Map.addLayer(mosGEEyy, vis.vismosaicoGEE, 'mosEE_' + nyear, false);
    Map.addLayer(raster_map_year, vis.map_class, 'mapbiomas_' + nyear, false);
    if(parseInt(nyear) > 2015){
        var raster_fv_year = raster_FV.select(band_fv_act);
        Map.addLayer(raster_fv_year.selfMask(), vis.fotovoltaica, 'mapa_fotoVol' + nyear, false);
    }
})
var lst_bnds = [
  "Panel_2020","Panel_2021", "Panel_2022", "Panel_2023", 
  "Panel_2024"
];
layer_FV_extra = layer_FV_extra.select(lst_bnds);

var name_export = 'solar-panel-br-30m_2020_2024_amaz';
var id_asset = 'projects/geo-data-s/assets/fotovoltaica/';
functionExports(layer_FV_extra.selfMask(), name_export, geom, id_asset)


Map.addLayer(raster_FV_v1, {min:0, max: 1, palette: '#FF0000'}, 'extra FV1');
Map.addLayer(raster_FV_v2, {min:0, max: 1, palette: '#FF0000'}, 'extra FV2');
Map.addLayer(raster_FV_v3, {min:0, max: 1, palette: '#FF0000'}, 'extra FV3');
Map.addLayer(raster_FV_v4, {min:0, max: 1, palette: '#FF0000'}, 'extra FV4');
Map.addLayer(shp_pontos, {color: 'black'}, 'ptos ANEEL');
Map.addLayer(feat_fv_amaz, {color: 'black'}, 'shp Neriv');
Map.addLayer(shp_areas_fv, {color: 'red'}, 'áreas FV');
Map.addLayer(shp_biomas, {color: 'black'}, 'shp ' + bioma_activo);
//Map.centerObject(shp_biomas, 10);