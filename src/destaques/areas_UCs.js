var Legend   = require('users/joaovsiqueira1/packages:Legend.js');
var Palettes = require('users/mapbiomas/modules:Palettes.js');

// ── Configuração ──────────────────────────────────────────────────────────────
var year_act    = '2024';
var band_activa = 'classification_' + year_act;

var palette = Palettes.get('brazil');
var vis     = {min: 0, max: 75, palette: palette, format: 'png'};

// ── Assets ───────────────────────────────────────────────────────────────────
var asset_col10 = 'projects/mapbiomas-public/assets/brazil/lulc/collection10/' +
                  'mapbiomas_brazil_collection10_coverage_v2';

// ⬇️  Substitua pelo seu asset de UCs
var asset_ucs   = 'projects/mapbiomas-arida/cnuc_2026_03_atualizado';

// Asset oficial de biomas (IBGE / MMA via GEE)
var asset_biomas = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019';
var asset_biomas_raters= 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019-raster'

// ── Raster e vetores ─────────────────────────────────────────────────────────
var ucs          = ee.FeatureCollection(asset_ucs)
                        .map(function(feat){
                            // adicionar a área em hectare
                            return feat.set('are_haa', feat.area(0.01).divide(10000));
                        });
print("layers UCS", ucs);
var biomas       = ee.FeatureCollection(asset_biomas);
var biomas_raster= ee.Image(asset_biomas_raters).rename('biomas');
print("show metadados biomas ", biomas_raster);
var raster_col10 = ee.Image(asset_col10).select(band_activa);


Map.addLayer(raster_col10, vis, 'Coleção 10 ' + year_act);

// ── Classes de vegetação nativa (MapBiomas Coleção 10) ───────────────────────
// Nível 1: Vegetação Nativa = classes 1, 3, 4, 5, 6, 10, 11, 12, 13, 32, 50
var native_veg_classes = [1, 3, 4, 5, 6, 10, 11, 12, 13, 32, 49, 50];
var native_mask = raster_col10.remap(
        native_veg_classes,
        ee.List.repeat(1, native_veg_classes.length),
        0       
    ).rename('native_veg');
var raster_veg_nat = native_mask.multiply(ee.Image.pixelArea()).addBands(biomas_raster);


function convert2featCollection (item){
    item = ee.Dictionary(item)
    var feature = ee.Feature(ee.Geometry.Point([0, 0])).set(
        'biomas', item.get('biomas'), "area", item.get('sum'))
        
    return feature;
}
// ── Interseção: UCs dentro de cada bioma ─────────────────────────────────────
var ucs_shps = ucs.map(function(feat_UC) {
    var geom_feat_UC = feat_UC.geometry();
    // Área de vegetação nativa dentro das UCs deste bioma
    var reducer = ee.Reducer.sum().group(1, 'biomas')
    var area_nat = raster_veg_nat
                        .reduceRegion({
                            reducer   : reducer,
                            geometry  : geom_feat_UC,
                            scale     : 30,
                            maxPixels : 1e13,
                            bestEffort: true,
                            tileScale: 16
                        });


    var areas = ee.List(area_nat.get('groups'))
                    .map(function(item){
                        return convert2featCollection(item);
                    })
    var updated_feat = ee.Feature(
        ee.List(areas).iterate(function(item, acc) {
            item = ee.Feature(item);
            acc  = ee.Feature(acc);
            return acc.set(item.get('biomas'), ee.Number(item.get('area')).divide(10000));
        }, feat_UC)
    );
    return updated_feat;

});

print('Vegetação nativa por bioma nas UCs:', ucs_shps);

// ── Export ────────────────────────────────────────────────────────────────────
Export.table.toDrive({
    collection  : ucs_shps,
    description : 'veg_nativa_bioma_ucs_' + year_act,
    fileFormat  : 'CSV'
});