function set_class_value(feat){
    var dictValor = ee.Dictionary({
        'Perda maior que 30%': 1,    
        'Perda de 15 a 30%': 2,
        'Perda de 5 a 15%':  3,
        'Ganho 5% a Perda 5% (estável)': 4,
        'Ganho de 5 a 15%': 5,
        'Ganho de 15 a 30%': 6,
        'Ganho maior que 30%': 7
    })
    
    return feat.set('cLossGain', dictValor.get(feat.get('Classe')));
}


// ##############################################
// ###     Helper function
// ###    @param item 
// ##############################################
function convert2featCollection (item){
    var item = ee.Dictionary(item);
    var feature = ee.Feature(ee.Geometry.Point([0, 0])).set(
        'class', item.get('class'),"area", item.get('sum'));
        
    return feature;
}
// #########################################################################
// ####     Calculate area crossing a cover map (deforestation, mapbiomas)
// ####     and a region map (states, biomes, municipalites)
// ####      @param image 
// ####      @param geometry
// #########################################################################
// # https://code.earthengine.google.com/5a7c4eaa2e44f77e79f286e030e94695
function calculateArea (image, pixelArea, geometry){
    pixelArea = pixelArea.addBands(image.rename('class'));
    var reducer = ee.Reducer.sum().group(1, 'class')
    var optRed = {
        'reducer': reducer,
        'geometry': geometry,
        'scale': 30,
        'bestEffort': true, 
        'maxPixels': 1e13
    }    
    var areas = pixelArea.reduceRegion(optRed);
    // print(" areas ", areas);
    areas = ee.List(areas.get('groups')).map(function(item) { return convert2featCollection(item)})
    areas = ee.FeatureCollection(areas)    
    return areas
}
// ========================METODOS=============================
// exporta a imagem classificada para o asset
// ============================================================
function processoExportar(ROIsFeat, nameB){    
    var optExp = {
          'collection': ROIsFeat, 
          'description': nameB, 
          'folder': 'stat_gain_loss'          
        }
    Export.table.toDrive(optExp)

    print("salvando ... " + nameB + "..!")    
}

var vis = {
    'cLossGain': {
        min: 0, max: 7, 
        palette: [
            '000000','#ea2b2b', '#f47979', '#f4c979', '#e0e0e0', '#97e507', 
            '#37a704', '#236b02'
        ]
    }
}
var param = {
    'asset_raster_biomas': 'projects/mapbiomas-workspace/AUXILIAR/biomas-raster-41',
    'assetBiomas': 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
    'asset_veg': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/municipios_statisticVeg',
    'asset_semiarido': 'projects/mapbiomas-workspace/AUXILIAR/SemiArido_2024',
}

// var biomeCaat = ee.FeatureCollection(param.assetBiomas).filter(ee.Filter.eq('Bioma', 'Caatinga')).geometry();
var shpterritorio = ee.FeatureCollection(param.asset_semiarido);
var shpPerdaGain = ee.FeatureCollection(param.asset_veg).filterBounds(shpterritorio.geometry());


var teritorrio_raster = shpterritorio.map(function(feat){return feat.set('class', 1)});
teritorrio_raster = teritorrio_raster.reduceToImage(['class'], ee.Reducer.first());

print("show metadata of shp ", shpPerdaGain.limit(10));
print("show size fo shp", shpPerdaGain.size());
print("statisticas da prop Classe ", shpPerdaGain.aggregate_histogram('Classe'));
print("statisticas da prop ClassePctN ",shpPerdaGain.aggregate_histogram('ClassePctN'));

shpPerdaGain = shpPerdaGain.map(set_class_value);
print("statisticas da prop make cLossGain ", 
                shpPerdaGain.aggregate_histogram('cLossGain'))


var dictColor = {
    'Perda maior que 30%': '#ea2b2b',    
    'Perda de 15 a 30%': '#f47979',
    'Perda de 5 a 15%':  '#f4c979',
    'Ganho 5% a Perda 5% (estável)': '#e0e0e0',
    'Ganho de 5 a 15%': '#97e507',
    'Ganho de 15 a 30%': '#37a704',
    'Ganho maior que 30%': '#236b02'
}

Map.addLayer(ee.Image.constant(1), {min: 0, max: 1}, 'mapBase');
Map.addLayer(shpterritorio, {color: '#120012'}, 'Territorio');

var filledOutlines = ee.Image().byte()
                            .paint(shpPerdaGain, 'cLossGain')
                            .paint(shpPerdaGain, 0, 1.5);
filledOutlines = filledOutlines.updateMask(teritorrio_raster)
Map.addLayer(filledOutlines, vis.cLossGain, 'map LossGain');
var npixelArea = ee.Image.pixelArea().divide(10000).updateMask(teritorrio_raster);
var maplossgain = shpPerdaGain.reduceToImage(['cLossGain'], ee.Reducer.first());
maplossgain = maplossgain.updateMask(teritorrio_raster);
var dictArea = calculateArea (maplossgain, npixelArea, shpterritorio.geometry());

print("calculo de áreas por classe ", dictArea);
processoExportar(dictArea, 'estatistica_semiarido_gain_loss')