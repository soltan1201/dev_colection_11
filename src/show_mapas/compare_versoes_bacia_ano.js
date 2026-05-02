// =====================================================================
// Comparação de versões da classificação por bacia e ano
// Mostra v1 vs v2, camada de diferença, e exporta v1→v2 quando falta
// =====================================================================

var palettes = require('users/mapbiomas/modules:Palettes.js');
var palette  = palettes.get('brazil');

// ==== Parâmetros ====
var NBACIA = '7584';
var NYEAR  = 2022;
var ASSET_ROOT = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1';
var ASSET_BACIAS = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions';
var ASSET_MOSAIC = 'LANDSAT/COMPOSITES/C02/T1_L2_32DAY';

var BAND = 'classification_' + NYEAR;

// ==== Geometria da bacia ====
var shpBacia = ee.FeatureCollection(ASSET_BACIAS)
                  .filter(ee.Filter.eq('nunivotto4', NBACIA))
                  .geometry();

Map.centerObject(shpBacia, 9);

// ==== Carrega os dois assets ====
var nameV1 = 'BACIA_' + NBACIA + '_' + NYEAR + '_GTB_col11_BND_fm-v_1';
var nameV2 = 'BACIA_' + NBACIA + '_' + NYEAR + '_GTB_col11_BND_fm-v_2';

var imgV1 = ee.Image(ASSET_ROOT + '/' + nameV1).select(BAND).clip(shpBacia);
var imgV2 = ee.Image(ASSET_ROOT + '/' + nameV2).select(BAND).clip(shpBacia);

// ==== Mosaico de referência ====
var visMosaic = { min: 0.012, max: 0.22, bands: ['red', 'green', 'blue'] };

var mosaicoMax = ee.ImageCollection(ASSET_MOSAIC)
                    .filterDate(NYEAR + '-01-01', NYEAR + '-12-31')
                    .filterBounds(shpBacia)
                    .max();

var mosaicoMediana = ee.ImageCollection(ASSET_MOSAIC)
                        .filterDate(NYEAR + '-01-01', NYEAR + '-12-31')
                        .filterBounds(shpBacia)
                        .median();

// ==== Camada de diferença ====
// 0 = coincide (cinza)  |  1 = só em V1 (vermelho)  |  2 = só em V2 (azul)
var iguais    = imgV1.eq(imgV2);
var soV1      = imgV1.neq(imgV2).and(imgV1.gt(0));
var soV2      = imgV1.neq(imgV2).and(imgV2.gt(0));

var diffLayer = iguais.multiply(0)         // coincide → 0
                .add(soV1.multiply(1))     // só V1    → 1
                .add(soV2.multiply(2));    // só V2    → 2

// Estatísticas de diferença
var stats = diffLayer.updateMask(diffLayer.gt(0)).reduceRegion({
  reducer: ee.Reducer.frequencyHistogram(),
  geometry: shpBacia,
  scale: 30,
  maxPixels: 1e10,
  bestEffort: true
});
print('Histograma de diferenças (1=sóV1, 2=sóV2):', stats);

// Percentual de pixels diferentes
var totalPx = imgV1.gt(0).reduceRegion({
  reducer: ee.Reducer.sum(),
  geometry: shpBacia,
  scale: 30,
  maxPixels: 1e10,
  bestEffort: true
});
print('Total pixels v1:', totalPx);

// ==== Visualizações ====
Map.addLayer(mosaicoMax,     visMosaic, 'Mosaico Max',     false);
Map.addLayer(mosaicoMediana, visMosaic, 'Mosaico Mediana', false);

Map.addLayer(
  imgV1,
  { bands: [BAND], min: 0, max: 75, palette: palette },
  'Classificação V1 (' + NYEAR + ')'
);

Map.addLayer(
  imgV2,
  { bands: [BAND], min: 0, max: 75, palette: palette },
  'Classificação V2 (' + NYEAR + ')'
);

Map.addLayer(
  diffLayer.selfMask(),
  {
    min: 1, max: 2,
    palette: ['FF0000', '0000FF']  // vermelho=sóV1, azul=sóV2
  },
  'Diferença (vermelho=sóV1 | azul=sóV2)'
);

Map.addLayer(
  iguais.updateMask(iguais),
  { palette: ['808080'] },
  'Coincide (cinza)'
);

// Borda da bacia
Map.addLayer(
  ee.Image().byte().paint(ee.FeatureCollection(ASSET_BACIAS)
                            .filter(ee.Filter.eq('nunivotto4', NBACIA)), 1, 2),
  { palette: ['FFFFFF'] },
  'Contorno bacia ' + NBACIA
);

// ==== Exportar V1 como V2 (quando V2 estiver faltando) ====
// Chame esta função manualmente no console caso V2 não exista para a bacia/ano.
function exportarV1comoV2(nbacia, nyear) {
  var bandName = 'classification_' + nyear;
  var srcName  = 'BACIA_' + nbacia + '_' + nyear + '_GTB_col11_BND_fm-v_1';
  var dstName  = 'BACIA_' + nbacia + '_' + nyear + '_GTB_col11_BND_fm-v_2';

  var srcImg = ee.Image(ASSET_ROOT + '/' + srcName);
  var geom   = ee.FeatureCollection(ASSET_BACIAS)
                  .filter(ee.Filter.eq('nunivotto4', nbacia))
                  .geometry();

  // Copia os metadados essenciais e marca como cópia de V1
  var dstImg = srcImg.set({
    'version':      2,
    'source_copy':  'copied_from_v1',
    'id_bacia':     nbacia,
    'year':         nyear,
    'classifier':   'GTB',
    'collection':   '11.0',
    'sensor':       'Landsat',
    'source':       'geodatin'
  });

  Export.image.toAsset({
    image:            dstImg,
    description:      dstName,
    assetId:          ASSET_ROOT + '/' + dstName,
    region:           geom,
    scale:            30,
    maxPixels:        1e13,
    pyramidingPolicy: {'.default': 'mode'}
  });

  print('Task criada: ' + srcName + ' → ' + dstName);
}

// Para exportar V1 → V2, descomente e ajuste bacia/ano:
// exportarV1comoV2('7584', 2022);
// exportarV1comoV2('7564', 1993);
// exportarV1comoV2('7564', 2010);
