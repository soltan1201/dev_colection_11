var asset_grades = 'projects/nexgenmap/SAD_MapBiomas/DL/SHP_grades_BR_35pathces_AllBrV3';
var asset_vetor_biomas_250 = 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil';
var asset_grid_output = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs';

function processoExportar_toDrive(ROIsFeat, name_export){   
    //"""Configura e dispara a tarefa para o Google Drive"""
    var optExp = {
          'collection': ROIsFeat, 
          'description': name_export, 
          'assetId': asset_grid_output + '/' + name_export     
    }
    Export.table.toAsset(optExp);
    print(" ⏳ Tarefa enviada para o Drive: "+ name_export + " ...! ")  
}
var grades_BR = ee.FeatureCollection(asset_grades);
var biomas = ee.FeatureCollection(asset_vetor_biomas_250);
var map_biomas = biomas.reduceToImage(['CD_Bioma'], ee.Reducer.first());

print("show metadados biomas ", biomas);
print(biomas.aggregate_histogram('Bioma'))
var outline = ee.Image().byte().paint({
  featureCollection: grades_BR,
  color: 1,
  width: 2
});

var palette = ['#78d532', '#b58145', '#e5a628', '#497d1e', '#d402f2', '#cdf439'];
Map.addLayer(map_biomas, {palette: palette, min: 1, max: 6}, 'biomas ');
Map.addLayer(outline, {palette: 'FF0000'}, 'grades');
Map.addLayer(grades_BR, {color: 'FF0000'}, 'grades shp', false);

var grades_area_coleta = grades_BR.filterBounds(area_procura);
print("know metadados nova gride ", grades_area_coleta);
var name_file = 'grades_area_pesquisa_caatinga_cerrado';
processoExportar_toDrive(grades_area_coleta, name_file);

//// ===== coleta cerrados 
var grades_area_col_cerr = grades_BR.filterBounds(grid_cerrado)
                          .map(function(feat){return feat.set('classe', 1)});
print("know metadados nova gride Cerrado ", grades_area_col_cerr);
name_file = 'grades_area_coleta_cerrado';
processoExportar_toDrive(grades_area_col_cerr, name_file);

//// ===== coleta Caatinga 
var grades_area_col_caat = grades_BR.filterBounds(grid_caatinga)
                            .map(function(feat){return feat.set('classe', 2)});
print("know metadados nova gride Caatinga ", grades_area_col_caat);
name_file = 'grades_area_coleta_caatinga';
processoExportar_toDrive(grades_area_col_caat, name_file);