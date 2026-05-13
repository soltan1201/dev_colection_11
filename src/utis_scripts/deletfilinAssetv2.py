import ee
import os
import sys
import collections
collections.Callable = collections.abc.Callable
from pathlib import Path
pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
projAccount = get_current_account()
print(f"projetos selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project= projAccount)
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise

def Get_Remove_Array_from_ImgCol(asset_imgcol, vers= 0, janela= 0, lstBacias= [], lstyear= [], play_eliminar= False):

    
    imgCol = ee.ImageCollection(asset_imgcol)
    
    if vers > 0:
        imgCol = imgCol.filter(ee.Filter.eq('version', vers))
    if janela > 0:
        imgCol = imgCol.filter(ee.Filter.eq('janela', janela))    
    if len(lstBacias) > 0:
        imgCol = imgCol.filter(ee.Filter.inList('id_bacias', lstBacias))
    if len(lstyear) > 0:
        imgCol = imgCol.filter(ee.Filter.inList('year', lstyear))
    
    lst_id = imgCol.reduceColumns(ee.Reducer.toList(), ['system:index']).get('list').getInfo()
    print(f'we will eliminate {len(lst_id)} file image from {asset_imgcol} ')
    
    for cc, idss in enumerate(lst_id):    
        path_ = str(asset_imgcol + '/' + idss)    
        print (f"... eliminando ❌ ... item 📍{cc + 1}/{len(lst_id)} : {idss}  ▶️ ")    
        try:
            if play_eliminar:
                ee.data.deleteAsset(path_)
                print(" > " , path_)
        except:
            print(f" {path_} -- > NAO EXISTE!")


# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/Classifier/Classify_fromMMBV2YY'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/Classifier/ClassifyV2YY'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/Classifier/ClassifyV2Y'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/Classifier/ClassifyVA'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Gap-fill'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Spatials_all'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Spatials_int'
# asset= 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/transition'
# asset= 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Merger'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Spatials'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Temporal'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/TemporalCC'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Frequency'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/Classifier/ClassifyV2YX'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/Classifier/ClassifyV1'
# asset= 'projects/mapbiomas-brazil/assets/WATER/COLLECTION-4/classification-monthly'
# asset= 'projects/nexgenmap/MapBiomas2/LANDSAT/DEGRADACAO/LAYER_SOIL_EMBED'
# asset= 'projects/nexgenmap/MapBiomas2/LANDSAT/DEGRADACAO/mosaics-harmonico'
# asset = 'projects/nexgenmap/SAD_MapBiomas/Caatinga/ndfia_min'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_Savana'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Gap-fill'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/Classifier/Classify_fromEEMV1'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/POS-CLASS/Spatial'
# asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/MergerV6'
asset = 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/POS-CLASS/Frequency'
lsBacias = []

eliminar_files = False
# lstyear=[2025], lstBacias=lsBacias, 
Get_Remove_Array_from_ImgCol(asset, vers= 3, play_eliminar= eliminar_files)  