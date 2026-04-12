#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
#SCRIPT DE CLASSIFICACAO POR BACIA
#Produzido por Geodatin - Dados e Geoinformacao
#DISTRIBUIDO COM GPLv2
'''

import ee
import os 
import time
import copy
import sys
from pathlib import Path
import collections
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[0])
sys.path.append(pathparent)
print("parents ", pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
from gee_tools import *
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

import ee
import datetime
import sys
import os
import copy


import ee
import datetime
import sys
import os
import copy

class processo_filterTemporal(object):

    options = {
            # 'output_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Frequency',
            'output_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Temporal',
            # 'input_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/POS-CLASS/SpatialV3',
            'input_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/TemporalA',
            # 'input_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Gap-fill',
            'asset_bacias_buffer' : 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions',
            'classMapB': [3, 4, 5, 6, 9, 11, 12, 13, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32, 33, 35, 36, 39, 40, 41, 46, 47, 48, 49, 50, 62],
            'classNew':  [4, 4, 4, 4, 4,  4,  4,  4, 21, 21, 21, 21, 21, 22, 22, 22, 22, 33, 29, 22, 33,  4, 33, 21, 21, 21, 21, 21, 21, 21, 21,  4,  4, 21],
            'classNat':  [1, 1, 1, 1, 1,  1,  1,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  1,  0],
            'last_year' : 2024,
            'first_year': 1985,
            'janela_bef' : 3, # This option name might be confusing now with forward window
            'step': 1
        }

    def __init__(self, name_bacia):
        self.id_bacias = name_bacia
        self.versoutput = 5
        self.versionInput = 5
        self.geom_bacia = (ee.FeatureCollection(self.options['asset_bacias_buffer'])
                    .filter(ee.Filter.eq('nunivotto4', name_bacia))
        )
        geomBacia = self.geom_bacia.map(lambda f: f.set('id_codigo', 1))
        self.bacia_raster = geomBacia.reduceToImage(['id_codigo'], ee.Reducer.first()).gt(0)
        self.geom_bacia = self.geom_bacia.geometry()

        self.years = [yy for yy in range(self.options['first_year'], self.options['last_year'] + 1)]
        self.lstbandNames = ['classification_' + str(yy) for yy in range(self.options['first_year'], self.options['last_year'] + 1)]

        # Load the image collection and get the first image (multi-band image)
        self.imgClass = (
                    ee.ImageCollection(self.options['input_asset'])
                        .filter(ee.Filter.eq('version', self.versionInput))
                        .filter(ee.Filter.eq('id_bacias', name_bacia ))
                )
        # The 'janela' filter here might be specific to the input asset 'TemporalA'.
        # Keep it if it's necessary to select the correct input image.
        if 'Temporal' in self.options['input_asset']:
            self.imgClass = self.imgClass.filter(ee.Filter.eq('janela', self.options['janela_bef']))

        # Assuming the input is indeed a single image with annual bands after filtering
        self.imgClass = self.imgClass.first()
        print("Input Image loaded with bands: ", self.imgClass.bandNames().getInfo())

    # Renomeia as bandas reclassificadas para um padrão consistente
    def reclass_natural_Antropic(self, raster_maps, listYYbnd):
        # Select the bands for the current window
        window_bands = raster_maps.select(listYYbnd)

        # Apply the remapping to create 'natural'/'antropic' bands
        remapped_bands = window_bands.remap(self.options['classMapB'], self.options['classNat'])

        # Rename the remapped bands to indicate they are reclassified
        # This getInfo() call is necessary to get the list of band names on the client side
        # to create the new names list. This is generally acceptable for getting metadata.
        original_band_names = window_bands.bandNames().getInfo()
        remapped_band_names = [f'{band_name}_reclass' for band_name in original_band_names]

        remapped_image = remapped_bands.rename(remapped_band_names)

        # Cast to the same data type as the original bands to ensure consistency
        # Get band type from the first band of the original window selection
        first_band_type = raster_maps.select([listYYbnd[0]]).first().bandTypes().getInfo()
        remapped_image = remapped_image.cast(first_band_type)

        return remapped_image

    # New helper function to calculate the mask for a window with at least 2 bands
    def _calculate_mask_for_window(self, imagem_reclassificada, listaBND_reclassificada_ee, valor_cc):
        """
        Calculates the temporal mask for a window with at least 2 bands based on
        reclassified values. This function is intended for server-side execution.

        Args:
            imagem_reclassificada (ee.Image): The image with reclassified bands for the window.
            listaBND_reclassificada_ee (ee.List): List of reclassified band names in the window.
            valor_cc (int): The reclassified class value to check (e.g., 1 for Natural).

        Returns:
            ee.Image: A binary mask image (1 where conditions are met, 0 otherwise).
        """
        def condition_fails (lstBND_reclassificada_ee):
            # Otherwise, calculate based on intermediate bands
            # Select the intermediate reclassified bands for counting
            intermediate_bands_reclass_names = lstBND_reclassificada_ee.slice(1, -1),
            intermediate_bands_reclass = imagem_reclassificada.select(intermediate_bands_reclass_names),
            windows = window_size_ee.subtract(2), # Number of intermediate bands

            maskCount = intermediate_bands_reclass.reduce(ee.Reducer.sum()),

            # Apply the intermediate condition based on valor_cc
            # If valor_cc == 1 (Natural), check if sum of natural (1s) in intermediate is > 0
            intermediate_condition = ee.Algorithms.If(
                valor_cc == 1,
                maskCount.gt(0),
                ee.Algorithms.If( # If not valor_cc == 1, check valor_cc == 0
                        valor_cc == 0,
                        maskCount.lt(windows.subtract(2)), # Sum of 1s < num_intermediate - 2
                        ee.Image(0) # Default to false if valor_cc is not 0 or 1
                )
            ),
            # Return the intermediate condition result            
            return intermediate_condition

        first_band_reclass_name = listaBND_reclassificada_ee.get(0)
        last_band_reclass_name = listaBND_reclassificada_ee.get(-1)

        first_band_reclass = imagem_reclassificada.select([first_band_reclass_name])
        last_band_reclass = imagem_reclassificada.select([last_band_reclass_name])

        # Start with the condition that the first and last bands match valor_cc
        initial_mask = first_band_reclass.eq(valor_cc).And(last_band_reclass.eq(valor_cc))

        # Handle intermediate bands logic only if window has at least 3 bands
        window_size_ee = listaBND_reclassificada_ee.size()
        intermediate_mask_val = ee.Algorithms.If(
            window_size_ee.lt(3),
            ee.Image(1), # If less than 3 bands, intermediate condition is considered true
            ee.Image(condition_fails())
        )

        # Combine initial mask with intermediate mask condition
        final_mask = initial_mask.And(ee.Image(intermediate_mask_val))

        return final_mask


    def mask_of_years(self, valor_cc, imagem_reclassificada, listaBND_reclassificada):
        """
        Determines the temporal mask for a given reclassified image window.
        Returns a mask of zeros for windows with less than 2 bands.

        Args:
            valor_cc (int): The reclassified class value to check (e.g., 1 for Natural).
            imagem_reclassificada (ee.Image): The image with reclassified bands for the window.
            listaBND_reclassificada (list): Python list of reclassified band names in the window.

        Returns:
            ee.Image: A binary mask image (1 where conditions are met, 0 otherwise).
        """
        listBND_ee = ee.List(listaBND_reclassificada)
        window_size_ee = listBND_ee.size() # Get current window size

        # If the window has less than 2 bands, return a zero mask.
        # Otherwise, call the helper function to calculate the mask.
        mask = ee.Algorithms.If(
            window_size_ee.lt(2),
            ee.Image(0), # Return a mask of zeros if window is too small
            self._calculate_mask_for_window(imagem_reclassificada, listBND_ee, valor_cc)
        )

        return ee.Image(mask).updateMask(mask) # Ensure output is an Image and apply mask


    def applyTemporalFilter(self):
        """
        Applies a moving temporal filter of 6 years (forward window) to the image,
        band by band, using reclassification, masking, and blending logic.
        """
        id_class_natural = 1 # Assuming 1 is the 'natural' class after reclassification
        mjanela = 6
        print(f"--------- processing  janela {mjanela} (forward) ----------")

        # Create a list of year indices to map over
        year_indices = ee.List.sequence(0, len(self.years) - 1)

        def process_year_window(year_index):
            """
            Server-side function to process a single year's band based on a 6-year forward window.
            """
            year_index = ee.Number(year_index)

            # Determine the band indices for the 6-year window starting at the current year index
            # The window includes the current year and the 5 subsequent years.
            window_start_index = year_index
            window_end_index_exclusive = year_index.add(mjanela) # Slice end index is exclusive

            # Get the band names for the current window
            # Use slice to get bands from start_index up to (but not including) end_index
            window_band_names = ee.List(self.lstbandNames).slice(window_start_index, window_end_index_exclusive)

            # Select the bands for the current window from the original multi-band image
            current_window_image = self.imgClass.select(window_band_names)

            # Reclassify the bands in the current window to 'natural'/'antropic'
            remapped_window_image = self.reclass_natural_Antropic(current_window_image, window_band_names)

            # Generate reclassified band names for masking server-side
            remapped_window_band_names = window_band_names.map(lambda name: ee.String(name).cat('_reclass'))

            # Apply the masking logic based on the reclassified bands
            # Assuming valor_cc=1 (Natural) filter is needed for blending
            mask_natural = self.mask_of_years(id_class_natural, remapped_window_image, remapped_window_band_names)

            # Get the original classification band for the current year
            current_year_band_name = ee.List(self.lstbandNames).get(year_index)
            original_current_year_band = self.imgClass.select([current_year_band_name])

            # Get the original classification band for the last year of the window
            # Need to check if the window has at least one band before getting the last element
            original_last_year_of_window_band = ee.Algorithms.If(
                window_band_names.size().gt(0),
                self.imgClass.select([window_band_names.get(-1)]),
                # If window is empty (should not happen with slice starting at year_index >= 0),
                # return a masked image or handle appropriately. Returning a masked zero image for now.
                ee.Image(0).updateMask(0)
            )

            # Apply the blending logic: replace pixels in the current year's band with the value
            # from the last year of the window where the natural mask is true.
            # Start with the original current year band
            filtered_band = original_current_year_band

            # Apply blending based on the natural mask
            filtered_band = filtered_band.blend(mask_natural.selfMask().multiply(original_last_year_of_window_band))

            # Rename the output band to the original year's band name
            filtered_band = filtered_band.rename([current_year_band_name])

            # Ensure consistent data type for the output band
            # Get band type from the original current year band
            original_band_type = original_current_year_band.bandTypes().getInfo()
            filtered_band = filtered_band.cast(original_band_type)

            return filtered_band.copyProperties(original_current_year_band, ['system:time_start']) # Keep time property


        # Map the server-side function over the list of year indices
        # The result will be an ee.List of images, each containing one filtered band
        filtered_bands_list = year_indices.map(process_year_window)

        # Convert the list of images (single-band each) into a single multi-band image
        filtered_collection = ee.ImageCollection(filtered_bands_list)

        # Combine the single-band images into a multi-band image.
        # Use toBands() to stack them. The band names will be the original year names
        # because we renamed them within the mapped function.
        imgOutput = filtered_collection.toBands()

        # The band names from toBands are usually '0', '1', '2', etc. We need to rename them
        # back to the original 'classification_YYYY' names in the correct order.
        # This renaming is crucial as toBands() does not preserve original band names from the list.
        imgOutput = imgOutput.select(imgOutput.bandNames(), self.lstbandNames)


        # Apply the basin mask and set properties (this part remains similar)
        imgOutput = (imgOutput.updateMask(self.bacia_raster)
                        .set(
                            'version',  self.versoutput,
                            'id_bacias', self.id_bacias,
                            'biome', 'CAATINGA',
                            'type_filter', 'temporal',
                            'collection', '10.0',
                            'janela', mjanela, # Update janela property
                            'sensor', 'Landsat',
                            'system:footprint' , self.geom_bacia
                        ))

        name_toexport = f"filterTP_BACIA_{self.id_bacias}_GTB_J{mjanela}_V{self.versoutput}"
        self.processoExportar(imgOutput, name_toexport, self.geom_bacia)

        # sys.exit() # Keep this if you want to stop after one basin for testing

    # exporta a imagem classificada para o asset
    def processoExportar(self, mapaRF,  nomeDesc, geom_bacia):
        # This part remains similar
        idasset =  os.path.join(self.options['output_asset'], nomeDesc)
        optExp = {
            'image': mapaRF,
            'description': nomeDesc,
            'assetId': idasset,
            'region': geom_bacia,
            'scale': 30,
            'maxPixels': 1e13,
            "pyramidingPolicy":{".default": "mode"}
        }
        task = ee.batch.Export.image.toAsset(**optExp)
        task.start()
        print("salvando ... " + nomeDesc + "..!")
        # Note: Getting status with getInfo() inside a loop can be slow.
        # You might want to manage tasks and check status separately after starting all exports.
        # for keys, vals in dict(task.status()).items():
        #     print ( "  {} : {}".format(keys, vals))




param = {      
    'numeroTask': 6,
    'numeroLimit': 20,
    'conta' : {
        '0': 'caatinga01',
        '4': 'caatinga02',
        '6': 'caatinga03',
        '8': 'caatinga04',
        '10': 'caatinga05',        
        '12': 'solkan1201',    
        '14': 'solkanGeodatin',
        '16': 'superconta'      
    }
}
relatorios = open("relatorioTaskXContas.txt", 'a+')
#============================================================
#========================METODOS=============================
#============================================================
def gerenciador(cont):    
    #=====================================
    # gerenciador de contas para controlar 
    # processos task no gee   
    #=====================================
    numberofChange = [kk for kk in param['conta'].keys()]
    print(numberofChange)
    
    if str(cont) in numberofChange:
        
        switch_user(param['conta'][str(cont)])
        projAccount = get_project_from_account(param['conta'][str(cont)])
        try:
            ee.Initialize(project= projAccount) # project='ee-cartassol'
            print('The Earth Engine package initialized successfully!')
        except ee.EEException as e:
            print('The Earth Engine package failed to initialize!') 

        # tasks(n= param['numeroTask'], return_list= True) 
        relatorios.write("Conta de: " + param['conta'][str(cont)] + '\n')

        tarefas = tasks(
            n= param['numeroTask'],
            return_list= True)
        
        for lin in tarefas:            
            relatorios.write(str(lin) + '\n')
    
    elif cont > param['numeroLimit']:
        return 0
    cont += 1    
    return cont


listaNameBacias = [
    '765', '7544', '7541', '7411', '746', '7591', '7592', 
    '761111', '761112', '7612', '7613', '7614', '7615', 
    '771', '7712', '772', '7721', '773', '7741', '7746', '7754', 
    '7761', '7764',   '7691', '7581', '7625', '7584', '751',     
     '7616', '745', '7424', '7618', '7561', '755', '7617', 
    '7564', '7422', '76116', '7671', '757', '766', '753', 
    '764', '7619', '7443', '7438', '763', '7622', '752'
]

# listaNameBacias = ['7411', '7422', '751', '752', '753', '7541' ]
print("ver quantidad ", len(listaNameBacias))
# sys.exit()

lstBacias = []
changeAcount = False
lstqFalta =  []
cont = 16
# input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/POS-CLASS/Estavel'
# input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/POS-CLASS/Gap-fillV2'
# input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Gap-fill'
# input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Frequency'
input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Temporal'
if changeAcount:
    cont = gerenciador(cont)
version = 5
janela = 3
modelo = 'GTB'
listBacFalta = []
knowMapSaved = False
show_interval = True
for cc, idbacia in enumerate(listaNameBacias[:]):   
    if knowMapSaved:
        try:
            nameMap = f"filterTP_BACIA_{idbacia}_GTB_J{janela}_V{version}"
            # # nameMap = 'filterSP_BACIA_'+ str(idbacia) + "_V" + str(version)
            # print(nameMap)
            imgtmp = ee.Image(os.path.join(input_asset, nameMap))
            # imgtmp = (ee.ImageCollection(input_asset)
            #                 # .filter(ee.Filter.eq('version', version))
            #                 .filter(ee.Filter.eq('id_bacia', idbacia ))
            #                 .first()
            #     )
            # print("know how many images exist ", imgtmp.size().getInfo())
            print(f" 👀> {cc} loading {imgtmp.get('system:index').getInfo()}", len(imgtmp.bandNames().getInfo()), "bandas ✅ ")
        except:
            listBacFalta.append(idbacia)
    else: 
        if idbacia not in lstBacias:
            # cont = gerenciador(cont)            
            print("----- PROCESSING BACIA {} -------".format(idbacia)) 
            aplicando_TemporalFilter = processo_filterTemporal(idbacia)            
            aplicando_TemporalFilter.applyTemporalFilter()
            if cc == 0:
                show_interval = False
