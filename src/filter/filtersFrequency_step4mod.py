#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
#SCRIPT DE CLASSIFICACAO POR BACIA
#Produzido por Geodatin - Dados e Geoinformacao
#DISTRIBUIDO COM GPLv2
'''

import ee
import os 
import gee
import json
import csv
import copy
import sys
import math
import arqParametros as arqParams 
import collections
collections.Callable = collections.abc.Callable
try:
    ee.Initialize()
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise


class processo_filterFrequence(object):

    options = {
        'output_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/POS-CLASS/Frequency',
        'input_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/POS-CLASS/TemporalAnt',
        # 'input_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/TemporalV3',
        # 'input_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Gap-fill',
        'asset_bacias_buffer' : 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions',
        'classMapB':     [3, 4, 5, 9, 12, 13, 15, 18, 19, 20, 21, 22, 23, 24, 25, 26, 29, 30, 31, 32, 33, 36, 39, 40, 41, 46, 47, 48, 49, 50, 62, 75],
        'classNew':      [3, 4, 3, 3, 12, 12, 15, 19, 19, 19, 21, 25, 25, 25, 25, 33, 29, 25, 33, 12, 33, 36, 19, 19, 19, 36, 36, 36,  4, 12, 19, 25],
        # 'classNew':    [3, 4, 3, 3, 12, 12, 21, 21, 21, 21, 21, 25, 25, 25, 25, 33, 29, 25, 33, 12, 33, 21, 21, 21, 21, 21, 21, 21,  4, 12, 21, 25],
        'classNat':      [1, 1, 1, 1,  1,  1,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  1,  0,  0,  0,  0,  0,  0,  0,  1,  1,  0,  0],  
        'versionInput': 1,
        'janela_input': 5,
        'num_classes': 10,  # 7, 10   
        'last_year' : 2025,
        'first_year': 1985
        }

    def __init__(self, nameBacia, nmodel):
                self.id_bacias = nameBacia
        self.versoutput = 1
        self.versinput = 1

        self.step = 1
        self.geom_bacia = ee.FeatureCollection(self.options['asset_bacias_buffer']).filter(
                                                   ee.Filter.eq('nunivotto4', nameBacia))  
        geomBacia = self.geom_bacia.map(lambda f: f.set('id_codigo', 1))
        self.bacia_raster = geomBacia.reduceToImage(['id_codigo'], ee.Reducer.first()).gt(0)            
        self.geom_bacia = self.geom_bacia.geometry()     

        # # filterSP_BACIA_778_V1     
        # if "Spatial" in self.options['input_asset']:
        #     self.name_imgClass = 'filterSP_BACIA_' + nameBacia + f"_{self.nmodel}_V" + str(self.versinput) + '_step' + str(self.step)
        # elif "Gap-fill" in self.options['input_asset']:
        #     self.name_imgClass = 'filterGF_BACIA_' + nameBacia + "_GTB_V" + str(self.versinput)
        # else:
        #     self.name_imgClass = 'filterTP_BACIA_' + nameBacia+ f"_GTB_J{janela}_V" + str(self.versinput)
        
        
        # self.imgClass = ee.Image(self.options['input_asset'] + "/" + self.name_imgClass)   

        self.imgClass =(ee.ImageCollection(self.options['input_asset'])
                                    .filter(ee.Filter.eq('version', self.versionInput))
                                    .filter(ee.Filter.eq('num_class', self.options['num_classes']))
                                    # só o Gap-fill tem id_bacia o resto tem id_bacias
                                    .filter(ee.Filter.eq('id_bacias', nameBacia))                                      
                                    # .first()
                        )        
        # print(" total of  image class ", self.imgClass.size().getInfo())
        if  self.options['janela_input'] > 0:
            self.imgClass = self.imgClass.filter(ee.Filter.eq('janela',  self.options['janela_input']))
        self.imgClass = self.imgClass.first()
        # print("numero de bandas ", self.imgClass.bandNames().getInfo())
        self.lstbandNames = ['classification_' + str(yy) for yy in range(self.options['first_year'], self.options['last_year'] + 1)]
        self.years = [yy for yy in range(self.options['first_year'], self.options['last_year'] + 1)]

        self.imgReclass = ee.Image().byte()
        for yband in self.lstbandNames:
            img_tmp = self.imgClass.select(yband)
            img_tmp = img_tmp.remap(self.options['classMapB'], self.options['classNew'])
            self.imgReclass = self.imgReclass.addBands(img_tmp.rename(yband))

        self.imgReclass = self.imgReclass.select(self.lstbandNames)

        ##### ////////Calculando frequencias /////////////#####
        #######################################################
        #############  General rule in Years ##################
        exp = '100*((b(0)  + b(1)  + b(2)  + b(3)  + b(4)  + b(5)  + b(6)  + b(7)  + b(8)  + b(9)  + b(10) + b(11)'
        exp +=    '+ b(12) + b(13) + b(14) + b(15) + b(16) + b(17) + b(18) + b(19) + b(20) + b(21) + b(22) + b(23)'
        exp +=    '+ b(24) + b(25) + b(26) + b(27) + b(28) + b(29) + b(30) + b(31) + b(32) + b(33) + b(34) + b(35)'
        exp +=    '+ b(36) + b(37) + b(38) + b(39) + b(40))/41)'

        self.florest_frequence = self.imgClass.eq(3).expression(exp)
        self.savana_frequence = self.imgClass.eq(4).expression(exp)
        self.grassland_frequence = self.imgClass.eq(12).expression(exp) 
        natural = self.imgReclass.expression(exp)
        # construindo a mascara Natural da serie completa 
        self.mask_natural = natural.eq(100)

        ## contruindo a regra de mudança para as classes naturais ####
        ### esta camada será de uma única banda com os pixels em 3, 4 ou 12 para as
        ### correspondentes classes e o resto em 0 
        ###########  /////Mapa base////// ############
        # atualizando os pixels que serão convertidos a formação campestre
        self.vegetation_map = ee.Image(0).where(self.mask_natural.eq(1).And(self.grassland_frequence.gt(60)), 12)
        # addicionando todos os pixels que serão convertidos em formação florestal 
        self.vegetation_map = self.vegetation_map.where(self.mask_natural.eq(1).And(self.florest_frequence.gt(70)), 3)
        # addicionando todos os pixels que serão convertidos em formação savanica 
        self.vegetation_map = self.vegetation_map.where(self.mask_natural.eq(1).And(self.savana_frequence.gte(80)), 4)
        self.vegetation_map = self.vegetation_map.updateMask(self.vegetation_map.gt(0))
        # sys.exit()
        # self.maskpropNatural = self.imgClass.eq(3).Or(self.imgClass.eq(4)).Or(self.imgClass.eq(12)).expression(exp)
        
       
    def applyStabilityNaturalClass(self, bandYearCourrent):        

        ############## get frequency   #######################
        mapCourrent = self.imgClass.select(bandYearCourrent)
        maskNatCourrent  = mapCourrent.eq(3).Or(mapCourrent.eq(4)).Or(mapCourrent.eq(12))            
        
        ###########  /////Mapa base////// ############
        # todo o quye esta na
        vegetation_map = ee.Image(0).where(maskNatCourrent.eq(1).And(self.grassland_frequence.gt(80)), 12)
        vegetation_map = vegetation_map.where(maskNatCourrent.eq(1).And(self.florest_frequence.gt(80)), 3)
        vegetation_map = vegetation_map.where(maskNatCourrent.eq(1).And(self.savana_frequence.gte(30)), 4)
                                        

        maskNatCourrent = maskNatCourrent.updateMask(vegetation_map.gt(0))
        img_output = mapCourrent.where(maskNatCourrent, vegetation_map)

        return img_output.clip(self.geom_bacia).rename(bandYearCourrent)

    def iterandoFilterbyYear(self):

        for cc, bandYY in enumerate(self.lstbandNames):            
            imgtempBase = self.applyStabilityNaturalClass(bandYY)
            if cc == 0:
                rasterFinal = imgtempBase
            else:
                rasterFinal = rasterFinal.addBands(imgtempBase)

        rasterFinal = rasterFinal.set(
                            'version',  int(self.versoutput), 
                            'biome', 'CAATINGA',
                            'type_filter', 'frequence',
                            'from', 'Gap-fill',
                            'collection', '10.0',
                            'model', self.nmodel,
                            'step', self.step,
                            'id_bacia', self.id_bacias,
                            'sensor', 'Landsat',
                            'system:footprint' , self.geom_bacia
                        )

        rasterFinal = ee.Image.cat(rasterFinal)
        name_toexport = 'filterFQ_BACIA_'+ str(self.id_bacias) + f"_{self.nmodel}_V" + str(self.versoutput) + '_' + str(self.step)
        self.processoExportar(rasterFinal, name_toexport)    

    ##### exporta a imagem classificada para o asset  ###
    def processoExportar(self, mapaRF,  nomeDesc):
        
        idasset =  self.options['output_asset'] + nomeDesc
        optExp = {
            'image': mapaRF, 
            'description': nomeDesc, 
            'assetId':idasset, 
            'region': self.geom_bacia.getInfo()['coordinates'],
            'scale': 30, 
            'maxPixels': 1e13,
            "pyramidingPolicy":{".default": "mode"}
        }
        task = ee.batch.Export.image.toAsset(**optExp)
        task.start() 
        print("salvando ... " + nomeDesc + "..!")
        # print(task.status())
        for keys, vals in dict(task.status()).items():
            print ( "  {} : {}".format(keys, vals))

param = {      
    'numeroTask': 6,
    'numeroLimit': 42,
    'conta' : {
        '0': 'caatinga01',
        '2': 'caatinga02',
        '4': 'caatinga03',
        '6': 'caatinga04',
        '8': 'caatinga05',        
        '10': 'solkan1201',    
        '12': 'solkanGeodatin',
        '14': 'diegoUEFS' ,
        '20': 'superconta'   
    }
}

#============================================================
#========================METODOS=============================
#============================================================

def gerenciador(cont):

    #=====================================#
    # gerenciador de contas para controlar# 
    # processos task no gee               #
    #=====================================#
    numberofChange = [kk for kk in param['conta'].keys()]
    
    if str(cont) in numberofChange:

        print("conta ativa >> {} <<".format(param['conta'][str(cont)]))        
        gee.switch_user(param['conta'][str(cont)])
        gee.init()        
        gee.tasks(n= param['numeroTask'], return_list= True)        
    
    elif cont > param['numeroLimit']:
        return  0
    
    cont += 1    
    return cont


listaNameBacias = [
    '745','741', '7422','744','746','751','752', '7492','7421',
    '753', '754','755','756','757','758','759','7621','7622','763',
    '764','765','766','767','771','772','773', '7741','7742','775',
    '776','76111','76116','7612','7613','7614','7615',  '777','778',
    '7616','7617','7618','7619'
]

# listaNameBacias = [
#     '76111', '756', '757', '758', '754', '7614', '7421'
# ]

# input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/POS-CLASS/TemporalV3/'
input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/POS-CLASS/FrequencyV3'
# input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/POS-CLASS/SpatialV3'
cont = 0
version = 31
modelo = 'GTB'
# imgtmp = ee.ImageCollection(input_asset).filter(ee.Filter.eq('version', version))
# print(" " ,imgtmp.size().getInfo())
# sys.exit()
knowMapSaved = False
listBacFalta = []

for cc, idbacia in enumerate(listaNameBacias[1:]):
    if knowMapSaved:
        try:
            imgtmp = ee.ImageCollection(input_asset).filter(
                                ee.Filter.eq('id_bacia', idbacia)).filter(
                                    ee.Filter.eq('version', version))
            print(f" {cc} 📢 ", imgtmp.first().get("system:index").getInfo() , " < > ", len(imgtmp.first().bandNames().getInfo()) )
            # print("loading ", nameMap, " ", len(imgtmp.bandNames().getInfo()), "bandas ")
        except:
            listBacFalta.append(idbacia)
    else:
        if idbacia not in listBacFalta:
            print(" ")
            print(f"--------- 📢 #{cc} PROCESSING BACIA {idbacia} ---------")
            print("----------------------------------------------")
            # cont = gerenciador(cont)
            aplicando_FrequenceFilter = processo_filterFrequence(idbacia, modelo)
            aplicando_FrequenceFilter.iterandoFilterbyYear()

if knowMapSaved:
    print("lista de bacias que faltam \n ",listBacFalta)
    print("total ", len(listBacFalta))