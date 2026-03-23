#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
#SCRIPT DE CLASSIFICACAO POR BACIA
#Produzido por Geodatin - Dados e Geoinformacao
#DISTRIBUIDO COM GPLv2
# https://code.earthengine.google.com/0c432999045898bb6e40c1fb7238d32f
'''

import ee
import os 
import gee
import json
import csv
import copy
import time
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


class processo_filterTemporal(object):

    options = {
            'asset_caat_buffer': 'users/CartasSol/shapes/caatinga_buffer5km',
            'asset_layer_florest': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/Classifier/toExportYY',         
            'last_year' : 2023,
            'first_year': 1985,
            'janela' : 5,
            'step': 1
        }

    def __init__(self):
        # self.id_bacias = nameBacia
        self.versoutput = 38
        self.versionInput = 38
        self.geom_bacia = ee.FeatureCollection(self.options['asset_caat_buffer']).geometry()              
        self.years = [yy for yy in range(self.options['first_year'], self.options['last_year'] + 1)]  
        # self.years = [kk for kk in years.sort(reverse= False)]
        self.lstbandNames = ['classification_' + str(yy) for yy in range(self.options['last_year'], self.options['first_year'] - 1, -1)]
        
        print("lista de anos ", self.years)
        self.lstBandFinal =  ['classification_' + str(yy) for yy in self.years]
        print("lista de anos ", self.lstBandFinal)

        self.colectAnos = [self.mapeiaAnos(
                                ano, self.options['janela'], self.years) for ano in self.years]


        # sys.exit()
    ################### CONJUNTO DE REGRAS PARA CONSTRUIR A LISTA DE BANDAS ##############
    def regra_primeira(self, jan, delt, lstYears):
        return lstYears[1 : delt + 1] + [lstYears[0]] + lstYears[delt + 1 : jan]
    def regra_primeiraJ4(self, jan, delt, lstYears):
        return [lstYears[1]] + [lstYears[0]] + lstYears[delt : jan]
    def regra_ultima(self, jan, delt, lstYears):
        print(lstYears[-1 * jan : ])
        return lstYears[-1 * jan : -1 *(delt + 1)] + [lstYears[-1]] + lstYears[-1 * (delt + 1) : -1]    
    def regra_segundo_stepJ5(self, jan, delt, lstYears):
        return [lstYears[0]] + [lstYears[1]] + lstYears[delt : jan]
    def regra_antespenultimo_stepJ5(self, jan, delt, lstYears):
        return [lstYears[-5], lstYears[-3]] + [lstYears[-4]] + lstYears[-2:]
    def regra_penultimo_stepJ5(self, jan, delt, lstYears):
        return [lstYears[-5], lstYears[-2]] + lstYears[-4: -2] + [lstYears[-1]]
    def regra_ultimo_stepJ5(self, jan, delt, lstYears):
        return [lstYears[-5], lstYears[-1]] + lstYears[-4: -1]    
    def regra_penultimo_stepJ4(self, jan, delt, lstYears):
        return [lstYears[-1 * jan]] + [lstYears[-2]] + [lstYears[-3]]  + [lstYears[-1]] 
    # retorna uma lista com as strings referentes a janela dada, por exemplo em janela 5, no ano 1Nan999, o metodo retornaria
    # ['classification_1997', 'classification_1998', 'classification_1999', 'classification_2000', 'classification_2001']
    # desse jeito pode-se extrair as bandas referentes as janelas

    def mapeiaAnos(self, ano, janela, anos):
        lsBandAnos = ['classification_'+str(item) for item in anos]
        # print("ultimo ano ", anos[-1])
        indice = anos.index(ano)
        delta = int(janela / 2)
        resto = int(janela % 2)
        ######### LIST OF BANDS FOR WINDOWS 3 #######################
        if janela == 3:
            if ano == self.options['first_year']:
                return self.regra_primeira(janela, delta, lsBandAnos)
            elif ano == anos[-1]: # igual a ultimo ano 
                return self.regra_ultima(janela, delta, lsBandAnos)
            else:
                return lsBandAnos[indice - delta: indice + delta + resto]
        ######### LIST OF BANDS FOR WINDOWS 4 #######################
        elif janela == 4:
            if ano == self.options['first_year']:
                return self.regra_primeiraJ4(janela, delta, lsBandAnos)
            elif ano == anos[1]:
                return lsBandAnos[:janela]
            elif ano == anos[-2]:
                return self.regra_penultimo_stepJ4(janela, delta, lsBandAnos)
            elif ano == anos[-1]:
                return self.regra_ultima(janela, delta, lsBandAnos)
            else:
                return lsBandAnos[indice - 1: indice + delta + 1]
        ######### LIST OF BANDS FOR WINDOWS 3 #######################
        elif janela == 5:
            if ano == self.options['first_year']:
                return self.regra_primeiraJ4(janela, delta, lsBandAnos)
            elif ano == anos[1]:
                return self.regra_segundo_stepJ5(janela, delta, lsBandAnos)
            elif ano == anos[-3]:
                return self.regra_antespenultimo_stepJ5(janela, delta, lsBandAnos)
            elif ano == anos[-2]:
                return self.regra_penultimo_stepJ5(janela, delta, lsBandAnos)
            elif ano == anos[-1]:
                return self.regra_ultimo_stepJ5(janela, delta, lsBandAnos)  
            else:                  
                return lsBandAnos[indice - 1: indice + 2 * delta]    
           
    def mask_3_years (self, valor, imagem):
        #### https://code.earthengine.google.com/1f9dd3ab081d243fa9d7962e06348579
        imagem = ee.Image(imagem)
        mmask = imagem.select([0]).eq(valor).And(
                    imagem.select([1]).neq(valor)).And(
                        imagem.select([2]).eq(valor)).unmask(0)    
        muda_img = imagem.select([1]).mask(mmask.eq(1)).where(mmask.eq(1), valor)
        return imagem.select([1]).blend(muda_img)

    def mask_4_years (self, valor, imagem):
        imagem = ee.Image(imagem)  
        # print("    === > ", imagem.bandNames().getInfo())      
        mmask = imagem.select([0]).eq(valor).And(
                    imagem.select([1]).neq(valor)).And(
                        imagem.select([2]).neq(valor)).And(
                            imagem.select([3]).eq(valor))
        
        muda_img = imagem.select([1]).mask(mmask.eq(1)).where(mmask.eq(1), valor) 
        muda_img_post = imagem.select([2]).mask(mmask.eq(1)).where(mmask.eq(1), valor) 
        # print("  <> ", imagem.select([2]).bandNames().getInfo())
        img_out = imagem.select([1]).blend(muda_img).addBands(imagem.select([2]).blend(muda_img_post))
        return img_out

    def mask_5_years (self, valor, imagem):
        # print("imagem bandas ", imagem.bandNames().getInfo())
        imagem = ee.Image(imagem)
        mmask = imagem.select([0]).eq(valor).And(
                    imagem.select([1]).neq(valor)).And(
                        imagem.select([2]).neq(valor)).And(
                            imagem.select([3]).neq(valor)).And(
                                imagem.select([4]).eq(valor))
        
        
        muda_img = imagem.select([1]).mask(mmask.eq(1)).where(mmask.eq(1), valor) 
        muda_img_post = imagem.select([2]).mask(mmask.eq(1)).where(mmask.eq(1), valor) 
        muda_img_pospos = imagem.select([3]).mask(mmask.eq(1)).where(mmask.eq(1), valor)

        img_out = imagem.select([1]).blend(muda_img).addBands(
                        imagem.select([2]).blend(muda_img_post)).addBands(
                            imagem.select([3]).blend(muda_img_pospos))
        # print("imagem bandas Out ", img_out.bandNames().getInfo())
        return img_out

    def applyTemporalFilter(self):           
        imgClass = ee.ImageCollection(self.options['asset_layer_florest']).filter(
                                ee.Filter.eq('type_filter', 'temporal')).filter(
                                    ee.Filter.eq('janela', self.options['janela'] - 1)).first()    
        print("loading ", imgClass.get('system:index').getInfo()) #.bandNames()
        imgClass = imgClass.gt(0).multiply(3)

        # print(imgClass.bandNames().getInfo())
        # self.colectAnos = self.colectAnos[1:]
        # sys.exit()
        # print(" --------- lista de bandas -------------\n", self.colectAnos)
        if self.options['janela'] == 3:
            id_class = 3   # classe de floresta 
            imgOutput = ee.Image().byte()
            rasterbefore = ee.Image().byte()
            print("processing class {} == janela {} ".format(id_class, self.options['janela'] )) 

            for cc, lstyear in enumerate(self.colectAnos):
                print("  => ", lstyear)
                if cc == 0:                        
                    # print(f" #{cc} show bands ", imgClass.select(lstyear).bandNames().getInfo())                     
                    imgtmp = self.mask_3_years(id_class, imgClass.select(lstyear))
                elif cc == len(self.colectAnos) - 1:
                    print(" show bands of end year ", lstyear[1])
                    imgtmp = imgClass.select(lstyear[1])
                else:                        
                    rasterbefore = imgtmp 
                    imgComposta = rasterbefore.addBands(imgClass.select(lstyear[1:]))
                    # print(f"#{cc} show bands ", imgComposta.bandNames().getInfo())
                    imgtmp = self.mask_3_years(id_class, imgComposta)
                
                imgOutput = imgOutput.addBands(imgtmp)
                # time.sleep(3)
            # sys.exit()
            imgOutput = imgClass.select(self.lstBandFinal[:-1]).addBands(imgOutput.select(['classification_2023']))
            imgClass = imgOutput

        elif self.options['janela'] == 4:
            imageTranscicao = None
            self.colectAnos = [self.mapeiaAnos(
                                    ano, self.options['janela'], self.years) for ano in self.years]   
            id_class = 3   # classe de floresta 
            imgOutput = ee.Image().byte()
            print("processing class {} == janela {} ".format(id_class, self.options['janela']))            
            for cc, lstyear in enumerate(self.colectAnos):
                # print("  => ", lstyear)
                if cc == 0:
                    print(f" #{cc} => ", imgClass.select(lstyear).bandNames().getInfo()) 
                    # imgtmp = self.mask_4_years(id_class, imgClass.select(lstyear))
                    imgstmp = imgClass.select(lstyear[1])
                    imageTranscicao = imgstmp
                    # print("transcição ", imageTranscicao.bandNames().getInfo())

                    imgOutput = imgOutput.addBands(imgstmp)
                    
                        
                elif cc == 1: 
                    imgComposta =  imgClass.select(lstyear)                    
                    # print(f" #{cc} =>  {imgComposta.bandNames().getInfo()}  cc")
                    imgstmp = self.mask_4_years(id_class, imgComposta)
                    imageTranscicao = imgstmp.select([1])
                    # print("par de imagens ", imgstmp.bandNames().getInfo())
                    # print("transcição ", imageTranscicao.bandNames().getInfo())
                    imgOutput = imgOutput.addBands(imgstmp.select([0]))

                elif cc < len(self.colectAnos) - 2:                        
                    # if cc == 3:
                    imgComposta = imgOutput.select(lstyear[0]).addBands(imageTranscicao).addBands(imgClass.select(lstyear[2:]))
                    # else:
                    #     imgComposta = imgClass.select(lstyear[0]).addBands(imageTranscicao).addBands(imgOutput.select(lstyear[2:]))
                    # print("   ", lstyear)                   
                    # print(f" #{cc} =>  {imgComposta.bandNames().getInfo()}  cc")
                    imgstmp = self.mask_4_years(id_class,imgComposta )
                    imageTranscicao = imgstmp.select([1])
                    # print("transcição ", imageTranscicao.bandNames().getInfo())
                    imgOutput = imgOutput.addBands(imgstmp.select([0]))
                
                elif cc == len(self.colectAnos) - 2:
                    # print("transcição ", imageTranscicao.bandNames().getInfo())
                    imgOutput = imgOutput.addBands(imageTranscicao)                        
                
                else:                        
                    imgstmp = imgClass.select(lstyear[1])                    
                    imgOutput = imgOutput.addBands(imgstmp)
            
            # print("image banda addicionada ", imgOutput.bandNames().getInfo())
            imgOutput = imgOutput.select(self.lstbandNames)
            imgClass = imgOutput
                # print("comprovando o número de bandas \n ====>", len(imgOutput.bandNames().getInfo()))
                # if id_class ==  self.ordem_exec_middle[0]:
                # print(imgOutput.bandNames().getInfo())

            # time.sleep(5)
        
        elif self.options['janela'] == 5:
            imageT1 = None
            imageT2 = None
            self.colectAnos = [self.mapeiaAnos(
                                    ano, self.options['janela'], self.years) for ano in self.years]   

            id_class = 3   # classe de floresta 
            imgOutput = ee.Image().byte()
            print("processing class {} == janela {} ".format(id_class, self.options['janela']))            
            for cc, lstyear in enumerate(self.colectAnos):
                # print("  => ", lstyear)
                if cc == 0:
                    # print(f" #{cc} => ", imgClass.select(lstyear).bandNames().getInfo()) 
                    imgstmp = imgClass.select(lstyear[1])
                    imageT1 = imgstmp.select(lstyear[1])
                    # print("    ", imgstmp.bandNames().getInfo())
                    imgOutput = imgOutput.addBands(imgstmp)

                elif cc < len(self.colectAnos) - 3:  
                    if cc == 1:   
                        # print(" Com cc = 1")
                        imgComposta =  imgClass.select(lstyear)                            
                    else:
                        imgComposta = imgOutput.select(lstyear[0]).addBands(imageT1).addBands(imageT2).addBands(imgClass.select(lstyear[3:]))                
                    
                    # print(f" #{cc} =>  {imgComposta.bandNames().getInfo()}  cc")
                    imgstmpM = self.mask_5_years(id_class, imgComposta)
                    imageT1 = imgstmpM.select([1])
                    imageT2 = imgstmpM.select([2])
                    imgOutput = imgOutput.addBands(imgstmpM.select([0]))
                    # if cc < 3:
                    #     print(f" {imgstmpM.select([0]).bandNames().getInfo()} | {imageT1.bandNames().getInfo()} | {imageT2.bandNames().getInfo()} ")
                elif cc == len(self.colectAnos) - 3:
                    # print("transcição ", imageTranscicao.bandNames().getInfo())
                    imgOutput = imgOutput.addBands(imageT1) 
                elif cc == len(self.colectAnos) - 2:
                    # print("transcição ", imageTranscicao.bandNames().getInfo())
                    imgOutput = imgOutput.addBands(imageT2)       
                else:                    
                    imgstmp = imgClass.select(lstyear[1])                    
                    imgOutput = imgOutput.addBands(imgstmp)
                
            # print("image banda addicionada ", imgOutput.bandNames().getInfo())
            imgOutput = imgOutput.select(self.lstbandNames)
            imgClass = imgOutput

        
                # syy.exit()
        print(imgClass.bandNames().getInfo())
        imgClass = imgClass.selfMask().set(
                            'version',  self.versoutput, 
                            'biome', 'CAATINGA',
                            'type_filter', 'temporal',
                            'collection', '9.0',                            
                            'janela', self.options['janela'],
                            'sensor', 'Landsat',
                            'system:footprint' , self.geom_bacia
                        )
        
        name_toexport = 'layer_florest_correcao_temporal_' + str(self.options['janela'])
        self.processoExportar(imgClass, name_toexport, self.geom_bacia)    
        # sys.exit()

    #exporta a imagem classificada para o asset
    def processoExportar(self, mapaRF,  nomeDesc, geom_bacia):
        
        idasset =  self.options['asset_layer_florest'] + "/" + nomeDesc
        optExp = {
            'image': mapaRF, 
            'description': nomeDesc, 
            'assetId':idasset, 
            'region': geom_bacia.getInfo()['coordinates'],
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
        '4': 'caatinga02',
        '6': 'caatinga03',
        '8': 'caatinga04',
        '10': 'caatinga05',        
        '12': 'solkan1201',    
        '14': 'solkanGeodatin',
        '16': 'diegoUEFS' ,
        '18': 'superconta'      
    }
}

#============================================================
#========================METODOS=============================
#============================================================
def gerenciador(cont):
    #0, 18, 36, 54]
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
        cont = 0
    
    cont += 1    
    return cont




aplicando_TemporalFilter = processo_filterTemporal()
cont = gerenciador(4)       
aplicando_TemporalFilter.applyTemporalFilter()
