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


class processo_filterTemporal(object):

    options = {
            'output_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/TemporalCC',
            'input_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Frequency',
            # 'input_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col8/CAATINGA/POS-CLASS/merge/',
            # 'input_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Spatial',
            # 'input_asset': 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/POS-CLASS/TemporalV3',
            'asset_bacias_buffer' : 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/bacias_hidrografica_caatinga_49_regions',            
            'last_year' : 2024,
            'first_year': 1985,
            'janela' : 3,
            'step': 1
        }

    def __init__(self):
        # self.id_bacias = nameBacia
        self.versoutput = 5
        self.versionInput = 5
        self.geom_bacia = ee.FeatureCollection(self.options['asset_bacias_buffer'])              
        self.years = [yy for yy in range(self.options['first_year'], self.options['last_year'] + 1)]  # ,  - 1, -1
        # self.years = [kk for kk in years.sort(reverse= False)]
        self.lstbandNames = ['classification_' + str(yy) for yy in range(self.options['last_year'], self.options['first_year'] - 1, -1)]
        
        print("lista de anos ", self.years)
        self.lstBandFinal =  ['classification_' + str(yy) for yy in self.years]
        print("lista de anos ", self.lstBandFinal)
        # self.ordem_exec_first = [4,21,4,3,12] #
        self.ordem_exec_last = [3,4]  # 29
        self.ordem_exec_middle = [3, 4, 21] # ,33, 22,      21,4,3,12

        self.colectAnos = [self.mapeiaAnos(ano, self.options['janela'], self.years) for ano in self.years]



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
        # muda_img = imagem.select([1]).mask(mmask.eq(1)).where(mmask.eq(1), valor)
        muda_img = mmask.eq(1).multiply(valor)
        return imagem.select([1]).blend(muda_img)

    def mask_4_years (self, valor, imagem):
        imagem = ee.Image(imagem)  
        # print("    === > ", imagem.bandNames().getInfo())      
        mmask = imagem.select([0]).eq(valor).And(
                    imagem.select([1]).neq(valor)).And(
                        imagem.select([2]).neq(valor)).And(
                            imagem.select([3]).eq(valor))
        
        muda_img = mmask.eq(1).multiply(valor) 
        # muda_img_post = imagem.select([2]).mask(mmask.eq(1)).where(mmask.eq(1), valor) 
        # print("  <> ", imagem.select([2]).bandNames().getInfo())
        img_out = imagem.select([1]).blend(muda_img).addBands(imagem.select([2]).blend(muda_img))
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

    def applyTemporalFilter(self, id_bacias): 

        if "Temporal" in self.options['input_asset']:
            # name_imgClass = 'filterTP_BACIA_'+ str(id_bacias) + f"_GTB_J{self.options['janela'] - 1}_V" + str(self.versoutput) 
            imgClass = (ee.ImageCollection(self.options['input_asset'])
                                .filter(ee.Filter.eq('id_bacia', id_bacias))
                                .filter(ee.Filter.eq('version',  self.versionInput))
                                .filter( ee.Filter.eq('janela', self.options['janela'] - 1))
                                .first() 
                        )
        else:     
            print( " load images spatial ")       
            imgClass = (ee.ImageCollection(self.options['input_asset'])
                                .filter(ee.Filter.eq('id_bacias', id_bacias))
                                .filter(ee.Filter.eq('version',  self.versionInput))
                                .first()    
                        )

            print(dict(imgClass.getInfo()).keys()) # ['properties']
        # name_imgClass = 'filterFQ_BACIA_'+ str(id_bacias) + "_V" + str(self.versionTP)
        
        # print(imgClass.size().getInfo())
        print("loading ", imgClass.get('system:index').getInfo()) #.bandNames()
        # self.colectAnos = self.colectAnos[1:]
        # sys.exit()
        # print(" --------- lista de bandas -------------\n", self.colectAnos)
        if self.options['janela'] == 3:
            print(f" ==== processing janela { self.options['janela']} ====== ")             
                
            rasterbefore = ee.Image().byte()                
            imgOutput = ee.Image().byte()
            for cc, lstyear in enumerate(self.colectAnos):
                print(" list of Years  => ", lstyear)
                mapa_raster = copy.deepcopy(imgClass.select(lstyear))
                for id_class in self.ordem_exec_middle[:]:
                    print(" >>> ", id_class)
                    if cc == 0:                        
                        print(f" #{cc} show bands {id_class} ")                     
                        map_raster_YY = self.mask_3_years(id_class, mapa_raster)
                        mapa_raster = mapa_raster.select([0]).addBands(map_raster_YY).addBands(mapa_raster.select([2]))
                    
                    elif cc < len(self.colectAnos) - 1:                    
                        print(f" #{cc} intermedios show bands {id_class} ") 
                        imgComposta = rasterbefore.addBands(mapa_raster.select(lstyear[1:]))
                        # print(f"#{cc} show bands ", imgComposta.bandNames().getInfo())
                        map_raster_YY = self.mask_3_years(id_class, imgComposta)
                        mapa_raster = mapa_raster.select([0]).addBands(map_raster_YY).addBands(mapa_raster.select([2]))
                    else:
                        print(" show bands ", lstyear[1])
                        map_raster_YY = imgClass.select(lstyear[1])
                    
                rasterbefore = map_raster_YY 
                imgOutput = imgOutput.addBands(map_raster_YY)
                    # time.sleep(3)
            # sys.exit()
            imgOutput = imgClass.select(self.lstBandFinal) # .addBands(imgOutput.select(['classification_2023']))
            imgClass = imgOutput

        elif self.options['janela'] == 4:
            imageTranscicao = None
            self.colectAnos = [self.mapeiaAnos(ano, self.options['janela'], self.years) for ano in self.years]   
            print(f" ==== processing janela { self.options['janela']} ====== ") 
            
            imgOutput = ee.Image().byte()
            for cc, lstyear in enumerate(self.colectAnos):
                print(" list of Years  => ", lstyear)
                mapa_raster = copy.deepcopy(imgClass.select(lstyear))                
                print("processing class {} == janela {} ".format(id_class, self.options['janela']))            
                for id_class in self.ordem_exec_middle[:]:
                    # print("  => ", lstyear)
                    if cc == 0:
                        print(f" #{cc} show bands {id_class} ")                     
                        map_raster_YY = self.mask_4_years(id_class, mapa_raster)
                        mapa_raster = (mapa_raster.select([0])
                                            .addBands(map_raster_YY.select([0]))
                                            .addBands(map_raster_YY.select([1]))
                                            .addBands(mapa_raster.select([2]))
                                    )                           
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

            for id_class in self.ordem_exec_middle:
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
                # print("comprovando o número de bandas \n ====>", len(imgOutput.bandNames().getInfo()))
                # if id_class ==  self.ordem_exec_middle[0]:
                #     print(imgOutput.bandNames().getInfo())
        
                # syy.exit()
        # lst_band_conn = [bnd + '_conn' for bnd in self.lstbandNames]
        # # / add connected pixels bands
        # imageFilledConnected = imgClass.addBands(
        #                             imgClass.connectedPixelCount(10, True).rename(lst_band_conn))
        geomBacia = self.geom_bacia.filter(ee.Filter.eq('nunivotto4', id_bacias))#.first().geometry()
        geomBacia = geomBacia.map(lambda f: f.set('id_codigo', 1))
        bacia_raster = geomBacia.reduceToImage(['id_codigo'], ee.Reducer.first()).gt(0)            
        geomBacia = geomBacia.geometry()
        print(imgClass.bandNames().getInfo())
        imageFilledConnected = imgClass.set(
                            'version',  self.versoutput, 
                            'biome', 'CAATINGA',
                            'type_filter', 'temporal',
                            'collection', '10.0',
                            'id_bacia', id_bacias,
                            'janela', self.options['janela'],
                            'sensor', 'Landsat', 'model', "GTB",
                            'system:footprint' , geomBacia
                        )
        imageFilledConnected = ee.Image.cat(imageFilledConnected).updateMask(bacia_raster)
        
        name_toexport = f"filterTP_BACIA_{id_bacias}_GTB_J{self.options['janela']}_V{self.versoutput}"
        self.processoExportar(imageFilledConnected, name_toexport, geomBacia)    
        sys.exit()

    #exporta a imagem classificada para o asset
    def processoExportar(self, mapaRF,  nomeDesc, geom_bacia):        
        idasset =  os.path.join(self.options['output_asset'], nomeDesc)
        optExp = {
            'image': mapaRF, 
            'description': nomeDesc, 
            'assetId':idasset, 
            'region': geom_bacia, # .getInfo()['coordinates']
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
    'numeroLimit': 16,
    'changeConta': False,
    'conta' : {
        '0': 'caatinga01',
        '4': 'caatinga02',
        '6': 'caatinga03',
        '8': 'caatinga04',
        '10': 'caatinga05',        
        '12': 'solkan1201',    
        '14': 'solkanGeodatin',
        # '16': 'diegoUEFS' ,
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
    '7691', '7754', '7581', '7625', '751', '7614', 
    '752', '7616', '745', '7424', '773', '7612', '7613', 
    '7618', '7561', '755', '7617', '7564','761111','761112', 
    '7741', '7422', '76116', '7671', '7615', '7411', 
    '7764', '757', '771', '766', '7746', '753', '764', 
    '7541', '7721', '772', '7619', '7443', '765', '7544', '7438', 
    '763', '7591', '7592', '7622', '746','7712','7584', '7761'
]

# listaNameBacias = [
#        '752', '753','755', '758'
#  ]

#'7421', '746', '764', '765', '772', '7741', '777',
# input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/POS-CLASS/SpatialV2'
# imgCol = ee.ImageCollection(input_asset).filter(ee.Filter.neq('step', 2))
# print("numero de imagens ", imgCol.size().getInfo())
# listId = imgCol.reduceColumns(ee.Reducer.toList(), ['id_bacia']).get('list').getInfo()
# print(listId)
# lstHH =  [kk for kk in listaNameBacias if kk not in listId]
# print(lstHH)
# sys.exit()

aplicando_TemporalFilter = processo_filterTemporal()
# sys.exit()

# for cc, lst in enumerate(aplicando_TemporalFilter.colectAnos):
#     print(1985 + cc, lst)
# 
input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col10/CAATINGA/POS-CLASS/Spatial'
# input_asset = 'projects/mapbiomas-workspace/AMOSTRAS/col9/CAATINGA/POS-CLASS/TemporalV3/'
version = 4
janela = 3
cont = 16
if param['changeConta']:
    cont = gerenciador(cont)
modelo = "GTB"
listBacFalta = []
knowMapSaved = False
for cc, idbacia in enumerate(listaNameBacias[:1]):   
    if knowMapSaved:
        try:
            if 'Spatial' in input_asset:
                nameMap = f"filterSP_BACIA_{idbacia}_GTB_V{version}_step1"
            else:
                nameMap = 'filterTP_BACIA_'+ idbacia + f"_GTB_J{janela}_V" + str(version)
            dir_assetId = os.path.join(input_asset, nameMap)
            # print("load >>> " + dir_assetId)
            imgtmp = ee.Image(dir_assetId)
            print(f"👀>  {cc} loading ", nameMap, " ", len(imgtmp.bandNames().getInfo()), "bandas ")
            if cc == 0:
                print('show bands \n ', imgtmp.bandNames().getInfo())
        except:
            listBacFalta.append(idbacia)
    else: 
        if idbacia not in listBacFalta:
            
            print("----- PROCESSING BACIA {} -------".format(idbacia))        
            aplicando_TemporalFilter.applyTemporalFilter(idbacia)

if knowMapSaved:
    print("lista de bacias que faltam \n ",listBacFalta)
    print("total ", len(listBacFalta))