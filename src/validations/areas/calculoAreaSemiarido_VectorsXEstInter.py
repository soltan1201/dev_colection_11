#!/usr/bin/env python2
# -*- coding: utf-8 -*-

'''
#  SCRIPT DE CALCULO DE AREA POR AREAS PRIORITARIAS DA CAATINGA
#  Produzido por Geodatin - Dados e Geoinformacao
#  DISTRIBUIDO COM GPLv2

#  Relação de camadas para destaques:
#  Assentamento_Brasil - Asentamentos 
#  nucleos_desertificacao - Nucleos de desertificação,
#  UnidadesConservacao_S - Unidades de conservação  -> 'TipoUso' -> ["Proteção Integral", "Proteção integral",  "Uso Sustentável"]
#  unidade_gerenc_RH_SNIRH_2020- Unidade de gerenciamento de recursos Hidricos 
#  tis_poligonais_portarias -  Terras indígenas
#  prioridade-conservacao - Prioridade de conservação (usar apenas Extremamente alta)
#  florestaspublicas - Unidades de conservação
#  areas_Quilombolas - áreas quilombolas
#  macro_RH - Bacias hidrográficas 
#  reserva da biosfera - 'zona' ->  ["nucleo","transicao","amortecimento"]
#  Novo limite do semiarido 2024


'''
import ee
import os 
# import copy
import sys
from pathlib import Path
import collections
collections.Callable = collections.abc.Callable

pathparent = str(Path(os.getcwd()).parents[1])
sys.path.append(pathparent)
print("parents ", pathparent)
from configure_account_projects_ee import get_current_account, get_project_from_account
from gee_tools import *
projAccount = get_current_account()
print(f"projetos selecionado >>> {projAccount} <<<")

try:
    ee.Initialize(project= projAccount) #
    print('The Earth Engine package initialized successfully!')
except ee.EEException as e:
    print('The Earth Engine package failed to initialize!')
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise

nomeVetor = 'APA_R_Capivara'
sufixo = nomeVetor + '_cob'

paraCobertura = True
if paraCobertura:
    sufixo = '_cob'
else:
    sufixo = '_tans'

param = {
    # 'inputAssetCol': 'projects/mapbiomas-workspace/public/collection8/mapbiomas_collection80_integration_v1',  
    # 'inputAssetCol': 'projects/mapbiomas-workspace/COLECAO9/integracao',
    'inputAssetCol': 'projects/mapbiomas-brazil/assets/LAND-COVER/COLLECTION-10/INTEGRATION/classification',
    'inputTransicao': 'projects/mapbiomas-workspace/COLECAO8/transicao',
    'assets' : {
        "Assentamento_Brasil" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/Assentamento_Brasil",
        "BR_ESTADOS_2022" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/BR_ESTADOS_2022",
        # https://code.earthengine.google.com/78daf28d589ad5ccd3160b2dde12241d   estados e biomas
        "br_estados_raster": 'projects/mapbiomas-workspace/AUXILIAR/estados-2016-raster',
        "br_estados_shp": 'projects/mapbiomas-workspace/AUXILIAR/estados-2017',
        "BR_Municipios_2022" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/BR_Municipios_2022",
        "BR_Pais_2022" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/BR_Pais_2022",
        "Im_bioma_250" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/Im_bioma_250",
        'vetor_biomas_250': 'projects/mapbiomas-workspace/AUXILIAR/biomas_IBGE_250mil',
        'biomas_250_rasters': 'projects/mapbiomas-workspace/AUXILIAR/RASTER/Bioma250mil',
        "Sigef_Brasil" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/Sigef_Brasil",
        "Sistema_Costeiro_Marinho" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/Sistema_Costeiro_Marinho",
        "aapd" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/aapd",
        "areas_Quilombolas" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/areas_Quilombolas",
        "buffer_pts_energias" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/buffer_pts_energias",
        "energias-dissolve-aneel" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/energias-dissolve-aneel",
        "florestaspublicas" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/florestaspublicas",
        "imovel_certificado_SNCI_br" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/imovel_certificado_SNCI_br",
        "macro_RH" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/macro_RH",
        "meso_RH" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/meso_RH",
        "micro_RH" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/micro_RH",
        "pnrh_asd" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/pnrh_asd",
        "prioridade-conservacao" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/prioridade-conservacao-caatinga-ibama",
        "prioridade-conservacao-V1" : "users/solkancengine17/shps_public/prioridade-conservacao-semiarido_V1",
        "prioridade-conservacao-V2" : "users/solkancengine17/shps_public/prioridade-conservacao-semiarido_V2",
        "tis_poligonais_portarias" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/tis_poligonais_portarias",
        "transposicao-cbhsf" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/transposicao-cbhsf",
        "nucleos_desertificacao" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/pnrh_nucleos_desertificacao",
        # "UnidadesConservacao_S" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/UnidadesConservacao_S",
        "UnidadesConservacao_S" : "projects/mapbiomas-workspace/AUXILIAR/areas-protegidas",
        "unidade_gerenc_RH_SNIRH_2020" : "projects/earthengine-legacy/assets/users/solkancengine17/shps_public/unidade_gerenc_RH_SNIRH_2020",
        "reserva_biosfera" : "projects/mapbiomas-workspace/AUXILIAR/RESERVA_BIOSFERA/caatinga-central-2019",
        "semiarido2024": 'projects/mapbiomas-workspace/AUXILIAR/SemiArido_2024',
        'semiarido' : 'users/mapbiomascaatinga04/semiarido_rec',
        "irrigacao": 'projects/ee-mapbiomascaatinga04/assets/polos_irrigaaco_atlas',
        "energiasE": 'projects/ee-mapbiomascaatinga04/assets/energias_renovaveis',
        "bacia_sao_francisco" : 'users/solkancengine17/shps_public/bacia_sao_francisco',
        "matopiba": 'projects/mapbiomas-fogo/assets/territories/matopiba'
    },   
    # 'assetVetor': 'users/data_sets_solkan/SHPs/APA_Guarajuba_limite',
    'assetVetor': 'users/data_sets_solkan/SHPs/APA_R_Capivara_limite',
    'collection': '10.0', # 
    'outputVersion': '0-7',
    'biome': 'CAATINGA', 
    'source': 'geodatin',
    'scale': 30,
    'date_end': 2024,
    'driverFolder': 'AREA-SEMIARIDO-CORR', 
    'lsClasses': [3,4,12,15,18,21,22,33,29],
    'numeroTask': 6,
    'numeroLimit': 40,
    'conta' : {
        '0': 'caatinga01',         
    },
}

lst_nameAsset = [
    # "semiarido2024",
    # 'Assentamento_Brasil', 
    # "nucleos_desertificacao",
    # "UnidadesConservacao_S", #"unidade_gerenc_RH_SNIRH_2020", 
    # 'areas_Quilombolas', 
    # "macro_RH", "meso_RH", #'micro_RH', 
    # 'prioridade-conservacao-V1', 
    # 'prioridade-conservacao-V2', 
    # 'tis_poligonais_portarias', 
    # "reserva_biosfera",
    # 'semiarido',
    # "energiasE",    
    # "bacia_sao_francisco",
    "matopiba",
    "micro_RH",
    "transposicao-cbhsf"
];                          

dict_name = {
    "prioridade-conservacao": 'prior-cons',
    "reserva_biosfera": 'res-biosf',
    "Assentamento_Brasil": 'Assent-Br', 
    "nucleos_desertificacao": "nucleos-desert",
    "UnidadesConservacao_S": "UnidCons-S", 
    "unidade_gerenc_RH_SNIRH_2020": "unid-ger-RH",
    "areas_Quilombolas": "areaQuil", 
    "macro_RH": "macro-RH", 
    "meso_RH": "meso-RH", 
    "micro_RH": "micro_RH",
    "matopiba": "matopiba",
    "tis_poligonais_portarias": "tis-port",
    "PARNAÍBA": "PARN", 
    "ATLÂNTICO NORDESTE ORIENTAL": "AtlTO", 
    "SÃO FRANCISCO": "SF", 
    "ATLÂNTICO LESTE": "AtlL",
    "Proteção Integral": "prot-Int", 
    "Proteção integral": "prot-Int2",  
    "Uso Sustentável": "Uso-sustt",
    'semiarido': 'semiarido',
    "energiasE": 'energias-renovaveis',
    'prioridade-conservacao-V1': 'prioridade-conservacao-V1',
    'prioridade-conservacao-V2': 'prioridade-conservacao-V2',
    "bacia_sao_francisco": "bacia-sao-francisco",
    "semiarido2024": "Semiarido-2024",
    "transposicao-cbhsf": "transposicao-cbhsf",
}
camadasIrrig = [
    "Jaíba", "Petrolina / Juazeiro", "Jaguaribe",
    "Mucugê-Ibicoara", "Oeste Baiano"
]
dict_Irrig = {
    "Jaíba": "Jaiba", 
    "Petrolina / Juazeiro": "PetroJuazei", 
    "Jaguaribe": "Jaguaribe",
    "Mucugê-Ibicoara": "MucuIbico", 
    "Oeste Baiano": "OestBaiano"
}
dict_baciaSF = {
    '196': "Submedio-Sao-Francisco",
    '197': "Medio-Sao-Francisco",
    '205':"Alto-Sao-Francisco",
    '219':"Baixo-Sao-Francisco"
}
lstSemiarido = ["08012100154","08012200053","08012900108"]
nameCamada = '';
lstTipoUso = [
    ["Proteção Integral", "Proteção integral"],  ["Uso Sustentável"]
];
lstMacro = [
    "PARNAÍBA", "ATLÂNTICO NORDESTE ORIENTAL", 
    "SÃO FRANCISCO", "ATLÂNTICO LESTE"
];
lstIdsbaciaSF = ['196','197','205','219']
dictMeso ={
    "PARNAÍBA": ["Alto Parnaíba", "Médio Parnaíba", "Baixo Parnaíba"],
    "ATLÂNTICO NORDESTE ORIENTAL": [
        "Jaguaribe", "Litoral do Ceará", "Litoral do Rio Grande do Norte e Paraíba", 
        "Piancó-Piranhas-Açu", "Litoral de Pernambuco e Alagoas"],
    "SÃO FRANCISCO": ["Médio São Francisco", "Submédio São Francisco", "Baixo São Francisco"],
    "ATLÂNTICO LESTE": ["Contas", "Itapicuru/Paraguaçu", "Jequitinhonha/Pardo", "Vaza-Barris"]
}
dictMesoSigla ={
    "PARNAÍBA": {
        "Alto Parnaíba": "AltoP", 
        "Médio Parnaíba": "MedioP", 
        "Baixo Parnaíba": "BaixoP"
    },
    "ATLÂNTICO NORDESTE ORIENTAL": {
        "Jaguaribe": "AtlaNO-Jag", 
        "Litoral do Ceará": "AtlaNO-LC", 
        "Litoral do Rio Grande do Norte e Paraíba": "AtlaNO-LRGNP", 
        "Piancó-Piranhas-Açu": "AtlaNO-PPA", 
        "Litoral de Pernambuco e Alagoas": "AtlaNO-LPA"
    },
    "SÃO FRANCISCO": {
        "Médio São Francisco": "MedioSF", 
        "Submédio São Francisco": "SubmedSF", 
        "Baixo São Francisco": "BaixoSF"
    },
    "ATLÂNTICO LESTE": {
        "Contas": "AtlaL-C", 
        "Itapicuru/Paraguaçu": "AtlaL-IP", 
        "Jequitinhonha/Pardo": "AtlaL-JP", 
        "Vaza-Barris": "AtlaL-VB"
    }
}
dictEst = {
    '21': 'MARANHÃO',
    '22': 'PIAUÍ',
    '23': 'CEARÁ',
    '24': 'RIO GRANDE DO NORTE',
    '25': 'PARAÍBA',
    '26': 'PERNAMBUCO',
    '27': 'ALAGOAS',
    '28': 'SERGIPE',
    '29': 'BAHIA',
    '31': 'MINAS GERAIS',
    '32': 'ESPÍRITO SANTO'
}


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

##############################################
###     Helper function
###    @param item 
##############################################
def convert2featCollection (item):
    item = ee.Dictionary(item)
    feature = ee.Feature(ee.Geometry.Point([0, 0])).set(
        'classe', item.get('classe'),"area", item.get('sum'))
        
    return feature

#########################################################################
####     Calculate area crossing a cover map (deforestation, mapbiomas)
####     and a region map (states, biomes, municipalites)
####      @param image 
####      @param geometry
#########################################################################
# https://code.earthengine.google.com/5a7c4eaa2e44f77e79f286e030e94695
def calculateArea (image, pixelArea, geomFC):
    # limite_shp = ee.FeatureCollection(geomFC).geometry()
    # maskGeom_raster = ee.FeatureCollection(geomFC).reduceToImage(['id_codigo'], ee.Reducer.first())
    pixelArea = pixelArea.addBands(image.rename('classe'))
    reducer = ee.Reducer.sum().group(1, 'classe')
    optRed = {
        'reducer': reducer,
        'geometry': geomFC,
        'scale': 30,
        'bestEffort': True, 
        'maxPixels': 1e13
    }    
    areas = pixelArea.reduceRegion(**optRed)
    areas = ee.List(areas.get('groups')).map(lambda item: convert2featCollection(item))
    areas = ee.FeatureCollection(areas)    
    return areas

# pixelArea, imgMapa, bioma250mil

def iterandoXanoImCruda(limite_feat_col, namesubVector, namemacroVect, isCobert, porAno, remap):
    
    classMapB = [3, 4, 5, 6, 9,12,13,15,18,19,20,21,22,23,24,25,26,29,30,31,32,33,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,62]
    classNew =  [3, 4, 3, 3, 3,12,12,15,18,18,18,21,22,22,22,22,33,29,22,33,12,33,18,33,33,18,18,18,18,18,18,18,18,18,18, 4,12,18]
    sufRemap = '_sinRM'
    shpSemiArGeom = ee.FeatureCollection(param['assets']["semiarido2024"])
                        # .geometry())
    shpStateGeom = ee.FeatureCollection(param['assets']["br_estados_shp"])    
    # print(shpStateGeom.aggregate_histogram('CD_GEOCUF').getInfo())        
    shpSemiArGeom = shpSemiArGeom.map(lambda feat: feat.set('id_codigo', 1))       
    mask_semiArido = ee.Image(shpSemiArGeom
                                .reduceToImage(['id_codigo'], ee.Reducer.first()))
    
    raster_mask_polygon = (ee.FeatureCollection(limite_feat_col)
                                .reduceToImage(['id_codigo'], ee.Reducer.first()))
    # geom_polygon = ee.FeatureCollection(limite_feat_col).geometry()
    shpSemiArGeom = shpSemiArGeom.geometry()

    ######### códigos e nomes dos estados ##################################
    # https://code.earthengine.google.com/05a89e0f9f7b0131dd9f1e200b72b7af #
    ########################################################################
    estados_raster = ee.Image(param['assets']["br_estados_raster"])
    lstEstCruz = [21,22,23,24,25,26,27,28,29,31,32]
    # lstEstCruz = [29,31]
    cont = 0
    if isCobert:
        print("Loadding image Cobertura Coleção 10.0 " )
        # imgMapp = ee.Image(param['inputAssetCol']).clip(limite)  # para a 7.1
        imgMapp = (ee.ImageCollection(param['inputAssetCol'])
                        .filter(ee.Filter.eq('version', param['outputVersion']))
                        .mosaic()
                        .updateMask(mask_semiArido)
                        .updateMask(raster_mask_polygon)
                )
        imgAreaRef =  (ee.Image.pixelArea().divide(10000)
                        .updateMask(mask_semiArido)
                        .updateMask(raster_mask_polygon)
                    )
        # print("---- SHOW ALL BANDS FROM MAPBIOKMAS MAPS -------\n ", imgMapp.bandNames().getInfo())
        # sys.exit()
        for estadoCod in lstEstCruz:
            areaGeral = ee.FeatureCollection([]) 
            print(f"processing Estado {dictEst[str(estadoCod)]} with code {estadoCod}")
            maskRasterEstado = estados_raster.eq(estadoCod)
            shpStateGeomS = shpStateGeom.filter(ee.Filter.eq('CD_GEOCUF', str(estadoCod))).geometry()
            rasterMapEstado = imgMapp.updateMask(maskRasterEstado)
            imgAreaRefEstado = imgAreaRef.updateMask(maskRasterEstado)
            geom_polygon_state = shpSemiArGeom.intersection(shpStateGeomS).intersection(limite_feat_col.geometry())
            # area_geom = geom_polygon_state.area(0.01).getInfo()
            # print("area geom ", area_geom)
            # if area_geom > 0:
            for year in range(1985, param['date_end'] + 1):
                # print(f" ======== processing year {year} for mapbiomas map =====")
                bandAct = "classification_" + str(year)

                if remap:
                    newimgMap = rasterMapEstado.select(bandAct).remap(classMapB, classNew)
                    sufRemap = '_remap'
                    print("    << remaping >>   ")
                else:
                    newimgMap = rasterMapEstado.select(bandAct)
                    # print("banda ", newimgMap.bandNames().getInfo())

                areaTemp = calculateArea (newimgMap, imgAreaRefEstado, geom_polygon_state) 
                # print(" area temporal ", areaTemp.first().getInfo())
                # sys.exit()       
                areaTemp = areaTemp.map( lambda feat: feat.set(
                                                    'year', year, 
                                                    'nomeVetor', nomeVetor,
                                                    'region', namesubVector, 
                                                    'sub_region', namemacroVect,
                                                    # colocar o nome do estado
                                                    'estado_name', dictEst[str(estadoCod)], 
                                                    'estado_codigo', estadoCod
                                                ))
                if porAno:                
                    nameCSV = "area_class_SA_" + namesubVector + "_"+ namemacroVect + "_codEst_" + str(estadoCod) +"_" + str(year) + sufRemap
                    processoExportar(areaTemp, nameCSV)
                    # cont = gerenciador(cont) 
                else:
                    # print(" ---- fazendo merger --- ")
                    areaGeral = areaGeral.merge(areaTemp)              

            if not porAno:
                nameCSV = "area_class_SA_" + namesubVector +  "_" + namemacroVect + "_codEst_" + str(estadoCod) + sufRemap
                processoExportar(areaGeral, nameCSV)

    return ee.FeatureCollection([]), False

        
#exporta a imagem classificada para o asset
def processoExportar(areaFeat, nameT):      
    optExp = {
          'collection': areaFeat, 
          'description': nameT, 
          'folder': param["driverFolder"]        
        }    
    task = ee.batch.Export.table.toDrive(**optExp)
    task.start() 
    print("salvando ... " + nameT + "..!")    


def unique(list1): 
    # insert the list to the set
    list_set = set(list1)
    # convert the set to the list
    unique_list = (list(list_set))
    for x in unique_list:
        print (" >>> ", x)

    return unique_list
 
def uniques(list1): 
    # insert the list to the set
    list_set = []
    
    for x in list1:
        if x not in list_set:
            list_set.append(x)

    return list_set

print('         limite caatinga carregado ')


select_Caatinga = True
sobreNomeGeom = "_Semiarido"
limitGeometria = ee.FeatureCollection(param['assets']["semiarido2024"])
print("=============== limite a Macro Selecionado ========== ", limitGeometria.size().getInfo())
# limitGeometria = limitGeometria.geometry()
limitGeometria = limitGeometria.map(lambda feat: feat.set('id_codigo', 1))

#testes do dado
# https://code.earthengine.google.com/8e5ba331665f0a395a226c410a04704d
# https://code.earthengine.google.com/306a03ce0c9cb39c4db33265ac0d3ead
# get raster with area km2
# lstBands = ['classification_' + str(yy) for yy in range(1985, param['date_end'] + 1)]
# shpLimit = ee.FeatureCollection(param['assetVetor'])

# imgMapa = ee.ImageCollection(param['inputAsset']).select(lstBands)

sufRemap = '_sinRM'
cont = 0
byYears = False
lstProp = []
for nameAsset in lst_nameAsset[:]:
    print(f"------ PROCESSING {nameAsset} --------")
    shp_tmp = ee.FeatureCollection(param['assets'][nameAsset])    
    print(" => " + nameAsset, " with = ", shp_tmp.size().getInfo())
    shp_tmp = shp_tmp.map(lambda feat: feat.set('id_codigo', 1))
    
    if nameAsset == 'prioridade-conservacao-V1' or nameAsset == 'prioridade-conservacao-V2':
        shp_tmp = shp_tmp.filter(ee.Filter.eq('import_bio', 'Extremamente Alta'))
        print("filtrado por prioridade ", shp_tmp.size().getInfo())
        nameCSV = "area_class_" + dict_name[nameAsset] + "_" + 'ext-alta' + sobreNomeGeom + sufixo
        # imgAreaRef, limite, namesubVector, namemacroVect, isCobert, porAno, remap
        csv_table, exportar = iterandoXanoImCruda(shp_tmp.filterBounds(limitGeometria.geometry()), 
                                dict_name[nameAsset], 'ext-alta', paraCobertura, byYears, False)
        
        if exportar:
            processoExportar(csv_table, nameCSV)

    elif nameAsset == 'reserva_biosfera':
        lstPropResBio = shp_tmp.reduceColumns(ee.Reducer.toList(), ['zona']).get('list').getInfo();
        lstPropResBio = unique(lstPropResBio)
        print("reserva da biosfera ", lstPropResBio)
        for typeRes in lstPropResBio:
            shp_tmp_resBio = shp_tmp.filter(ee.Filter.eq('zona', typeRes))
            nameCSV = "area_class_" + dict_name[nameAsset] + "_" + typeRes + sobreNomeGeom + sufixo
            csv_table, exportar = iterandoXanoImCruda(shp_tmp_resBio.filterBounds(limitGeometria.geometry()), 
                                    dict_name[nameAsset], typeRes, paraCobertura, byYears, False)
            
            if exportar:
                processoExportar(csv_table, nameCSV)
    
    elif nameAsset == 'UnidadesConservacao_S':
        lstTipoUso = shp_tmp.reduceColumns(ee.Reducer.toList(), ['TipoUso']).get('list').getInfo();
        print("Unidades de conservação ", unique(lstTipoUso))
        lstTipoUso = [["Proteção Integral", "Proteção integral"],  ["Uso Sustentável"]]
        for typeUso in lstTipoUso:
            shp_tmp_uso = shp_tmp.filter(ee.Filter.inList('TipoUso', typeUso))      
            nameCSV = "area_class_" + dict_name[nameAsset] + "_" + dict_name[typeUso[0]] + sobreNomeGeom + sufixo      
            csv_table, exportar = iterandoXanoImCruda(shp_tmp_uso.filterBounds(limitGeometria.geometry()), 
                            dict_name[nameAsset], dict_name[typeUso[0]], paraCobertura, byYears,  False)
            
            if exportar:
                processoExportar(csv_table, nameCSV)


    elif nameAsset == 'macro_RH':
        lstProp = shp_tmp.reduceColumns(ee.Reducer.toList(), ['nm_macroRH']).get('list').getInfo();
        print("Macro Região Hidrografica ", unique(lstProp))
        for nmmacro in lstMacro:
            shp_tmp_macro = shp_tmp.filter(ee.Filter.eq('nm_macroRH', nmmacro))
            # nameCamada = nameAsset + "_" + nmmacro
            # Map.addLayer(shp_tmp_macro, {}, nameCamada, false);
            nameCSV = "area_class_" + dict_name[nameAsset] + "_" + dict_name[nmmacro] + sobreNomeGeom + sufixo
            csv_table, exportar = iterandoXanoImCruda(shp_tmp_macro.filterBounds(limitGeometria.geometry()), 
                            dict_name[nameAsset], 'Caatinga', paraCobertura, byYears, False)
            
            if exportar:
                processoExportar(csv_table, nameCSV)

    elif nameAsset == 'meso_RH':
        lstProp = shp_tmp.reduceColumns(ee.Reducer.toList(2),  
                            ['nm_macroRH', 'nm_mesoRH']).get('list').getInfo();
        print("Meso Região Hidrografica ", uniques(lstProp));
        for nmmacro in lstMacro:
            lstMesoRH = dictMeso[nmmacro]
            print("Macro => ", nmmacro, " with ", lstMesoRH)
            for nmmeso in lstMesoRH:
                print("loading ", nmmeso)
                shp_tmp_meso = shp_tmp.filter(ee.Filter.eq('nm_mesoRH', nmmeso))
                # nameCamada = nameAsset + "_" + dictMesoSigla[nmmacro][nmmeso]
                nameCSV = "area_class_" + dict_name[nameAsset] + dictMesoSigla[nmmacro][nmmeso] + sobreNomeGeom + sufixo
                csv_table, exportar = iterandoXanoImCruda(shp_tmp_meso.filterBounds(limitGeometria.geometry()), 
                            dict_name[nameAsset], dictMesoSigla[nmmacro][nmmeso], paraCobertura, byYears, False)
                
                if exportar:
                    processoExportar(csv_table, nameCSV)

    elif nameAsset == 'semiarido':        
        nameCSV = "area_class_" + nameAsset + sufixo
        csv_table, exportar = iterandoXanoImCruda(shp_tmp, nameAsset, 'semiarido', paraCobertura, False, False)
            
        if exportar:
            processoExportar(csv_table, nameCSV)

    elif nameAsset == "irrigacao":
        for irrig in camadasIrrig:
            shp_tmp_irr = shp_tmp.filter(ee.Filter.eq('Polo_irrig', irrig))
            nameCSV = "area_class_" + nameAsset + "_" + dict_Irrig[irrig] + sobreNomeGeom + sufixo
            csv_table, exportar = iterandoXanoImCruda(shp_tmp_irr.filterBounds(limitGeometria.geometry()), 
                            nameAsset, dict_Irrig[irrig], paraCobertura, byYears, nameCSV, False)
            
            if exportar:
                processoExportar(csv_table, nameCSV)                    

    elif nameAsset == "bacia_sao_francisco":

        for idBaciaSF in lstIdsbaciaSF:
            nameCSV = "area_class_" + nameAsset + "_" + dict_baciaSF[idBaciaSF] +sobreNomeGeom + sufixo
            shp_tmp_bSF = shp_tmp.filter(ee.Filter.eq('id', int(idBaciaSF))) #'id','nm_mesoRH'
            print("size shp São Francisco <==> " + dict_baciaSF[idBaciaSF] , shp_tmp_bSF.size().getInfo())
            csv_table, exportar = iterandoXanoImCruda( shp_tmp_bSF.filterBounds(limitGeometria.geometry()), 
                            nameAsset, dict_baciaSF[idBaciaSF], paraCobertura, byYears, False) # remap = False
            
            if exportar:
                processoExportar(csv_table, nameCSV)
            # sys.exit()
    else:
        nameCSV = "area_class_" + dict_name[nameAsset] + sobreNomeGeom + sufixo + sufRemap 
        csv_table, exportar = iterandoXanoImCruda(shp_tmp.filterBounds(limitGeometria.geometry()), 
                            dict_name[nameAsset], 'Caatinga', paraCobertura, False, False) # remap = False
        
        if exportar:
            processoExportar(csv_table, nameCSV)

    # cont = gerenciador(cont) 
    # sys.exit()    

