import ee
import os
import sys
import json
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

pathJson = str(Path(os.getcwd())) + "/"
b_file = open(pathJson + "dict_basin_49_lista_grades.json", 'r')
dictbasinGrid = json.load(b_file)
b_file.close()


def GetGridsfromFolder(assetFolder, dictbasinGrid, lstBacias=[], lstYear=[], play_eliminar=False):
    # Expande bacias em grades usando o dicionário
    lst_grades = []
    if len(lstBacias) > 0:
        for bacia in lstBacias:
            if bacia in dictbasinGrid:
                lst_grades += [str(g) for g in dictbasinGrid[bacia]]
            else:
                print(f"[AVISO] bacia {bacia} não encontrada no dicionário")
        lst_grades = list(set(lst_grades))
        print(f"Total de grades para as bacias {lstBacias}: {len(lst_grades)}")

    getlistPtos = ee.data.listAssets(assetFolder)
    assets_lista = getlistPtos['assets']
    print(f"Total de assets no folder: {len(assets_lista)}")
    lst_path = []

    for idAsset in assets_lista:
        path_ = idAsset['id']
        name = path_.split("/")[-1]

        if len(lst_grades) > 0:
            # Verifica se alguma parte do nome (separada por '_') corresponde a uma grade
            parts = name.split('_')
            grade_match = next((p for p in parts if p in lst_grades), None)

            if grade_match:
                if len(lstYear) > 0:
                    nyear = int(name.split('_')[-1])
                    if nyear in lstYear:
                        print(f'  grade={grade_match}  year={nyear}  {name}')
                        lst_path.append(path_)
                else:
                    print(f'  grade={grade_match}  {name}')
                    lst_path.append(path_)
        else:
            lst_path.append(path_)

    sizeFiles = len(lst_path)
    print(f"\nAssets selecionados para eliminação: {sizeFiles}")
    for cc, npath in enumerate(lst_path):
        name = npath.split("/")[-1]
        print(f"eliminando {cc}/{sizeFiles}:  {name}")
        if play_eliminar:
            ee.data.deleteAsset(npath)

    print(f"\nBacias: {lstBacias}")
    print(f"Grades: {lst_grades}")
    print(f"Foram eliminidados {len(lst_grades)} grades")


asset = "projects/mapbiomas-workspace/AMOSTRAS/col11/CAATINGA/ROIs/ROIs_byGradesInd"

lstbacias = ["7561", "7564", "7584", "7612", "76116", "7424", "7438", "7617"]
lst_years = []
eliminar_files = False
GetGridsfromFolder(asset, dictbasinGrid, lstBacias=lstbacias, lstYear=lst_years, play_eliminar=eliminar_files)
