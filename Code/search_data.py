from pathlib import Path
import pandas as pd
#from conexion import load_data

#Obtenemos la lista de archivos en datasets
def get_path(directorio):
    rutas = []
    for item in directorio.iterdir():
        if item.is_file():
            rutas_archivos = f'{directorio}/{item.name}'
            database = rutas_archivos.split('/')[-1].split('.')[0]
            rutas.append({'ruta':rutas_archivos, 'database':database})
            
    return rutas

#Definimos nombre de la base de datos
def define_database_info(paths):
    for item in paths:
        database_name = item.get('database')
        path_name = item.get('ruta')
    return database_name,path_name


#Obtenemos los nombres de las hojas de cálculo en el archivo excel
def sheets_names(path_name):
    xls = pd.ExcelFile(path_name,engine = 'openpyxl')
    return xls.sheet_names

#Crear los df de xlsc
def create_df(path_name,sheet_name):
    try:
        data = {}
        for sheet in sheet_name:
            df = pd.read_excel(path_name,sheet_name=sheet,engine='openpyxl')
            data[sheet] = df
        return data

    except Exception as e:
        print(f'Error loading data: {str(e)}')


# directorio = Path('/Users/kcire/Google Drive/Mi unidad/Programacion/Python/Projects/empresa/Data')
# database_name,path_name = define_database_info(get_path(directorio))
# sheets = sheets_names(path_name)
# data = create_df(path_name,sheets)


# for sheet in sheets:
#     dataframe = data[sheet]
#     print(dataframe.head())







    
