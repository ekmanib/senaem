import pandas as pd
import numpy as np
import glob
import os

# read in csv with codes for the Harmonized System\
# (Sistema Armonizado) in Spanish, thus the label `sa`
sa = pd.read_excel('c:/Users/Elias/Desktop/GitHub/d-hub/data/senaem/files_support/chapters_only.xlsx', dtype={0:'str',1:'str'})

# parse the title by getting the phrase after the initial #.
sa['TÍTULO'] = sa['TÍTULO'].str.split(pat = '.', expand=True)[1]

# read in csv for world countries, containing geographical coordinates
cnty_ = pd.read_csv('c:/Users/Elias/Desktop/GitHub/d-hub/data/senaem/files_support/worldcountries.csv', sep = '\t', usecols=['country', 'latitude', 'longitude'])

# read in csv for world cities, containing geographical coordinates (filter for ecuadorian cities)
cty_ = pd.read_csv('c:/Users/Elias/Desktop/GitHub/d-hub/data/senaem/files_support/worldcities.csv', usecols=['city', 'lat', 'lng', 'country'])

cty_ = cty_.loc[cty_['country'] == 'Ecuador']

cty_.drop(columns=['country'], inplace=True)

# define a dictionary for variables of interest that will be kept
varnames = [
    'TIPO_IMPORTACION', 'FEC_INGRESO', 'DISTRITO', 'SUBPARTIDA', 
    'PAIS_ORIGEN', 'FOB',  'FLETE', 'SEGURO', 'CIF', 'SALVAGUARDIA', 
    'SALVAGUARDIA_ESPECIFICA', 'FODINFA', 'ICE_ADVALOREM', 
    'ICE_ESPECIFICO', 'IVA', 'ADVALOREM', 'ADVALOREM_ESPECIFICO'
]

def process(file_paths, year):
    
    # create empty list to store all df related to specific year
    dfs = []
    # read in each of the files and store them in `dfs`
    for file_path in file_paths:
        df_temp = pd.read_csv(file_path, usecols=varnames)
        dfs.append(df_temp)

    # concatenate vertically, stack each one upon another
    df = pd.concat(dfs, ignore_index=True)

    # ensure whitespace is stripped from the beginning and the ending of strings
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # create `date` datetime column from FEC_INGRESO
    df['date'] = pd.to_datetime(df['FEC_INGRESO'])

    # Separate the variable 'PAIS_DESTINO' into iso code and country name variables
    paisD_expanded = df['PAIS_ORIGEN'].str.split(pat='-', expand=True)
    df['iso'] = paisD_expanded[0]
    df['pais'] = paisD_expanded[1]
    # Capitalize each word in the strings
    df['pais'] = df['pais'].str.title()

    # GET CODES FOR CHAPTERS IN THE GENERAL CLASSIFICATION OF PRODUCTS: i) Change the 'SUBPARTIDA' column data type\
    # to string. ii) create a new variable that tells us the length of the elements in 'SUBPARTIDA'. iii) \ Whenever
    # 'SUBPARTIDA' is of length 9, add a 0 at the beginning. 'SUBPARTIDA' must be 10-digit. iv) The chapter reference \
    # are the first two characters/digits in 'SUBPARTIDA'
    df['SUBPARTIDA'] = df['SUBPARTIDA'].astype('str')
    df['SUBPART_LENGTH'] = df['SUBPARTIDA'].str.len()
    df['SUBPARTIDA'] = np.where(df['SUBPART_LENGTH'] == 9, '0' + df['SUBPARTIDA'], df['SUBPARTIDA'])
    df['chap'] = df['SUBPARTIDA'].str.slice(stop=2)

    # Add up all duties to form a single column: `import_duties`
    duties=[
        'SALVAGUARDIA', 
        'SALVAGUARDIA_ESPECIFICA', 
        'FODINFA', 
        'ICE_ADVALOREM', 
        'ICE_ESPECIFICO', 
        'IVA', 
        'ADVALOREM', 
        'ADVALOREM_ESPECIFICO'
    ]
    # new var is going to be name derechos, as a rough translation of import duties
    df['duties'] = df[duties].sum(axis=1)

    # from variable `DISTRITO` extract specific code for the district as well as name
    df['distr_code'] = df['DISTRITO'].str.replace(' ', '').str.split('-', expand=True)[0]
    df['distr_city'] = df['DISTRITO'].str.replace(' ', '').str.split('-', expand=True)[1]
    df['distr_compl'] = df['DISTRITO'].str.replace(' ', '').str.split('-', expand=True)[2]

    # Capitalize `distr_city`, `` so as to be able to do later merge
    df['distr_city'] = df['distr_city'].str.capitalize()
    df['distr_compl'] = df['distr_compl'].str.capitalize()

    df['distr_name'] = np.where(df['distr_compl'].isna(), df['distr_city'], df['distr_city'] + '-' + df['distr_compl'])

    # Actual city at border is Macará, not Loja. Variable `distr_city` will be modify in accordance
    df.loc[df['distr_compl'] == 'MACARA', 'distr_city'] = 'Macara'

    # Construct a column with city names to later include latitudes and longitudes for ports of entry and exit
    port_dict = {
        '028-GUAYAQUIL - MARITIMO' : 'Guayaquil',
        '019-GUAYAQUIL - AEREO' : 'Guayaquil',
        '055-QUITO' : 'Quito',
        '073-TULCAN' : 'Tulcán',
        '082-HUAQUILLAS' : 'Huaquillas',
        '064-PUERTO BOLIVAR' : 'Machala',
        '037-MANTA': 'Manta',
        '127-LATACUNGA' : 'Latacunga',
        '046-ESMERALDAS' : 'Esmeraldas',
        '145-CEBAF SAN MIGUEL' : 'Nueva Loja',
        '109-LOJA - MACARA': 'Macará',
        '091-CUENCA' : 'Cuenca'
    }

    df['cty_ref'] = df['DISTRITO']

    df.replace(
        {'cty_ref':port_dict}
        , inplace=True)

    # merging main df with supporting dfs to include geographical data
    df0 = pd.merge(df, sa, how='left', left_on='chap', right_on='CAPÍTULO')
    df1 = pd.merge(df0, cty_, how='left', left_on='cty_ref', right_on='city')
    df2 = pd.merge(df1, cnty_, how = 'left', left_on='iso', right_on='country')

    # define dictionary to change variables names
    varnames_new = {
    'TIPO_IMPORTACION' : 'tip_import', 'FOB' : 'fob', 'FLETE' : 'flete', 
    'SEGURO' : 'insurance', 'CIF' : 'cif', 'SALVAGUARDIA' : 'salvaguardia', 
    'SALVAGUARDIA_ESPECIFICA' : 'salvaguardia_espec', 'FODINFA' : 'fodinfa', 
    'ICE_ADVALOREM' : 'ice_advalorem', 'ICE_ESPECIFICO' : 'ice_espec', 
    'IVA' : 'iva', 'ADVALOREM' : 'advalorem', 'ADVALOREM_ESPECIFICO' : 'advalorem_espec', 
    'TÍTULO' : 'titulo', 'lat' : 'cty_lat', 'lng' : 'cty_lng', 'latitude' : 'pais_lat', 'longitude' : 'pais_lng'
    }

    # rename variable names
    df2.rename(columns=varnames_new, inplace=True)

    keep = ['date', 'tip_import', 'fob', 'flete', 'insurance',  
    'cif', 'salvaguardia', 'salvaguardia_espec', 
    'fodinfa', 'ice_advalorem', 'ice_espec', 'iva', 
    'advalorem', 'advalorem_espec', 'iso', 'pais', 'chap',
    'duties', 'distr_code', 'distr_name', 'cty_ref', 'titulo',
    'cty_lat', 'cty_lng', 'pais_lat', 'pais_lng']

    df = df2[keep]

    # aggregate to monthly frequency
    df = df.groupby([
        df['date'].dt.strftime('%Y-%m'),
        'tip_import', 'iso', 'pais', 'pais_lat', 'pais_lng',
        'distr_code', 'distr_name', 'cty_ref', 'cty_lat', 'cty_lng',
        'titulo', 'chap'
    ]).agg({
        'fob' : 'sum',
        'flete' : 'sum',
        'insurance' : 'sum',
        'cif' : 'sum', 
        'salvaguardia' : 'sum',
        'salvaguardia_espec' : 'sum',
        'fodinfa' : 'sum', 
        'ice_advalorem' : 'sum',
        'ice_espec' : 'sum',
        'iva' : 'sum', 
        'advalorem' : 'sum', 
        'advalorem_espec' : 'sum',
        'duties': 'sum'
    }).reset_index()

    # set `date` column as index
    df.set_index('date', inplace=True)

    # save the df containing imports data for whole year on specified location
    df.to_csv(f'../senaem/files/importaciones_{year}.csv')



if __name__ == '__main__':

    anio_list = [2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022]

    for anio in anio_list:
        file_paths_anio = glob.glob(os.path.join('../senaem/downloads/', f'senae_importacion*{anio}.csv'))
        process(file_paths=file_paths_anio, year=anio)