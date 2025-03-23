import pandas as pd
import requests
import urllib3
import concurrent.futures

from . import cod


URL_BCRA = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/series.xlsm'
URL_BCRA_TCRM = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/ITCRMSerie.xlsx'
URL_BCRA_TC = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/com3500.xls'
URL_OPER = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/Data_operaciones.xlsx'
URL_BCRA_BAL = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt'
URL_BCRA_RES = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din2_ser.txt'
URL_BCRA_ACT = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din3_ser.txt'
URL_BCRA_PAS = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din4_ser.txt'
URL_API_MON = 'https://api.bcra.gob.ar/estadisticas/v3.0/monetarias'


def get_file_bcra(sheet_name: str) -> pd.DataFrame:
    """
    Devuelve un df con los datos de la hoja seleccionada del archivo series.xlsm del BCRA..
    :param sheet_name: Nombre de la hoja a devolver.
    :return: df con datos de la hoja seleccionada.
    """
    return pd.read_excel(URL_BCRA, header=[0, 1, 2, 3, 4, 5, 6, 7, 8], sheet_name=sheet_name)

def get_file_tc_oficial():
    df = pd.read_excel(URL_BCRA_TC, header=3).dropna(axis='columns')
    df.columns = ['Fecha', 'TC_A3500']
    df.index = pd.to_datetime(df['Fecha'])
    return df

def get_principales_variables():
    """
    Devuelve un df con las variables que se pueden acceder mediante la API del BCRA y sus respectivos códigos.
    :return: df 'idVariable', 'cdSerie', 'descripcion', 'fecha', 'valor'.
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = requests.get(URL_API_MON, verify=False)
    return pd.DataFrame(response.json()['results'])

def get_from_api(idvariable: int, nombre: str):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = requests.get(f'{URL_API_MON}/{idvariable}?limit=3000', verify=False)
    df = pd.DataFrame(response.json()['results']).drop(columns=['idVariable']).set_index('fecha', drop=True)
    while response.json()['results']:
        desde = pd.to_datetime(df.index[-1]) - pd.offsets.DateOffset(3000)
        hasta = pd.to_datetime(df.index[-1]) - pd.offsets.DateOffset()
        response = requests.get(f'{URL_API_MON}/{idvariable}?desde={desde}&hasta={hasta}&limit=3000', verify=False)
        if response.json()['results']:
            df = pd.concat(
                [df, pd.DataFrame(response.json()['results']).drop(columns=['idVariable']).set_index('fecha', drop=True)])
    df.index = pd.to_datetime(df.index)
    df.index.name = 'Fecha'
    df.columns = [nombre]
    return df.sort_index()

def get_series_api(arguments: list[tuple]) -> pd.DataFrame:
    with concurrent.futures.ThreadPoolExecutor() as executor:
        idvariable = [arg[0] for arg in arguments]
        nombre = [arg[1] for arg in arguments]
        results = executor.map(get_from_api, idvariable, nombre)
    df = pd.concat(list(results), axis='columns')
    df['Fecha'] = df.index
    return df

def get_file_bcra_plus(file: int, serie: list[int], div: bool = True) -> pd.DataFrame:
    def download(link: str, dividir: bool) -> pd.DataFrame:
        df = pd.read_csv(link, sep=';', names=['Cat', 'Fecha', 'Monto'],
                         dtype={'Cat': str, 'Fecha': str, 'Monto': float}).dropna()
        df['Cat'] = pd.to_numeric(df.loc[:, 'Cat'])
        df['Fecha'] = pd.to_datetime(df.loc[:, 'Fecha'], format='%d/%m/%Y')
        df['Monto'] = pd.to_numeric(df.loc[:, 'Monto'])
        data = df.pivot_table(columns='Cat', values='Monto', index='Fecha')
        data.columns.name = None
        data.index.name = None
        if dividir:
            data = data / 1000
        data['Fecha'] = data.index
        return data

    match file:
        case 1:
            return download(URL_BCRA_BAL, div)[['Fecha'] + serie].copy()
        case 2:
            return download(URL_BCRA_RES, div)[['Fecha'] + serie].copy()
        case 3:
            return download(URL_BCRA_ACT, div)[['Fecha'] + serie].copy()
        case 4:
            return download(URL_BCRA_PAS, div)[['Fecha'] + serie].copy()
        case _:
            return pd.DataFrame()

def get_plazo_fijos(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """
    Devuelve un df con los datos diarios de los depósitos a plazo fijo.
    :param date_cod: Define si agregan columnas para código de fecha 'Date'.
    :param api: Define si se utiliza la API del BCRA o un el archivo series.xlsm local.
    :return: df 'PF', 'PF_UVA', 'PF_Privado', 'PF_UVA_Privado', 'Fecha', 'Dia', 'Date', 'Mes', 'Año'.
    """
    if api:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            arguments = [(87, 'PF'), (88, 'PF_UVA'), (96, 'PF_Privado'), (97, 'PF_UVA_Privado')]
            idvariable = [arg[0] for arg in arguments]
            nombre = [arg[1] for arg in arguments]
            results = executor.map(get_from_api, idvariable, nombre)
        df = pd.concat(list(results), axis='columns')
        df['Fecha'] = df.index
    else:
        df = get_file_bcra('DEPOSITOS')
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        df = df.loc[df['30'] == 'D', ['1', '4', '5', '13', '14']]
        df.columns = ['Fecha', 'PF_total', 'PF_UVA_total', 'PF_privado', 'PF_UVA_privado']
    if date_cod:
        return cod.get_date(df)
    return df

def get_base_monetaria(date_cod: bool = False, api: bool = True, q: bool = False, only_BMT: bool = False) -> pd.DataFrame:
    """
    Devuelve un df con los datos diarios de 'BMT', 'CM', 'df', 'DPB', 'CCBCRA', 'CC', 'BM', 'DT'.
    'BMT': Base monetaria totol = 'DPP' + 'DPB' + 'CCBCRA' + 'CC'.
    'CM': Circulación monetaria.
    'DPP': Billetes y Monedas en Poder del Público.
    'DPB': Billetes y Monedas en Entidades (Dinero en poder de bancos).
    'CCBCRA': Cuenta Corriente en el BCRA (de los bancos).
    'CC': Cheques cancelatorios.
    'QM': Cuasimonedas.
    'BMTQ': 'BMT' + 'QM'.
    'DT': Dinero total = 'DPP' + 'DPB'.

    :param date_cod: Define si agregan columns para código de fecha 'Date'.
    :param api: Define si se utiliza la API del BCRA o un el archivo series.xlsm local.
    :param q:
    :param only_BMT:
    :return: df 'Fecha', 'Date', 'Dia', 'BMT', 'CM', 'DPP', 'DPB', 'CCBCRA', 'CC', 'QM', 'BMTQ', 'DT'.
    """
    columns = ['BMT', 'CM', 'DPP', 'DPB', 'CCBCRA', 'CC', 'QM', 'BMTQ', 'DT']

    if api:
        if only_BMT:
            df = get_series_api([(15, 'BMT')])
            columns = ['BMT']
        else:
            if q:
                df = get_series_api(
                    [(15, 'BMT'), (16, 'CM'), (17, 'DPP'), (18, 'DPB'), (19, 'CCBCRA'), (69, 'CC'), (72, 'QM'),
                     (73, 'BMTQ')])
            else:
                df = get_series_api(
                    [(15, 'BMT'), (16, 'CM'), (17, 'DPP'), (18, 'DPB'), (19, 'CCBCRA')])
                columns = ['BMT', 'CM', 'DPP', 'DPB', 'CCBCRA', 'DT']
    else: # En el archivo está la fecha 2010-12-31 y en la API no.
        df = get_file_bcra('BASE MONETARIA')
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        df = df.loc[df['33'] == 'D', ['1', '26', '27', '28', '29', '30', '31', '32']].copy()
        df.columns = ['Fecha', 'DPP', 'DPB', 'CC', 'CCBCRA', 'BMT', 'QM', 'BMTQ']
        df['CM'] = df['DPP'] + df['DPB'] + df['CC']
        df.index = df['Fecha']
    if not only_BMT:
        df['DT'] = df['DPP'] + df['DPB']
    if date_cod:
        return cod.get_date(df)[cod.COLS + columns].copy().dropna().sort_index()
    return df.dropna()

def get_lefis(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """
    Devuelve un df con los datos diarios de 'BMT', 'CM', 'df', 'DPB', 'CCBCRA', 'CC', 'BM', 'DT'.
    Devuelve un df con los datos diarios del stock de 'LEFI' y su flujo 'LEFI_Flujo'.
    :param date_cod:
    :param api:
    :return:
    """

    if api:
        df = get_series_api([(196, 'LEFI'), (58, 'LEFI_Flujo')])
    else:
        df = pd.read_excel(URL_OPER, header=[0, 1, 2])
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        df = df.dropna(subset=['1', '2'])
        df = df.loc[:, ['1', '3', '4', '5', '6']].copy()
        df.columns = ['Fecha', 'VT', 'Publico', 'Privado', 'BCRA']
        df['LEFI'] = df['Publico'] + df['Privado']
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        df = df.set_index('Fecha', drop=False)

        # Por cumsum en el archivo series.xlsx
        # df = get_file_bcra('BASE MONETARIA')
        # df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        # lefis = df.loc[df['33'] == 'D', ['1', '15']].copy()
        # lefis.columns = ['Fecha', 'LEFI_Flujo']
        # lefis['LEFI'] = -lefis['LEFI_Flujo'].cumsum()
    if date_cod:
        return cod.get_date(df)[cod.COLS + ['LEFI', 'LEFI_Flujo']].dropna()
    else:
        return df.dropna()

def get_instrumentos(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """

    :param date_cod:
    :param api:
    :return: df 'Fecha', 'Date', 'Dia', 'Pases_Pasivos', 'Pases_Pasivos_FCI', 'Pases_Activos', 'LELIQs', 'LEBACs', 'LEBACsD_LEVID_BOPREAL', 'NOCOMs'
    """
    if api: # Los pases pasivos tienen un día menos de datos con la API.
        df = get_series_api([(42, 'Pases_Pasivos'), (154, 'Pases_Activos'), (153, 'Pases_Pasivos_FCI'), (156, 'LEBACs'), (155, 'LELIQs'), (158, 'LEBACsD_LEVID_BOPREAL'), (159, 'NOCOMs')])
    else:
        df = get_file_bcra('INSTRUMENTOS DEL BCRA')
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        df = df[['1', '2', '3', '4', '5', '6', '8', '9']].copy()
        df.columns = ['Fecha', 'Pases_Pasivos', 'Pases_Pasivos_FCI', 'Pases_Activos', 'LELIQs', 'LEBACs', 'LEBACsD_LEVID_BOPREAL',
                       'NOCOMs']
        df[df.columns[1:]] = df.loc[:, 'Pases_Pasivos': 'NOCOMs'].apply(pd.to_numeric, errors='coerce').fillna(0)

    if date_cod:
        return cod.get_date(df)[cod.COLS + ['Pases_Pasivos', 'Pases_Pasivos_FCI', 'Pases_Activos', 'LELIQs', 'LEBACs',
                                                 'LEBACsD_LEVID_BOPREAL', 'NOCOMs']]
    return df

def get_dep_gob_pesos(date_cod: bool = False) -> pd.DataFrame:
    df = get_file_bcra_plus(2, [8842])
    df = df.rename(columns={8842: 'Depositos_gob'})
    if date_cod:
        return cod.get_date(df).dropna()
    else:
        return df.dropna()

def get_tc_oficial(date_cod: bool = False, api: bool = True, mensual: bool = False) -> pd.DataFrame:
    """
    Devuelve un df con los valores del tipo de cambio oficial A3500.
    :param date_cod: Define si agregan columnas para código de fecha 'Date'.
    :param mensual: Define si se devuelven promedios mensuales.
    :param api: Define si se utiliza la API del BCRA o un el archivo com3500.xls local.
    :return: df 'Date', 'Dia', 'TC_A3500'.
    """
    if api:
        if mensual:
            df = get_from_api(5, 'TC_A3500').resample('MS').mean()
            df['Fecha'] = df.index
        else:
            df = get_from_api(5, 'TC_A3500')
            df['Fecha'] = df.index
    else:
        if mensual:
            df = get_file_tc_oficial().resample('MS').mean()
            df['Fecha'] = df.index
        else:
            df = get_file_tc_oficial()
    if date_cod:
        return cod.get_date(df)[cod.COLS + ['TC_A3500']]
    return df.dropna()

def get_cer() -> pd.DataFrame:
    """
    Devuelve un df con la serie del coeficiente CER indizada por la fecha.
    :return: df 'CER'
    """
    return get_from_api(30, 'CER')

def get_reservas(date_cod: bool = False, api: bool = True ) -> pd.DataFrame:
    """
    Devuelve un df con la serie de las reservas internacionales brutas del BCRA indizado con la fecha.
    :param date_cod:
    :param api:
    :return: df 'Fecha', 'RRII'
    """
    if api:
        df = get_from_api(1, 'RRII')
        df['Fecha'] = df.index
    else:
        # reservas = get_file_bcra_plus(2, [246]).dropna()
        df = get_file_bcra('RESERVAS')
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        df = df[df['17'] == 'D'][['1', '3']].copy()
        df.columns = ['Fecha', 'RRI']
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        df.index = df['Fecha']

    if date_cod:
        return cod.get_date(df)[cod.COLS + ["RRII"]]
    return df

def get_creditos(date_cod: bool = False, online: bool = True) -> pd.DataFrame:
    if online:
        df = get_from_api(117, 'Préstamos')
    else:
        df = get_file_bcra("PRESTAMOS")
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        df = df.loc[df["22"] == "D", ["1", "9"]].copy()
        df = df.rename(columns={"1": "Fecha", "9": "Creditos"})
    if date_cod:
        return cod.get_date(df)
    else:
        return df

# ----------------------------------------------------------------

def get_tasas(date_cod: bool = False, api: bool = True, type: int = 0) -> pd.DataFrame:
    """
    Devuelve un df con los datos diarios de las tasas de interés para los plazos fijos de 30-44 días.
    :param date_cod: Si el df debe contener el código 'Date' o no.
    :param api:
    :param type:
    :return: df 'TNA', 'TEM', 'TEA'.
    """
    columns_pesos = ['TNA_GenP', 'TNA_100KP', 'TNA_1MP', 'Fecha']
    columns_dolares = ['TNA_GenD', 'TNA_100KD', 'TNA_1MD', 'Fecha']


    def get_file() -> pd.DataFrame:
        df = get_file_bcra('TASAS DE MERCADO')
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        return df

    def calculate_rates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """
        Calculate TEM and TEA for a subset of interest rate data.
        """
        df.columns = columns

        for tasa in columns[:-1]:  # Skip 'Fecha'
            df[tasa] = df[tasa] / 100

            tem_col = f'TEM_{tasa.split("_")[1]}'
            df[tem_col] = df[tasa] / 12

            tea_col = f'TEA_{tasa.split("_")[1]}'
            df[tea_col] = (1 + df[tem_col]) ** 12 - 1
        df.index = df['Fecha']
        return df

    # Process based on type_value
    match type:
        case 0:  # Pesos only
            if api:
                tasas_pesos = get_series_api([(128, 'TNA_genP'), (129, 'TNA_100KP'), (131, 'TNA_1MP')])
            else:
                df = get_file()
                tasas_pesos = df[['2', '3', '5', '1']].copy()
            df = calculate_rates(tasas_pesos, columns_pesos)
        case 1:  # Both pesos and dollars
            if api:
                tasas_pesos = get_series_api([(128, 'TNA_genP'), (129, 'TNA_100KP'), (131, 'TNA_1MP')])
                tasas_dolares = get_series_api([(132, 'TNA_genD'), (133, 'TNA_100KD'), (134, 'TNA_1MD')]).fillna(0) # Se pone cero en los NA de 'TNA_1MD'
            else:
                df = get_file()
                tasas_pesos = df[['2', '3', '5', '1']].copy()
                tasas_dolares = df[['6', '7', '8', '1']].copy()
                tasas_dolares['8'] = pd.to_numeric(tasas_dolares['8'], errors='coerce').fillna(0) # Se pone cero en los NA de 'TNA_1MD'
            df = pd.concat([calculate_rates(tasas_pesos, columns_pesos).drop(columns='Fecha'),
                            calculate_rates(tasas_dolares, columns_dolares)],
                           axis='columns')
        case 2:  # Dollars only
            if api:
                tasas_dolares = get_series_api([(132, 'TNA_genD'), (133, 'TNA_100KD'), (134, 'TNA_1MD')]).fillna(0) # Se pone cero en los NA de 'TNA_1MD'
            else:
                df = get_file()
                tasas_dolares = df[['6', '7', '8', '1']].copy()
                tasas_dolares['8'] = pd.to_numeric(tasas_dolares['8'], errors='coerce').fillna(0)  # Se pone cero en los NA de 'TNA_1MD'
            df = calculate_rates(tasas_dolares, columns_dolares)

        case _:
            raise ValueError(f"Invalid type_value: {type}. Must be 0, 1, or 2.")
    df = df.drop(columns='Fecha')
    df['Fecha'] = df.index
    if date_cod:
        return cod.get_date(df)
    return df




def main() -> None:
    ...


if __name__ == '__main__':
    main()
