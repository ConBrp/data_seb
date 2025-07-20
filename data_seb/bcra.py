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


def get_file_bcra(sheet_name: str = '', download_file: bool = False) -> pd.DataFrame|None:
    """
    Devuelve un DataFrame con los datos de la hoja seleccionada del archivo series.xlsm del BCRA.

    Returns a DataFrame with data from the selected sheet of the BCRA's series.xlsm file.

    :param sheet_name: Nombre de la hoja a devolver / Name of the sheet to retrieve.
    :return: DataFrame con datos de la hoja seleccionada / DataFrame with data from the selected sheet.
    """
    if download_file:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        response = requests.get(URL_BCRA, verify=False)
        with open('series.xlsm', 'wb') as archivo:
            archivo.write(response.content)
        return
    return pd.read_excel(URL_BCRA, header=[0, 1, 2, 3, 4, 5, 6, 7, 8], sheet_name=sheet_name)

def get_file_tc_oficial() -> pd.DataFrame:
    """
    Devuelve un DataFrame con los datos del tipo de cambio oficial (A3500) del archivo com3500.xls del BCRA.

    Returns a DataFrame with the official exchange rate (A3500) of the BCRA's com3500.xls file.

    :return: DataFrame 'Fecha', 'TC_A3500' / DataFrame 'Fecha', 'TC_A3500'.
    """
    df = pd.read_excel(URL_BCRA_TC, header=3).dropna(axis='columns')
    df.columns = ['Fecha', 'TC_A3500']
    df.index = pd.to_datetime(df['Fecha'])
    return df

def get_file_itcrm(sheet_name: str) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los datos del Índice de Tipo de Cambio Real Multilateral (ITCRM) del BCRA.

    Returns a DataFrame with data from the Multilateral Real Exchange Rate Index (ITCRM) of the BCRA.

    :param sheet_name: Nombre de la hoja a procesar (diaria o promedio mensual) / Name of the sheet to process (daily or monthly average).
    :return: DataFrame con columnas 'Fecha' y datos del ITCRM / DataFrame with 'Fecha' column and ITCRM data.
    """
    df = pd.read_excel(URL_BCRA_TCRM, sheet_name=sheet_name, header=[1]).dropna().rename(columns={'Período': 'Fecha'})
    df['Fecha'] = pd.to_datetime(df['Fecha'])
    return df

def get_file_bcra_plus(file: int, serie: list[int], div: bool = True) -> pd.DataFrame:
    """
    Devuelve un DataFrame con datos del BCRA desde un archivo específico, opcionalmente dividiendo los valores por 1000.

    Returns a DataFrame with BCRA data from a specific file, optionally dividing values by 1000.

    :param file: Identificador del archivo (1: BAL, 2: RES, 3: ACT, 4: PAS) / File identifier (1: BAL, 2: RES, 3: ACT, 4: PAS).
    :param serie: Lista de códigos de series a obtener / List of series codes to retrieve.
    :param div: Si es True, divide los valores por 1000. Por defecto es True / If True, divide the values by 1000. Defaults to True.
    :return: DataFrame con las series seleccionadas y una columna 'Fecha' / DataFrame with the selected series and a 'Fecha' column.
    """

    def download(link: str, dividir: bool) -> pd.DataFrame:
        """
        Descarga y preprocesa datos del BCRA desde una URL dada.

        Downloads and preprocesses BCRA data from a given URL.

        :param link: URL del archivo a descargar / URL of the file to download.
        :param dividir: Si es True, divide los valores por 1000 / If True, divide the values by 1000.
        :return: DataFrame preprocesado con una columna 'Fecha' / Preprocessed DataFrame with a 'Fecha' column.
        """
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

def get_principales_variables() -> None:
    """
    Devuelve un DataFrame con las variables principales accesibles mediante la API del BCRA y sus códigos.

    Returns a DataFrame with the main variables accessible via the BCRA API and their codes.

    :return: DataFrame 'idVariable', 'cdSerie', 'descripcion', 'fecha', 'valor' / DataFrame 'idVariable', 'cdSerie', 'descripcion', 'fecha', 'valor'.
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = requests.get(URL_API_MON, verify=False)
    return pd.DataFrame(response.json()['results']).to_excel('Principales_variables.xlsx')

def get_from_api(idvariable: int, nombre: str) -> pd.DataFrame:
    """
    Devuelve un DataFrame con datos históricos de una variable específica desde la API del BCRA.

    Returns a DataFrame with historical data for a specific variable from the BCRA API.

    :param idvariable: ID de la variable a obtener / ID of the variable to retrieve.
    :param nombre: Nombre a asignar a la columna de la variable en el DataFrame / Name to assign to the variable column in the DataFrame.
    :return: DataFrame con los datos de la variable, indexado por fecha / DataFrame with the variable data, indexed by date.
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    response = requests.get(f'{URL_API_MON}/{idvariable}?limit=3000', verify=False)
    df = pd.DataFrame(response.json()['results']).drop(columns=['idVariable']).set_index('fecha', drop=True)
    while response.json()['results']:
        desde = pd.to_datetime(df.index[-1]) - pd.offsets.DateOffset(3000)
        hasta = pd.to_datetime(df.index[-1]) - pd.offsets.DateOffset()
        response = requests.get(f'{URL_API_MON}/{idvariable}?desde={desde}&hasta={hasta}&limit=3000', verify=False)
        if response.json()['results']:
            df = pd.concat(
                [df,
                 pd.DataFrame(response.json()['results']).drop(columns=['idVariable']).set_index('fecha', drop=True)])
    df.index = pd.to_datetime(df.index)
    df.columns = [nombre]
    return df.sort_index()

def get_series_api(arguments: list[tuple], date: bool = False) -> pd.DataFrame:
    """
    Devuelve un DataFrame con múltiples variables desde la API del BCRA usando solicitudes concurrentes.

    Returns a DataFrame with multiple variables from the BCRA API using concurrent requests.

    :param arguments: Lista de tuplas con (idvariable, nombre) para cada variable a obtener / List of tuples with (idvariable, nombre) for each variable to retrieve.
    :return: DataFrame con columnas para cada variable y una columna 'Fecha' / DataFrame with columns for each variable and a 'Fecha' column.
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        idvariable = [arg[0] for arg in arguments]
        nombre = [arg[1] for arg in arguments]
        results = executor.map(get_from_api, idvariable, nombre)
    df = pd.concat(list(results), axis='columns')
    df.index.name = None
    if date:
        df['Date'] = df.index
    return df

def get_fixed_term_deposits(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los datos diarios de los depósitos a plazo fijo.

    Returns a DataFrame with daily data on fixed-term deposits from the BCRA.

    :param date_cod: Define si agregan columnas para código de fecha 'Date' / If True, add columns for date code 'Date'.
    :param api: Define si se utiliza la API del BCRA o un el archivo series.xlsm local / If True, use the BCRA API; otherwise, use the local series.xlsm file.
    :return: DataFrame 'PF', 'PF_UVA', 'PF_Privado', 'PF_UVA_Privado', 'Fecha', 'Dia', 'Date', 'Mes', 'Año' / DataFrame 'PF', 'PF_UVA', 'PF_Privado', 'PF_UVA_Privado', 'Fecha', 'Dia', 'Date', 'Mes', 'Año'.
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

def get_current_account_bcra() -> pd.DataFrame:
    return get_from_api(70, 'CA_BCRA')

def get_monetary_base(date_cod: bool = False, api: bool = True, q: bool = False,
                       only_bmt: bool = False) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los datos diarios de 'BMT', 'CM', 'df', 'DPB', 'CCBCRA', 'CC', 'BM', 'DT'.

    Returns a DataFrame with daily data on monetary base components from the BCRA.

    'BMT': Base monetaria totol = 'DPP' + 'DPB' + 'CCBCRA' + 'CC' / Total monetary base = 'DPP' + 'DPB' + 'CCBCRA' + 'CC'.
    'CM': Circulación monetaria / Monetary circulation.
    'DPP': Billetes y Monedas en Poder del Público / Notes and coins held by the public.
    'DPB': Billetes y Monedas en Entidades (Dinero en poder de bancos) / Notes and coins in entities (money held by banks).
    'CCBCRA': Cuenta Corriente en el BCRA (de los bancos) / Current account at the BCRA (of banks).
    'CC': Cheques cancelatorios / Cancelled checks.
    'QM': Cuasimonedas / Quasi-money.
    'BMTQ': 'BMT' + 'QM' / 'BMT' + 'QM'.
    'DT': Dinero total = 'DPP' + 'DPB' / Total money = 'DPP' + 'DPB'.

    :param date_cod: Define si agregan columns para código de fecha 'Date' / If True, add columns for date code 'Date'.
    :param api: Define si se utiliza la API del BCRA o un el archivo series.xlsm local / If True, use the BCRA API; otherwise, use the local series.xlsm file.
    :param q: Si es True, incluye cuasi-monedas ('QM') y 'BMTQ' / If True, include quasi-money ('QM') and 'BMTQ'.
    :param only_bmt: Si es True, devuelve solo la base monetaria total ('BMT') / If True, return only the total monetary base ('BMT').
    :return: DataFrame 'Fecha', 'Date', 'Dia', 'BMT', 'CM', 'DPP', 'DPB', 'CCBCRA', 'CC', 'QM', 'BMTQ', 'DT' / DataFrame 'Fecha', 'Date', 'Dia', 'BMT', 'CM', 'DPP', 'DPB', 'CCBCRA', 'CC', 'QM', 'BMTQ', 'DT'.
    """
    columns = ['BMT', 'CM', 'DPP', 'DPB', 'CCBCRA', 'CC', 'QM', 'BMTQ', 'DT']

    if api:
        if only_bmt:
            df = get_from_api(15, 'BMT')
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
    else:  # En el archivo está la fecha 2010-12-31 y en la API no.
        df = get_file_bcra('BASE MONETARIA')
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        df = df.loc[df['33'] == 'D', ['1', '26', '27', '28', '29', '30', '31', '32']].copy()
        df.columns = ['Fecha', 'DPP', 'DPB', 'CC', 'CCBCRA', 'BMT', 'QM', 'BMTQ']
        df['CM'] = df['DPP'] + df['DPB'] + df['CC']
        df.index = df['Fecha']
    if not only_bmt:
        df['DT'] = df['DPP'] + df['DPB']
    if date_cod:
        df['Date'] = df.index
        return cod.get_date(df)[cod.COLS + columns].dropna().sort_index().copy()
    return df.dropna()

def  get_m2(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    if api:
        if date_cod:
            df = get_from_api(109, 'M2')
            df['Date'] = df.index
            return cod.get_date(df)
        return get_from_api(109, 'M2')
    else:
        return pd.DataFrame()

def get_lefis(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los datos diarios del stock de LEFI y su flujo (LEFI_Flujo).

    Returns a DataFrame with daily data on LEFI stock and its flow (LEFI_Flujo) from the BCRA.

    :param date_cod: Define si agregan columnas para código de fecha 'Date' / If True, add columns for date code 'Date'.
    :param api: Define si se utiliza la API del BCRA o un el archivo series.xlsm local / If True, use the BCRA API; otherwise, use the local series.xlsm file.
    :return: DataFrame 'LEFI', 'LEFI_Flujo', 'Fecha', 'Date', 'Dia', 'Mes', 'Año' / DataFrame 'LEFI', 'LEFI_Flujo', 'Fecha', 'Date', 'Dia', 'Mes', 'Año'.
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
    if date_cod:
        return cod.get_date(df)[cod.COLS + ['LEFI', 'LEFI_Flujo']].dropna()
    else:
        return df.dropna()

def get_lebacs(date_cod: bool = False, api: bool = True):
    """
    Saldo de LEBAC y NOBAC en Pesos, LEGAR y LEMIN  (en millones de $)
    :param date_cod:
    :param api:
    :return:
    """
    return get_from_api(156, 'LEBACs')

def get_leliqs() -> pd.DataFrame:
    """
    Saldo de LELIQ y NOTALIQ (en millones de $)
    :return:
    """
    return get_from_api(155, 'LELIQs')

def get_repo():
    """
    Pases activos
    :return:
    """
    return get_from_api(154, 'REPOs')

def get_reverse_repo():
    """
    Pases pasivos
    :return:
    """
    return get_from_api(152, 'REVERSE_REPO')

def get_monetary_instruments(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los datos diarios de los instrumentos monetarios del BCRA.

    Returns a DataFrame with daily data on BCRA monetary instruments.

    :param date_cod: Define si agregan columnas para código de fecha 'Date' / If True, add columns for date code 'Date'.
    :param api: Define si se utiliza la API del BCRA o un el archivo series.xlsm local / If True, use the BCRA API; otherwise, use the local series.xlsm file.
    :return: DataFrame 'Fecha', 'Date', 'Dia', 'Pases_Pasivos', 'Pases_Pasivos_FCI', 'Pases_Activos', 'LELIQs', 'LEBACs', 'LEBACsD_LEVID_BOPREAL', 'NOCOMs' / DataFrame 'Fecha', 'Date', 'Dia', 'Pases_Pasivos', 'Pases_Pasivos_FCI', 'Pases_Activos', 'LELIQs', 'LEBACs', 'LEBACsD_LEVID_BOPREAL', 'NOCOMs'.
    """
    if api:  # Los pases pasivos tienen un día menos de datos con la API.
        df = get_series_api([(42, 'Pases_Pasivos'), (154, 'Pases_Activos'), (153, 'Pases_Pasivos_FCI'), (156, 'LEBACs'),
                             (155, 'LELIQs'), (158, 'LEBACsD_LEVID_BOPREAL'), (159, 'NOCOMs')])
    else:
        df = get_file_bcra('INSTRUMENTOS DEL BCRA')
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        df = df[['1', '2', '3', '4', '5', '6', '8', '9']].copy()
        df.columns = ['Fecha', 'Pases_Pasivos', 'Pases_Pasivos_FCI', 'Pases_Activos', 'LELIQs', 'LEBACs',
                      'LEBACsD_LEVID_BOPREAL',
                      'NOCOMs']
        df[df.columns[1:]] = df.loc[:, 'Pases_Pasivos': 'NOCOMs'].apply(pd.to_numeric, errors='coerce').fillna(0)

    if date_cod:
        return cod.get_date(df)[cod.COLS + ['Pases_Pasivos', 'Pases_Pasivos_FCI', 'Pases_Activos', 'LELIQs', 'LEBACs',
                                            'LEBACsD_LEVID_BOPREAL', 'NOCOMs']]
    return df

def get_government_deposits(date_cod: bool = False) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los datos diarios de los depósitos del gobierno en pesos del BCRA.

    Returns a DataFrame with daily data on government deposits in pesos from the BCRA.

    :param date_cod: Define si agregan columnas para código de fecha 'Date' / If True, add columns for date code 'Date'.
    :return: DataFrame 'Fecha', 'Depositos_gob', 'Date', 'Dia', 'Mes', 'Año' / DataFrame 'Fecha', 'Depositos_gob', 'Date', 'Dia', 'Mes', 'Año'.
    """
    df = get_file_bcra_plus(2, [8842])
    df = df.rename(columns={8842: 'Depositos_gob'})
    if date_cod:
        return cod.get_date(df).dropna()
    else:
        return df.dropna()

def get_accounting_exchange_rate() -> pd.DataFrame:
    return get_from_api(84, 'A_Exchange_Rate')

def get_official_exchange_rate(date_cod: bool = False, api: bool = True, mensual: bool = False) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los valores del tipo de cambio oficial (A3500) del BCRA.

    Returns a DataFrame with the official exchange rate (A3500) values from the BCRA.

    :param date_cod: Define si agregan columnas para código de fecha 'Date' / If True, add columns for date code 'Date'.
    :param api: Define si se utiliza la API del BCRA o un el archivo com3500.xls local / If True, use the BCRA API; otherwise, use the local com3500.xls file.
    :param mensual: Define si se devuelven promedios mensuales / If True, return monthly averages.
    :return: DataFrame 'Fecha', 'TC_A3500', 'Date', 'Dia', 'Mes', 'Año' / DataFrame 'Fecha', 'TC_A3500', 'Date', 'Dia', 'Mes', 'Año'.
    """
    if api:
        if mensual:
            df = get_from_api(5, 'A3500_ER').resample('MS').mean()
        else:
            df = get_from_api(5, 'A3500_ER')
    else:
        if mensual:
            df = get_file_tc_oficial().resample('MS').mean()
        else:
            df = get_file_tc_oficial()
    if date_cod:
        df['Fecha'] = df.index
        return cod.get_date(df)[cod.COLS + ['A3500_ER']]
    return df.dropna()

def get_retail_exchange_rate() -> pd.DataFrame:
    return get_from_api(4, 'R_exchange_rate')

def get_cer() -> pd.DataFrame:
    """
    Devuelve un DataFrame con la serie del coeficiente CER indexada por fecha del BCRA.

    Returns a DataFrame with the CER coefficient series indexed by date from the BCRA.

    :return: DataFrame 'CER' / DataFrame 'CER'.
    """
    return get_from_api(30, 'CER')

def get_uva() -> pd.DataFrame:
    """
    Devuelve un DataFrame con la serie del coeficiente CER indexada por fecha del BCRA.

    Returns a DataFrame with the CER coefficient series indexed by date from the BCRA.

    :return: DataFrame 'CER' / DataFrame 'CER'.
    """
    return get_from_api(31, 'UVA')

def get_international_reserves(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """
    Devuelve un DataFrame con la serie de las reservas internacionales brutas del BCRA, indexado por fecha.

    Returns a DataFrame with the series of gross international reserves from the BCRA, indexed by date.

    :param date_cod: Define si agregan columnas para código de fecha 'Date' / If True, add columns for date code 'Date'.
    :param api: Define si se utiliza la API del BCRA o un el archivo series.xlsm local / If True, use the BCRA API; otherwise, use the local series.xlsm file.
    :return: DataFrame 'Fecha', 'RRII', 'Date', 'Dia', 'Mes', 'Año' / DataFrame 'Fecha', 'RRII', 'Date', 'Dia', 'Mes', 'Año'.
    """
    if api:
        df = get_from_api(1, 'RRII')
    else:
        df = get_file_bcra('RESERVAS')
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        df = df[df['17'] == 'D'][['1', '3']].copy()
        df.columns = ['Fecha', 'RRII']
        df['Fecha'] = pd.to_datetime(df['Fecha'])
        df = df.set_index('Fecha')

    if date_cod:
        df['Fecha'] = df.index
        return cod.get_date(df)[cod.COLS + ["RRII"]]
    return df

def get_loans(date_cod: bool = False, online: bool = True) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los datos diarios de préstamos del BCRA.

    Returns a DataFrame with daily data on loans from the BCRA.

    :param date_cod: Define si agregan columnas para código de fecha 'Date' / If True, add columns for date code 'Date'.
    :param online: Define si se utiliza la API del BCRA o un el archivo series.xlsm local / If True, use the BCRA API; otherwise, use the local series.xlsm file.
    :return: DataFrame 'Fecha', 'Creditos', 'Date', 'Dia', 'Mes', 'Año' / DataFrame 'Fecha', 'Creditos', 'Date', 'Dia', 'Mes', 'Año'.
    """
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

def get_rates(date_cod: bool = False, api: bool = True, type: int = 0) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los datos diarios de las tasas de interés para los plazos fijos de 30-44 días.

    Returns a DataFrame with daily interest rate data for 30-44 day fixed-term deposits.

    :param date_cod: Define si agregan columnas para código de fecha 'Date' / If True, add columns for date code 'Date'.
    :param api: Define si se utiliza la API del BCRA o un el archivo series.xlsm local / If True, use the BCRA API; otherwise, use the local series.xlsm file.
    :param type: Tipo de tasas a procesar (0: pesos, 1: ambas, 2: dólares) / Type of rates to process (0: pesos, 1: both, 2: dollars).
    :return: DataFrame 'TNA_GenP', 'TNA_100KP', 'TNA_1MP', 'TEM_GenP', 'TEM_100KP', 'TEM_1MP', 'TEA_GenP', 'TEA_100KP', 'TEA_1MP', 'Fecha' (para type=0); similar para type=1 y type=2 / DataFrame 'TNA_GenP', 'TNA_100KP', 'TNA_1MP', 'TEM_GenP', 'TEM_100KP', 'TEM_1MP', 'TEA_GenP', 'TEA_100KP', 'TEA_1MP', 'Fecha' (for type=0); similar for type=1 and type=2.
    """
    columns_pesos = ['TNA_GenP', 'TNA_100KP', 'TNA_1MP', 'Fecha']
    columns_dolares = ['TNA_GenD', 'TNA_100KD', 'TNA_1MD', 'Fecha']

    def get_file() -> pd.DataFrame:
        """
        Devuelve un DataFrame con los datos de tasas de mercado del BCRA.

        Returns a DataFrame with market rate data from the BCRA.

        :return: DataFrame con datos de tasas de mercado / DataFrame with market rate data.
        """
        df = get_file_bcra('TASAS DE MERCADO')
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        return df

    def calculate_rates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """
        Calcula TEM y TEA para un subconjunto de datos de tasas de interés.

        Calculates TEM and TEA for a subset of interest rate data.

        :param df: Subconjunto de DataFrame con datos de tasas de interés / Subset of DataFrame with interest rate data.
        :param columns: Lista de nombres de columnas a procesar / List of column names to process.
        :return: DataFrame con columnas calculadas TEM y TEA / DataFrame with calculated TEM and TEA columns.
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
                tasas_dolares = get_series_api([(132, 'TNA_genD'), (133, 'TNA_100KD'), (134, 'TNA_1MD')]).fillna(
                    0)  # Se pone cero en los NA de 'TNA_1MD'
            else:
                df = get_file()
                tasas_pesos = df[['2', '3', '5', '1']].copy()
                tasas_dolares = df[['6', '7', '8', '1']].copy()
                tasas_dolares['8'] = pd.to_numeric(tasas_dolares['8'], errors='coerce').fillna(
                    0)  # Se pone cero en los NA de 'TNA_1MD'
            df = pd.concat([calculate_rates(tasas_pesos, columns_pesos).drop(columns='Fecha'),
                            calculate_rates(tasas_dolares, columns_dolares)],
                           axis='columns')
        case 2:  # Dollars only
            if api:
                tasas_dolares = get_series_api([(132, 'TNA_genD'), (133, 'TNA_100KD'), (134, 'TNA_1MD')]).fillna(
                    0)  # Se pone cero en los NA de 'TNA_1MD'
            else:
                df = get_file()
                tasas_dolares = df[['6', '7', '8', '1']].copy()
                tasas_dolares['8'] = pd.to_numeric(tasas_dolares['8'], errors='coerce').fillna(
                    0)  # Se pone cero en los NA de 'TNA_1MD'
            df = calculate_rates(tasas_dolares, columns_dolares)

        case _:
            raise ValueError(f"Invalid type_value: {type}. Must be 0, 1, or 2.")
    df = df.drop(columns='Fecha')
    df['Fecha'] = df.index
    if date_cod:
        return cod.get_date(df)
    return df

def get_reference_rates() -> pd.DataFrame:
    return get_series_api([(135, 'TAMAR'), (138, 'BADLAR'), (141, 'TM20')])

def get_tamar(kind: int = 1) -> pd.DataFrame:
    if kind == 1:
        return get_from_api(136, 'TAMAR_PB')
    return pd.DataFrame()

def get_leliqs_rates() -> pd.DataFrame:
    """
    Tasas de interés de LEBAC en Pesos / LELIQ de 1 mes, TNA (en %)
    :return:
    """
    return get_from_api(166, 'leliq_rate')

def get_itcrm(date_cod: bool = False, monthly: bool = False):
    """
    Devuelve un DataFrame con los datos del Índice de Tipo de Cambio Real Multilateral (ITCRM) del BCRA.

    Returns a DataFrame with data from the Multilateral Real Exchange Rate Index (ITCRM) of the BCRA.

    :param date_cod: Define si agregan columnas para código de fecha 'Date' / If True, add columns for date code 'Date'.
    :param monthly: Define si se devuelven promedios mensuales / If True, return monthly averages.
    :return: DataFrame con columnas 'Fecha' y datos del ITCRM, opcionalmente con 'Date', 'Dia', 'Mes', 'Año' / DataFrame with 'Fecha' column and ITCRM data, optionally with 'Date', 'Dia', 'Mes', 'Año'.
    """
    if monthly:
        if date_cod:
            return cod.get_date(get_file_itcrm('ITCRM y bilaterales prom. mens.'))
        return get_file_itcrm('ITCRM y bilaterales prom. mens.').set_index('Fecha')
    else:
        if date_cod:
            return cod.get_date(get_file_itcrm('ITCRM y bilaterales'))
        return get_file_itcrm('ITCRM y bilaterales').set_index('Fecha')

# Tools. It can be OOP one day.

def get_annual_variations(df: pd.DataFrame) -> pd.DataFrame:
    return df.resample('YE').last().pct_change()

def get_usd_deposits(kind: int = 3) -> pd.DataFrame:
    match kind:
        case 1:
            return get_file_bcra_plus(4, [539, 540]).rename(columns={539: "Total_Total", 540: "Total_efectivo"})
        case 2:
            return get_file_bcra_plus(4, [681, 682]).rename(columns={681: "Publico_Total", 682: "Publico_efectivo"})
        case 3:
            return get_file_bcra_plus(4, [821, 822]).rename(columns={821: "Privado_Total", 822: "Privado_efectivo"})
        case _:
            return pd.DataFrame()

def get_interbank_market_data() -> None:
    df = get_series_api([(146, 'Tasa Call Privado'), (147, 'Monto Call Privado'), (148, 'Tasa Call Total'), (149, 'Monto Call Total'), (150, 'Tasa Pases'), (150, 'Tasa Montos')])
    return df


def main() -> None:
    """
    Ejecuta el programa principal para procesar datos del BCRA.

    Runs the main program to process BCRA data.

    :return: None / None.
    """


if __name__ == '__main__':
    main()