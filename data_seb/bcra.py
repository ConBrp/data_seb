import pandas as pd
import numpy as np
import requests
import urllib3
import concurrent.futures
from urllib.parse import urljoin
from bs4 import BeautifulSoup

from . import cod, pbi, ipc

# Suppression of insecure request warnings at module level
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL_BCRA = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/series.xlsm'
URL_BCRA_TCRM = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/ITCRMSerie.xlsx'
URL_BCRA_TC = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/com3500.xls'
URL_OPER = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/Data_operaciones.xlsx'
URL_BCRA_BAL = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din1_ser.txt'
URL_BCRA_RES = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din2_ser.txt'
URL_BCRA_ACT = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din3_ser.txt'
URL_BCRA_PAS = 'https://www.bcra.gob.ar/Pdfs/PublicacionesEstadisticas/din4_ser.txt'
URL_API_MON = 'https://api.bcra.gob.ar/estadisticas/v4.0/monetarias'
URL_BCRA_REPORTS = 'https://www.bcra.gob.ar/informes/'


def _preprocess_excel_bcra(sheet_name: str, filter_col: str, cols_map: dict) -> pd.DataFrame:
    """Internal helper to preprocess BCRA Excel sheets.

    Renames columns to strings '1', '2', ..., filters for daily data ('D'),
    selects/renames target columns, and standardizes the 'Date' column and index.

    Args:
        sheet_name (str): Name of the Excel sheet to process.
        filter_col (str): Column index used to filter for daily data ('D').
        cols_map (dict): Mapping of column indices to target names.

    Returns:
        pd.DataFrame: A preprocessed DataFrame with standardized dates.
    """
    df = get_file_bcra(sheet_name)
    if df is None:
        return pd.DataFrame()
    
    df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
    # Ensure filter_col and date column ('1') are strings for safety
    df = df.loc[df[filter_col] == 'D', list(cols_map.keys())].copy()
    df = df.rename(columns=cols_map)
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.set_index('Date', drop=False)
    return df


def get_file_bcra(sheet_name: str = '', download_file: bool = False) -> pd.DataFrame|None:
    """Retrieves a DataFrame from the specified sheet of the BCRA's series.xlsm file.

    This function can either download the entire Excel file locally or read a specific 
    sheet into a pandas DataFrame.

    Args:
        sheet_name (str, optional): Name of the sheet to retrieve. Defaults to ''.
        download_file (bool, optional): If True, downloads the file and saves it as 
            'series.xlsm' instead of returning a DataFrame. Defaults to False.

    Returns:
        pd.DataFrame | None: A DataFrame containing the session data if download_file 
            is False, or None if the file was downloaded.
    """
    if download_file:
        response = requests.get(URL_BCRA, verify=False)
        with open('series.xlsm', 'wb') as archivo:
            archivo.write(response.content)
        return
    return pd.read_excel(URL_BCRA, header=[0, 1, 2, 3, 4, 5, 6, 7, 8], sheet_name=sheet_name)

def get_file_tc_oficial() -> pd.DataFrame:
    """Retrieves the official exchange rate (A3500) from the BCRA's com3500.xls file.

    Returns:
        pd.DataFrame: A DataFrame with columns 'Date' and 'TC_A3500', indexed by Date.
    """
    df = pd.read_excel(URL_BCRA_TC, header=3).dropna(axis='columns')
    df.columns = ['Date', 'TC_A3500']
    df.index = pd.to_datetime(df['Date'])
    return df

def get_file_itcrm(sheet_name: str) -> pd.DataFrame:
    """Retrieves Multilateral Real Exchange Rate Index (ITCRM) data from the BCRA.

    Args:
        sheet_name (str): Name of the sheet to process (e.g., daily or monthly average).

    Returns:
        pd.DataFrame: A DataFrame with columns 'Date' and ITCRM values.
    """
    df = pd.read_excel(URL_BCRA_TCRM, sheet_name=sheet_name, header=[1]).dropna().rename(columns={'Período': 'Date'})
    df['Date'] = pd.to_datetime(df['Date'])
    return df

def get_file_bcra_plus(file: int, serie: list[int], div: bool = True) -> pd.DataFrame:
    """Retrieves BCRA data from a specific text-based file.

    This function downloads and processes time-series data from various BCRA 
    endpoints (BAL, RES, ACT, PAS).

    Args:
        file (int): File identifier (1: BAL, 2: RES, 3: ACT, 4: PAS).
        serie (list[int]): List of series codes to retrieve from the file.
        div (bool, optional): If True, divides the values by 1000. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame with the selected series and a 'Date' column.
    """

    def download(link: str, dividir: bool) -> pd.DataFrame:
        """Downloads and preprocesses BCRA data from a given URL.

        Args:
            link (str): URL of the file to download.
            dividir (bool): If True, divide the values by 1000.

        Returns:
            pd.DataFrame: Preprocessed DataFrame with a 'Date' column.
        """
        df = pd.read_csv(link, sep=';', names=['Cat', 'Fecha', 'Monto'],
                         dtype={'Cat': str, 'Fecha': str, 'Monto': float}).dropna()
        df['Cat'] = pd.to_numeric(df.loc[:, 'Cat'])
        df['Date'] = pd.to_datetime(df.loc[:, 'Fecha'], format='%d/%m/%Y')
        df['Monto'] = pd.to_numeric(df.loc[:, 'Monto'])
        data = df.pivot_table(columns='Cat', values='Monto', index='Date')
        data.columns.name = None
        data.index.name = None
        if dividir:
            data = data / 1000
        data['Date'] = data.index
        return data

    links = {1: URL_BCRA_BAL, 2: URL_BCRA_RES, 3: URL_BCRA_ACT, 4: URL_BCRA_PAS}
    
    if file in links:
        return download(links[file], div)[['Date'] + serie].copy()
    return pd.DataFrame()

def get_principales_variables() -> pd.DataFrame:
    """Retrieves the main variables accessible via the BCRA API.

    Returns:
        pd.DataFrame: A DataFrame containing the results of 'Principales Variables'.
    """
    response = requests.get(URL_API_MON, verify=False)
    return pd.DataFrame(response.json()['results'])

def get_from_api(idvariable: int, nombre: str) -> pd.DataFrame:
    """Retrieves historical data for a specific variable from the BCRA API.

    The function handles pagination, fetching data in chunks of 3000 results until 
    the full history is retrieved.

    Args:
        idvariable (int): ID of the variable to retrieve.
        nombre (str): Name to assign to the variable column in the DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with the variable data, indexed by 'Date'.
    """
    response = requests.get(f'{URL_API_MON}/{idvariable}?limit=3000', verify=False)
    df = pd.DataFrame(response.json().get('results')[0].get('detalle')).set_index('fecha', drop=True)
    while response.json().get('results')[0].get('detalle'):
        desde = pd.to_datetime(df.index[-1]) - pd.offsets.DateOffset(3000)
        hasta = pd.to_datetime(df.index[-1]) - pd.offsets.DateOffset()
        response = requests.get(f'{URL_API_MON}/{idvariable}?desde={desde}&hasta={hasta}&limit=3000', verify=False)
        if response.json().get('results')[0].get('detalle'):
            df = pd.concat(
                [df,
                 pd.DataFrame(response.json().get('results')[0].get('detalle')).set_index('fecha', drop=True)])
    df.index = pd.to_datetime(df.index)
    df.columns = [nombre]
    df.index.name = 'Date'
    return df.sort_index()

def get_series_api(arguments: list[tuple], date: bool = False) -> pd.DataFrame:
    """Retrieves multiple variables from the BCRA API using concurrent requests.

    Args:
        arguments (list[tuple]): List of tuples with (idvariable, nombre) 
            for each variable to retrieve.
        date (bool, optional): If True, adds an explicit 'Date' column. 
            Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with columns for each variable, indexed by Date.
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
    """Retrieves daily data on fixed-term deposits from the BCRA.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date' 
            (Month-Year). Defaults to False.
        api (bool, optional): If True, uses the BCRA API; otherwise, uses 
            the local series.xlsm file. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame with columns 'PF', 'PF_UVA', 'PF_Privado', 
            'PF_UVA_Privado', and 'Date'.
    """
    if api:
        arguments = [(87, 'PF'), (88, 'PF_UVA'), (96, 'PF_Privado'), (97, 'PF_UVA_Privado')]
        df = get_series_api(arguments)
        df['Date'] = df.index
    else:
        df = _preprocess_excel_bcra('DEPOSITOS', '30', {'1': 'Date', '4': 'PF_total', '5': 'PF_UVA_total', '13': 'PF_privado', '14': 'PF_UVA_privado'})
    if date_cod:
        return cod.get_date(df)
    return df

def get_current_account_bcra() -> pd.DataFrame:
    return get_from_api(70, 'CA_BCRA')

def get_monetary_base(date_cod: bool = False, api: bool = True, q: bool = False,
                       only_bmt: bool = False) -> pd.DataFrame:
    """Retrieves daily data on monetary base components from the BCRA.

    'BMT': Total monetary base = 'DPP' + 'DPB' + 'CCBCRA' + 'CC'.
    'CM': Monetary circulation = 'DPP' + 'DPB' + 'CC'.
    'DPP': Notes and coins held by the public.
    'DPB': Notes and coins in entities (money held by banks).
    'CCBCRA': Current account at the BCRA (of banks).
    'CC': Cancelled checks.
    'QM': Quasi-money.
    'BMTQ': 'BMT' + 'QM'.
    'DT': Total money = 'DPP' + 'DPB'.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        api (bool, optional): If True, uses the BCRA API; otherwise, uses 
            the local series.xlsm file. Defaults to True.
        q (bool, optional): If True, includes quasi-money ('QM') and 'BMTQ'. 
            Defaults to False.
        only_bmt (bool, optional): If True, returns only the total monetary 
            base ('BMT'). Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame containing some or all of: 'Date', 'BMT', 'CM', 
            'DPP', 'DPB', 'CCBCRA', 'CC', 'QM', 'BMTQ', 'DT'.
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
        df = _preprocess_excel_bcra('BASE MONETARIA', '33', {'1': 'Date', '26': 'DPP', '27': 'DPB', '28': 'CC', '29': 'CCBCRA', '30': 'BMT', '31': 'QM', '32': 'BMTQ'})
        df['CM'] = df['DPP'] + df['DPB'] + df['CC']
    if not only_bmt:
        df['DT'] = df['DPP'] + df['DPB']
    if date_cod:
        df['Date'] = df.index
        return cod.get_date(df)[cod.COLS + columns].dropna().sort_index().copy()
    
    df['Date'] = df.index
    return df.dropna()

def  get_m2(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """Retrieves daily data of the M2 monetary aggregate from the BCRA.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        api (bool, optional): If True, uses the BCRA API. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame with the 'M2' series and 'Date'.
    """
    if api:
        if date_cod:
            df = get_from_api(109, 'M2')
            df['Date'] = df.index
            return cod.get_date(df)
        return get_from_api(109, 'M2')
    else:
        return pd.DataFrame()

def get_lefis(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """Retrieves daily data on LEFI (Letras Fiscales de Liquidez) stock and flow.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        api (bool, optional): If True, uses the BCRA API; otherwise, uses 
            the URL_OPER Excel file. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame with 'LEFI', 'LEFI_Flujo', and 'Date'.
    """
    if api:
        df = get_series_api([(196, 'LEFI'), (58, 'LEFI_Flujo')])
    else:
        df = pd.read_excel(URL_OPER, header=[0, 1, 2])
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        df = df.dropna(subset=['1', '2'])
        df = df.loc[:, ['1', '3', '4', '5', '6']].copy()
        df.columns = ['Date', 'VT', 'Publico', 'Privado', 'BCRA']
        df['LEFI'] = df['Publico'] + df['Privado']
        df['Date'] = pd.to_datetime(df['Date'])
        df = df.set_index('Date', drop=False)
    if date_cod:
        return cod.get_date(df)[cod.COLS + ['LEFI', 'LEFI_Flujo']].dropna()
    else:
        return df.dropna()

def get_lebacs(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """Retrieves the balance of LEBACs and NOBACs from the BCRA.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        api (bool, optional): If True, uses the BCRA API. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame with the 'LEBACs' series.
    """
    return get_from_api(156, 'LEBACs')

def get_leliqs() -> pd.DataFrame:
    """Retrieves the balance of LELIQ and NOTALIQ from the BCRA.

    Returns:
        pd.DataFrame: A DataFrame with the 'LELIQs' series.
    """
    return get_from_api(155, 'LELIQs')

def get_repo() -> pd.DataFrame:
    """Retrieves active repos (Pases activos) from the BCRA.

    Returns:
        pd.DataFrame: A DataFrame with the 'REPOs' series.
    """
    return get_from_api(154, 'REPOs')

def get_reverse_repo() -> pd.DataFrame:
    """Retrieves reverse repos (Pases pasivos) from the BCRA.

    Returns:
        pd.DataFrame: A DataFrame with the 'REVERSE_REPO' series.
    """
    return get_from_api(152, 'REVERSE_REPO')

def get_monetary_instruments(date_cod: bool = False, api: bool = True) -> pd.DataFrame:
    """Retrieves daily data of various BCRA monetary instruments.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        api (bool, optional): If True, uses the BCRA API; otherwise, uses 
            the local series.xlsm file. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame with columns 'Date', 'Pases_Pasivos', 
            'Pases_Pasivos_FCI', 'Pases_Activos', 'LELIQs', 'LEBACs', 
            'LEBACsD_LEVID_BOPREAL', and 'NOCOMs'.
    """
    if api:  # Los pases pasivos tienen un día menos de datos con la API.
        df = get_series_api([(152, 'Pases_Pasivos'), (154, 'Pases_Activos'), (153, 'Pases_Pasivos_FCI'), (156, 'LEBACs'),
                             (155, 'LELIQs'), (158, 'LEBACsD_LEVID_BOPREAL'), (159, 'NOCOMs'), (198, 'Otros_Pases')])
    else:
        df = _preprocess_excel_bcra('INSTRUMENTOS DEL BCRA', '1', {'1': 'Date', '2': 'Pases_Pasivos', '3': 'Pases_Pasivos_FCI', '4': 'Pases_Activos', '5': 'LELIQs', '6': 'LEBACs', '8': 'LEBACsD_LEVID_BOPREAL', '9': 'NOCOMs'})
        df[df.columns[1:]] = df.loc[:, 'Pases_Pasivos': 'NOCOMs'].apply(pd.to_numeric, errors='coerce').fillna(0)

    if date_cod:
        return cod.get_date(df)[cod.COLS + ['Pases_Pasivos', 'Pases_Pasivos_FCI', 'Pases_Activos', 'LELIQs', 'LEBACs',
                                            'LEBACsD_LEVID_BOPREAL', 'NOCOMs', 'Otros_Pases']]
    return df

def get_government_deposits(date_cod: bool = False, kind: str = 'ARS') -> pd.DataFrame:
    """Retrieves daily data on government deposits from the BCRA.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        kind (str, optional): Type of deposits to retrieve ('ARS', 'USD', 'BOTH'). 
            Defaults to 'ARS'.

    Returns:
        pd.DataFrame: A DataFrame with government deposit columns and 'Date'.
    """
    match kind:
        case 'ARS':
            df = get_file_bcra_plus(2, [8842])
            df = df.rename(columns={8842: 'Gob_Dep_ARS'})
        case 'USD':
            df = pd.concat([get_file_bcra_plus(2, [8843]), get_file_bcra_plus(2, [271], div=False).drop(columns='Date')], axis=1)
            df = df.rename(columns={8843: 'Gob_Dep_USD_in_ARS', 271: 'ER'})
            df['Gob_Dep_USD'] = df['Gob_Dep_USD_in_ARS'] / df['ER']
        case 'BOTH':
            df = pd.concat([get_file_bcra_plus(2, [8842]),
                            get_file_bcra_plus(2, [8843]).drop(columns='Date'),
                            get_file_bcra_plus(2, [271], div=False).drop(columns='Date')], axis=1)
            df = df.rename(columns={8842: 'Gob_Dep_ARS', 8843: 'Gob_Dep_USD_in_ARS', 271: 'ER'})
            df['Gob_Dep_USD'] = df['Gob_Dep_USD_in_ARS'] / df['ER']
        case _:
            df = None
    if date_cod:
        return cod.get_date(df).dropna()
    else:
        return df.dropna()

def get_accounting_exchange_rate() -> pd.DataFrame:
    """Retrieves the accounting exchange rate from the BCRA API.

    Returns:
        pd.DataFrame: A DataFrame with the 'A_Exchange_Rate' column, indexed by Date.
    """
    return get_from_api(84, 'A_Exchange_Rate')

def get_official_exchange_rate(date_cod: bool = False, api: bool = True, mensual: bool = False) -> pd.DataFrame:
    """Retrieves the official exchange rate (A3500) values from the BCRA.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        api (bool, optional): If True, uses the BCRA API; otherwise, uses 
            the local com3500.xls file. Defaults to True.
        mensual (bool, optional): If True, returns monthly averages. 
            Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with 'A3500_ER' and 'Date'.
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
        df['Date'] = df.index
        return cod.get_date(df)[cod.COLS + ['A3500_ER']]
    return df.dropna()

def get_retail_exchange_rate() -> pd.DataFrame:
    """Retrieves the retail exchange rate from the BCRA API.

    Returns:
        pd.DataFrame: A DataFrame with the 'R_exchange_rate' column, indexed by Date.
    """
    return get_from_api(4, 'R_exchange_rate')

def get_cer() -> pd.DataFrame:
    """Retrieves the CER (Coeficiente de Estabilización de Referencia) index.

    Returns:
        pd.DataFrame: A DataFrame with the 'CER' column, indexed by Date.
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
    """Retrieves daily data on BCRA international reserves (RRII).

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        api (bool, optional): If True, uses the BCRA API; otherwise, uses 
            the local series.xlsm file. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame with the 'RRII' series and 'Date'.
    """
    if api:
        df = get_from_api(1, 'RRII')
    else:
        df = _preprocess_excel_bcra('RESERVAS', '17', {'1': 'Date', '3': 'RRII'})

    if date_cod:
        df['Date'] = df.index
        return cod.get_date(df)[cod.COLS + ["RRII"]]
    return df.rename(columns={'Fecha': 'Date'}) if 'Fecha' in df.columns else df

def get_loans(date_cod: bool = False, online: bool = True) -> pd.DataFrame:
    """Retrieves daily data of BCRA loans.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        online (bool, optional): If True, uses the BCRA API; otherwise, uses 
            the local series.xlsm file. Defaults to True.

    Returns:
        pd.DataFrame: A DataFrame with the 'Creditos' series and 'Date'.
    """
    if online:
        df = get_from_api(117, 'Préstamos')
    else:
        df = _preprocess_excel_bcra("PRESTAMOS", "22", {"1": "Date", "9": "Creditos"})
    if date_cod:
        return cod.get_date(df)
    else:
        return df

def get_rates(date_cod: bool = False, api: bool = True, type: int = 0) -> pd.DataFrame:
    """Retrieves various BCRA interest rates.

    This function fetches interest rates for fixed terms, LELIQs, and repos, 
    calculating Monthly Effective (TEM) and Annual Effective (TEA) rates.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        api (bool, optional): If True, uses the BCRA API; otherwise, uses 
            the local series.xlsm file. Defaults to True.
        type (int, optional): Type of rates to process (0: pesos, 1: both, 2: dollars). 
            Defaults to 0.

    Returns:
        pd.DataFrame: A DataFrame with various interest rate series and their TEM/TEA equivalents.
    """
    columns_pesos = ['TNA_GenP', 'TNA_100KP', 'TNA_1MP', 'Date']
    columns_dolares = ['TNA_GenD', 'TNA_100KD', 'TNA_1MD', 'Date']

    def get_file() -> pd.DataFrame:
        """Retrieves market rate data from the BCRA's Excel file.

        Returns:
            pd.DataFrame: A DataFrame with market rate data.
        """
        df = get_file_bcra('TASAS DE MERCADO')
        df.columns = [str(i) for i in range(1, len(df.columns) + 1)]
        return df

    def calculate_rates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """Calculates TEM and TEA for a given set of interest rates.

        Args:
            df (pd.DataFrame): DataFrame containing interest rate data.
            columns (list[str]): List of column names to assign.

        Returns:
            pd.DataFrame: A DataFrame with additional TEM and TEA columns.
        """
        df.columns = columns

        for tasa in columns[:-1]:  # Skip 'Fecha'
            df[tasa] = df[tasa] / 100

            tem_col = f'TEM_{tasa.split("_")[1]}'
            df[tem_col] = df[tasa] / 12

            tea_col = f'TEA_{tasa.split("_")[1]}'
            df[tea_col] = (1 + df[tem_col]) ** 12 - 1
        df.index = df['Date']
        return df

    # Process based on type_value
    match type:
        case 0:  # Pesos only
            if api:
                tasas_pesos = get_series_api([(128, 'TNA_genP'), (129, 'TNA_100KP'), (131, 'TNA_1MP')], date=True)
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
            df = pd.concat([calculate_rates(tasas_pesos, columns_pesos).drop(columns='Date'),
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
    df = df.drop(columns='Date')
    df['Date'] = df.index
    if date_cod:
        return cod.get_date(df)
    return df

def get_reference_rates() -> pd.DataFrame:
    """Retrieves standard reference rates (TAMAR, BADLAR, TM20) from the BCRA API.

    Returns:
        pd.DataFrame: A DataFrame with the reference rate series.
    """
    return get_series_api([(135, 'TAMAR'), (138, 'BADLAR'), (141, 'TM20')])

def get_tamar(kind: int = 1) -> pd.DataFrame:
    """Retrieves the TAMAR_PB (Tasa de Mercado de Arreglo Repos) rate.

    Args:
        kind (int, optional): Identifier for the TAMAR type. Defaults to 1.

    Returns:
        pd.DataFrame: A DataFrame with the 'TAMAR_PB' series.
    """

def get_leliqs_rates() -> pd.DataFrame:
    """Retrieves the interest rates for LELIQs (1 month, TNA in %).

    Returns:
        pd.DataFrame: A DataFrame with the 'leliq_rate' series.
    """
    return get_from_api(166, 'leliq_rate')

def get_itcrm(date_cod: bool = False, monthly: bool = False):
    """Retrieves Multilateral Real Exchange Rate Index (ITCRM) data from the BCRA.

    Args:
        date_cod (bool, optional): If True, adds columns for date code 'Date'. 
            Defaults to False.
        monthly (bool, optional): If True, returns monthly average indexes. 
            Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with ITCRM data indexed by Date.
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
    """Calculates year-on-year variations for a given DataFrame.

    Args:
        df (pd.DataFrame): Time-series DataFrame.

    Returns:
        pd.DataFrame: A DataFrame containing the annual percentage changes.
    """
    return df.resample('YE').last().pct_change()

def _get_latest_rem_url() -> str:
    """Internal helper to discover the latest REM report Excel URL.

    Scrapes the BCRA reports page to find the most recent REM report 
    and extracts the download link for the Excel result tables.

    Returns:
        str: The absolute URL of the latest REM Excel file.
    """
    try:
        base_url = "https://www.bcra.gob.ar"
        response = requests.get(URL_BCRA_REPORTS, verify=False)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        rem_page_url = None
        for a in soup.find_all('a', href=True):
            if "Relevamiento de Expectativas de Mercado (REM)" in a.get_text():
                rem_page_url = urljoin(base_url, a['href'])
                break
                
        if not rem_page_url:
            return None
            
        response = requests.get(rem_page_url, verify=False)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for a in soup.find_all('a', href=True):
            href = a['href']
            if "tablas-relevamiento-expectativas-mercado" in href and href.endswith(".xlsx"):
                return urljoin(base_url, href)
    except Exception:
        return None
    return None

def get_inflation_expectations(url: str = None) -> pd.DataFrame:
    """Retrieves inflation expectations from the BCRA REM report.

    Args:
        url (str, optional): URL of the BCRA REM report Excel file. 
            If None, attempts to discover the latest available URL.

    Returns:
        pd.DataFrame: A DataFrame with 'Date' and 'Expected_Inflation' 
            (median) columns.
    """
    if url is None:
        url = _get_latest_rem_url()
    
    if url is None:
        # Fallback to the last known manual URL if discovery fails
        url = 'https://www.bcra.gob.ar/archivos/Pdfs/PublicacionesEstadisticas/informes/tablas-relevamiento-expectativas-mercado-feb-2026.xlsx'
    
    df = pd.read_excel(url, sheet_name='Cuadros de resultados', header=None)
    
    estimations = []
    # Row 6 to 12 in 0-indexed DataFrame has the monthly estimates
    for i in range(6, 13):
        date_val = df.iloc[i, 1]
        infla_pct = df.iloc[i, 3]  # Mediana column (index 3)
        if pd.notna(date_val) and pd.notna(infla_pct):
            estimations.append({
                'Date': pd.to_datetime(date_val),
                'Expected_Inflation': float(infla_pct) / 100.0
            })
            
    return pd.DataFrame(estimations)

def get_usd_deposits(kind: int = 3) -> pd.DataFrame:
    """Retrieves daily data of USD deposits in the financial system.

    Args:
        kind (int, optional): Type of deposits to retrieve (1: Total, 2: Public, 3: Private). 
            Defaults to 3.

    Returns:
        pd.DataFrame: A DataFrame with USD deposit columns and Date.
    """
    match kind:
        case 1:
            return get_file_bcra_plus(4, [539, 540]).rename(columns={539: "Total_Total", 540: "Total_efectivo"})
        case 2:
            return get_file_bcra_plus(4, [681, 682]).rename(columns={681: "Publico_Total", 682: "Publico_efectivo"})
        case 3:
            return get_file_bcra_plus(4, [821, 822]).rename(columns={821: "Privado_Total", 822: "Privado_efectivo"})
        case _:
            return pd.DataFrame()

def get_interbank_market_data() -> pd.DataFrame:
    """Retrieves various interbank market data (Call and Pases) from the BCRA API.

    Returns:
        pd.DataFrame: A DataFrame with columns for Tasa/Monto Call and Pases.
    """
    df = get_series_api([(146, 'Tasa Call Privado'), (147, 'Monto Call Privado'), (148, 'Tasa Call Total'), (149, 'Monto Call Total'), (150, 'Tasa Pases'), (151, 'Monto Pases')])
    return df

def get_money_demand(config: dict, real: bool = True, estimado: bool = False, monthly_mean: bool = False) -> pd.DataFrame:
    """Retrieves and calculates money demand metrics from the BCRA.

    Args:
        config (dict): Configuration dictionary containing script and file paths.
        real (bool, optional): If True, calculates demand in real terms. 
            Defaults to True.
        estimado (bool, optional): If True, uses estimated PBI data. 
            Defaults to False.
        monthly_mean (bool, optional): If True, returns monthly average demand. 
            Defaults to False.

    Returns:
        pd.DataFrame: A DataFrame with money demand metrics.
    """
    def tratar_pbi(pib: pd.DataFrame) -> pd.DataFrame:
        """Processes and merges PBI data with monetary aggregates to calculate demand.

        Args:
            pib (pd.DataFrame): PBI data DataFrame.

        Returns:
            pd.DataFrame: A DataFrame with demand calculations.
        """
        pib['year'] = pib.index.year
        pib['quarter'] = pib.index.quarter
        pib['var'] = pib['PBI'].pct_change()
        
        # Pulling monetary data once
        base = get_monetary_base()
        m2 = get_m2(date_cod=True).drop(columns=['month', 'Date_Cod', 'day']).copy()
        dinero = pd.concat([base, m2], axis='columns').dropna().copy()
        dinero['quarter'] = dinero.index.quarter
        pib['Code'] = pib['year'].astype(int).astype(str) + '-' + pib['quarter'].astype(int).astype(str)
        dinero['Code'] = dinero['year'].astype(int).astype(str) + '-' + dinero['quarter'].astype(int).astype(str)
        final = pd.merge(pib, dinero.drop(columns=['year', 'quarter']).copy(), on='Code', how='inner').replace({np.nan: 0})
        final['Cant_D'] = final['Date'].apply(pbi.days_in_quarter)
        final['Dia'] = final['Date'].apply(pbi.days_in_quarter, args=(False,))
        final['PBI_Ajustado'] = final['PBI'] / (1 + final['var'])
        final['PBI_Ajustado'] = final['PBI_Ajustado'] * (1 + final['var']) ** (final['Dia'] / final['Cant_D'])
        # Vectorized demand calculation
        demand_cols = ['BMT', 'CM', 'DPP', 'DPB', 'CCBCRA', 'DT', 'M2']
        for col in demand_cols:
            if col in final.columns:
                final[f'Demanda_{col}'] = final[col] / final['PBI'] * 100
                final[f'Demanda_{col}_A'] = final[col] / final['PBI_Ajustado'] * 100

        return final

    if real:
        dd = get_monetary_base(date_cod=True)
        ipc_index = ipc.get_ipc(config.get('ipc_script').get('FILE_INFLA_EMPALMADA'))
        
        m2 = get_m2()[['M2']].copy()
        
        # Reset index so 'Date' is not ambiguous (index and column)
        m2['Date'] = m2.index
        m2 = m2.reset_index(drop=True)
        dd_nom = pd.merge(dd, m2, on='Date')
        
        if monthly_mean:
            dd_nom = dd_nom.set_index('Date')
            monthly_nom = dd_nom[['DPP', 'DT', 'BMT', 'M2']].resample('ME').mean()
            monthly_nom['Date'] = monthly_nom.index
            monthly_nom = cod.get_date(monthly_nom)
            ddreal = pd.merge(monthly_nom, ipc_index[['Date_Cod', 'IPC']], on='Date_Cod', how='inner')
            # The capitalization factor is inversely proportional to IPC, normalized to the last month
            ddreal['Capitalizador'] = 1 / ddreal['IPC']
            ddreal['Capitalizador'] = ddreal['Capitalizador'] / ddreal['Capitalizador'].iloc[-1]
            
            for col in ['DPP', 'DT', 'BMT', 'M2']:
                ddreal[f'{col}_real'] = ddreal[col] * ddreal['Capitalizador']
            return ddreal[['Date', 'DPP_real', 'DT_real', 'BMT_real', 'M2_real']].copy()
        else:
            ipc_index = ipc_index.drop(columns='Date')
            ddreal = pd.merge(dd_nom, ipc_index, on='Date_Cod')
            
            ddreal = ipc.get_act_cap(ddreal)
            ddreal['DPP_real'] = ddreal['DPP'] * ddreal['Capitalizador']
            ddreal['DT_real'] = ddreal['DT'] * ddreal['Capitalizador']
            ddreal['BMT_real'] = ddreal['BMT'] * ddreal['Capitalizador']
            ddreal['M2_real'] = ddreal['M2'] * ddreal['Capitalizador']
            return ddreal[['Date', 'DPP_real', 'DT_real', 'BMT_real', 'M2_real']].copy()
    elif not estimado:
        pib = pbi.get_pbi_pcorrientes(config.get('pbi_script').get('URL_INDEC_PBI'))
        final = tratar_pbi(pib)
        final.to_excel('final.xlsx', index=False)
        return final.set_index('Date', drop=False)[
            ['Date', 'Demanda_BMT', 'Demanda_CM', 'Demanda_DPP', 'Demanda_DPB', 'Demanda_CCBCRA', 'Demanda_DT',
             'Demanda_M2', 'Demanda_BMT_A', 'Demanda_CM_A', 'Demanda_DPP_A', 'Demanda_DPB_A', 'Demanda_CCBCRA_A',
             'Demanda_DT_A', 'Demanda_M2_A']].copy()
    else:
        pib = pbi.get_pbi_pcorrientes(config.get('pbi_script').get('URL_INDEC_PBI'), config.get('ipc_script').get('FILE_INFLA_EMPALMADA'), sin_estimar=False)
        final = tratar_pbi(pib)
        final.to_excel('final_E.xlsx', index=False)
        return final.set_index('Date', drop=False)[
            ['Date', 'Demanda_BMT', 'Demanda_CM', 'Demanda_DPP', 'Demanda_DPB', 'Demanda_CCBCRA', 'Demanda_DT',
             'Demanda_M2', 'Demanda_BMT_A', 'Demanda_CM_A', 'Demanda_DPP_A', 'Demanda_DPB_A', 'Demanda_CCBCRA_A',
             'Demanda_DT_A', 'Demanda_M2_A']].copy()

def main() -> None:
    """Main execution function for processing BCRA data.

    Returns:
        None
    """


if __name__ == '__main__':
    main()
