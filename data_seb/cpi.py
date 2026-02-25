import pandas as pd
import requests
import print_calendar
import json
from datetime import datetime
from . import cod


def _fetch_bls_data(series_ids: list, start_year: int|str, end_year: int|str, registration_key: str) -> dict | None:
    """
    Obtiene datos de series temporales desde la API del BLS.

    Fetches timeseries data from the BLS API.

    :param series_ids: Lista de IDs de series del BLS / List of BLS series IDs.
    :param start_year: Año de inicio (entero o cadena) / Start year (integer or string).
    :param end_year: Año final (entero o cadena) / End year (integer or string).
    :param registration_key: Clave de registro para la API del BLS / Registration key for the BLS API.
    :return: Diccionario con los datos en formato JSON, o None si falla /
             Dictionary with the data in JSON format, or None if it fails.
    """
    url = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
    headers = {'Content-Type': 'application/json'}
    payload = {
        "seriesid": series_ids,
        "startyear": str(start_year),
        "endyear": str(end_year),
        "catalog": True,
        "calculations": True,
        "annualaverage": True,
        "aspects": True,
        "registrationkey": registration_key
    }

    try:
        response = requests.post(url, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        data = response.json()
        return data

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        return None
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
        return None
    except ValueError as json_err:
        print(f"Error parsing JSON response: {json_err}")
        return None


def get_cpi(file_cpi1913: str = '', api: bool = False, api_key: str = '', witch: int = 1) -> pd.DataFrame | dict[pd.DataFrame]:
    """
    Devuelve un DataFrame con el IPC del archivo CPI1913.xlsx, con el CPI base 1967.

    Returns a DataFrame with the CPI from the CPI1913.xlsx file, with the 1967 base CPI.

    :param file_cpi1913: Ruta al archivo CPI1913.xlsx / Path to the CPI1913.xlsx file.
    :param api: Si busca los datos en la API del BLS o del archivo local /
                If True, fetches data from the BLS API; if False, loads from the file.
    :param api_key: Clave de la API del BLS / API key of the BLS API.
    :param witch: Qué base de IPC obtener (0: ambas, 1: 1982-84=100, 2: 1967=100) /
                  Which CPI base to fetch (0: both, 1: 1982-84=100, 2: 1967=100).
    :return: DataFrame o diccionario de DataFrames con los valores del 'CPI' indizados por la fecha /
             DataFrame or dict of DataFrames with 'CPI' values indexed by date.
    """
    if api:
        match witch:
            case 0:
                series_ids = ['CUUR0000SA0', 'CUUR0000AA0']  # 1982-84=100 & 1967=100
            case 1:
                series_ids = ['CUUR0000SA0']  # 1982-84=100
            case 2:
                series_ids = ['CUUR0000AA0']  # 1967=100
            case _:
                series_ids = None
        if series_ids is None:
            raise ValueError("Invalid value for 'witch'. Must be 0, 1, or 2.")

        cpi_us_dfs = {series_id: pd.DataFrame() for series_id in series_ids}
        latest_bool = {series_id: False for series_id in series_ids}
        inicio = 1900
        final = datetime.today().year
        while True:
            data = _fetch_bls_data(series_ids, inicio, final, api_key)
            for serie in data.get('Results').get('series'):
                df = pd.DataFrame(serie.get('data'))
                if 'latest' in df.columns:
                    cpi_us_dfs[serie.get('seriesID')] = pd.concat([cpi_us_dfs[serie.get('seriesID')],
                                                                   df.sort_values(by='year')[
                                                                       ['year', 'period', 'value', 'latest']]],
                                                                  ignore_index=True)
                else:
                    cpi_us_dfs[serie.get('seriesID')] = pd.concat(
                        [cpi_us_dfs[serie.get('seriesID')], df.sort_values(by='year')[['year', 'period', 'value']]],
                        ignore_index=True)
                if 'latest' in cpi_us_dfs[serie.get('seriesID')].columns and any(cpi_us_dfs[serie.get('seriesID')]['latest'] == 'true'):
                    latest_bool[serie.get('seriesID')] = True
            if all(latest_bool.values()):
                break
            inicio += 20
        for serie_id in series_ids:
            cpi_us_dfs[serie_id] = cpi_us_dfs[serie_id].query("period != 'M13'").copy()
            cpi_us_dfs[serie_id]['month'] = cpi_us_dfs[serie_id]['period'].str.replace('M', '')
            cpi_us_dfs[serie_id]['date'] = pd.to_datetime(
                cpi_us_dfs[serie_id]['year'].astype(str) + '-' + cpi_us_dfs[serie_id]['month'].astype(str) + '-01')
            cpi_us_dfs[serie_id] = cpi_us_dfs[serie_id][['date', 'value']].copy()
            cpi_us_dfs[serie_id]['date'] = cpi_us_dfs[serie_id]['date'] + pd.offsets.MonthEnd(0)
            cpi_us_dfs[serie_id]['value'] = pd.to_numeric(cpi_us_dfs[serie_id]['value'])
            cpi_us_dfs[serie_id].columns = ['Date', 'CPI']
            cpi_us_dfs[serie_id] = cpi_us_dfs[serie_id].set_index('Date').sort_index()
        if witch in (1, 2):
            return cpi_us_dfs[series_ids[0]]
        return cpi_us_dfs
    else:
        cpi = pd.read_excel(file_cpi1913, parse_dates=['Date'])[['Date', 'CPI']].copy()
        # cpi['Fecha'] = pd.to_datetime(cpi['Fecha'], format='%Y-%m-%d')
        cpi['InflaMensual'] = cpi['CPI'].pct_change()
        cpi = cod.get_date(cpi, day=False)
        cpi['CantD'] = cpi.apply(lambda row: calendar.monthrange(row['year'], row['month'])[1], axis=1)
        # TODO set index date.
        return cpi[cod.COLS[:-1] + ['CPI', 'InflaMensual', 'CantD']].copy()

def get_act_cap(df: pd.DataFrame, us: bool = False) -> pd.DataFrame:
    """
    Devuelve un DataFrame con datos diarios, para actualizar y capitalizar valores.

    Returns a DataFrame with daily data for updating and capitalizing values.

    :param df: DataFrame con datos de 'CPI', 'InflaMensual', 'Dia', 'CantD' / DataFrame with 'CPI', 'InflaMensual', 'Dia', 'CantD' data.
    :param us: Si agregar 'us' al final de las columnas del DataFrame o no / If True, append 'us' to the DataFrame column names.
    :return: DataFrame 'Actualizador', 'Capitalizador' / DataFrame 'Actualizador', 'Capitalizador'.
    """
    infla_column = 'InflaMensual_US' if us else 'InflaMensual'
    df['CPI'] = df['CPI'] / (1 + df[infla_column])
    df['CPI'] = df['CPI'] / df['CPI'].iloc[0]

    actualizador_column = 'ActualizadorUS' if us else 'Actualizador'
    capitalizador_column = 'CapitalizadorUS' if us else 'Capitalizador'

    df[actualizador_column] = df['CPI'] * (1 + df[infla_column]) ** (df['day'] / df['CantD'])
    df[capitalizador_column] = 1 / df[actualizador_column]
    df[capitalizador_column] = df[capitalizador_column] / df[capitalizador_column].iloc[-1]

    return df


def main() -> None:
    """
    Ejecuta el programa principal para procesar datos del IPC.

    Runs the main program to process CPI data.

    :return: None / None.
    """
    print(f'Se corrió el main de {__name__}')


if __name__ == '__main__':
    main()
