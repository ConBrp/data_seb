import pandas as pd
import calendar
import json
from datetime import datetime
# import print_calendar

from . import cod

URL_INDEC_DIVISIONES = 'https://www.indec.gob.ar/ftp/cuadros/economia/serie_ipc_divisiones.csv'
URL_INDEC_APERTURAS = 'https://www.indec.gob.ar/ftp/cuadros/economia/serie_ipc_aperturas.csv'
URL_INDEC_PONDERADORES = 'https://www.indec.gob.ar/ftp/cuadros/economia/ponderadores_ipc.xls'


def get_file_indec(tipo: int = 1) -> pd.DataFrame | None:
    """Fetch division or opening data from INDEC CSV files.

    Args:
        tipo: 1 for divisions, 2 for openings.

    Returns:
        DataFrame containing the CSV data, or None if the type is invalid.
    """
    match tipo:
        case 1:
            return pd.read_csv(URL_INDEC_DIVISIONES, encoding='ISO-8859-1', decimal=",", delimiter=";")
            # return pd.read_csv(FILE_INDEC_DIVISIONES, encoding='ISO-8859-1', decimal=",", delimiter=";")
        case 2:
            return pd.read_csv(URL_INDEC_APERTURAS, encoding='ISO-8859-1', decimal=",", delimiter=";")
            # return pd.read_csv(FILE_INDEC_APERTURAS, encoding='ISO-8859-1', decimal=",", delimiter=";")
        case _:
            print(f'Error en get_file_INDEC(tipo: int = {tipo})')


def get_ipc(file_infla_empalmada: str) -> pd.DataFrame:
    """Read and format monthly IPC data from local XLSX file.

    Args:
        file_infla_empalmada: Path to the IPC2000.xlsx file.

    Returns:
        DataFrame containing date, IPC, monthly inflation, and month length.
    """
    ipc = pd.read_excel(file_infla_empalmada)
    ipc['Date'] = pd.to_datetime(ipc['Date'], format='%Y-%m-%d')
    ipc = ipc.set_index('Date', drop=False)
    ipc['InflaMensual'] = ipc['IPC'].pct_change()
    ipc = cod.get_date(ipc, day=False)
    ipc['CantD'] = ipc.apply(lambda row: calendar.monthrange(row['year'], row['month'])[1], axis=1)
    # Without the day because is always the last day of the month.
    return ipc[cod.COLS[:-1] + ['IPC', 'InflaMensual',
                                'CantD']].copy()


def get_act_cap(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate updating and capitalizing indices.

    Args:
        df: DataFrame with IPC, InflaMensual, day, and CantD columns.

    Returns:
        DataFrame with computed 'Actualizador' and 'Capitalizador' columns.
    """
    df['IPC'] = df['IPC'] / (1 + df['InflaMensual'])
    df['IPC'] = df['IPC'] / df['IPC'].iloc[0]
    df['Actualizador'] = df['IPC'] * (1 + df['InflaMensual']) ** (df['day'] / df['CantD'])

    df['Capitalizador'] = (1 / df['Actualizador'])

    df['Capitalizador'] = df['Capitalizador'] / df['Capitalizador'].iloc[-1]
    return df


def get_ipc_indec() -> pd.DataFrame:
    """Fetch monthly IPC data from INDEC website and format as national index.

    Returns:
        DataFrame containing national IPC, MoM variation, YoY variation, and Date_Cod.
    """
    df = get_file_indec()
    nacional = df.query("Codigo == '0' & Region == 'Nacional'")[
        ['Periodo', 'Indice_IPC', 'v_m_IPC', 'v_i_a_IPC']].copy().reset_index(drop=True)
    nacional = nacional.apply(pd.to_numeric, errors='coerce')
    nacional['InflaMensual'] = nacional['Indice_IPC'].pct_change()
    nacional['Date_Cod'] = pd.to_datetime(nacional['Periodo'], format='%Y%m').dt.strftime('%m-%Y')
    nacional = nacional[['Indice_IPC', 'v_m_IPC', 'v_i_a_IPC', 'InflaMensual', 'Date_Cod']].copy()
    nacional.columns = ['IPC', 'VarMoM', 'VarYoY', 'InflaMensual', 'Date_Cod']
    return nacional


def get_div_ipc(tipo: int = 1, region: str = 'Nacional') -> pd.DataFrame:
    """Get division indices, categories, or goods/services for a region.

    Args:
        tipo: 1 for divisions, 2 for categories, 3 for goods/services.
        region: Region to filter by.

    Returns:
        DataFrame containing division/category indices for the specified region.
    """
    general = get_file_indec()
    columnas = ['Codigo', 'Descripcion', 'Periodo', 'Indice_IPC', 'Region']
    match tipo:
        case 1:
            df = general.query("Region == @region & Clasificador == 'Nivel general y divisiones COICOP'")[ columnas].copy().reset_index(drop=1)
        case 2:
            df = general.query("Region == @region & Clasificador == 'Categorias'")[ columnas].copy().reset_index(drop=1)
            df['Descripcion'] = df['Codigo'].copy()
        case 3:
            df = general.query("Region == @region & Clasificador == 'Bienes y servicios'")[ columnas].copy().reset_index(drop=1)
            df['Descripcion'] = df['Codigo'].apply( lambda x: 'Bienes' if x == 'B' else ('Servicios' if x == 'S' else 'Other'))
        case _:
            df = None
            print('Error en get_div_IPC(tipo: int = 1)')

    if df is None:
        return None

    df['Indice_IPC'] = pd.to_numeric(df['Indice_IPC'], errors='coerce')
    df = cod.get_date_ipc(df)
    return df[['Codigo', 'Descripcion', 'Indice_IPC', 'Date_Cod', 'Region']].copy()


def get_aper_ipc(prepagas: bool = True) -> pd.DataFrame:
    """Fetch and parse detailed IPC openings from INDEC.

    Args:
        prepagas: If True, normalize prepagas code.

    Returns:
        DataFrame with detailed opening indices by region.
    """
    aperturas = get_file_indec(2)
    aperturas['Codigo'] = aperturas['Codigo'].astype(str)
    if prepagas:
        aperturas.loc[aperturas['Codigo'] == '06.4.1', 'Codigo'] = '06.4'
    aperturas['Indice_IPC'] = pd.to_numeric(aperturas['Indice_IPC'], errors='coerce')
    return aperturas[['Codigo', 'Descripcion_aperturas', 'Periodo', 'Indice_IPC', 'Region']].copy()


def get_ponderadores_ipc() -> pd.DataFrame:
    """Fetch regional weight shares for IPC categories.

    Returns:
        DataFrame containing weights by region for each category code.
    """
    ponderadores = pd.read_excel(URL_INDEC_PONDERADORES, header=2).iloc[:-2,
    :].copy()
    ponderadores.columns = ['Codigo', 'Descripcion', 'GBA', 'Pampeana', 'Noreste', 'Noroeste', 'Cuyo', 'Patagonia']
    ponderadores['Codigo'] = ponderadores['Codigo'].astype(str)
    return ponderadores


def get_next_indec_release_date(dates_file: str) -> datetime | None:
    """
    Reads INDEC release dates from a JSON file and returns the first future date.

    :param dates_file: Path to the JSON file with release dates.
    :return: Next release datetime or None if not found.
    """
    try:
        with open(dates_file, 'r') as f:
            data = json.load(f)
        
        now = datetime.now()
        # Flatten all dates across years
        all_dates = []
        for year in data:
            all_dates.extend(data[year])
        
        # Convert to datetime and sort
        date_objs = sorted([datetime.strptime(d, "%Y-%m-%d") for d in all_dates])
        
        for release_date in date_objs:
            # We want the next release date that is in the future
            if release_date.date() >= now.date():
                # If it's today, we only schedule it if it's before 20:00 (since we run at 20:00)
                # But typically this function is called AFTER a run, so we likely want the next one.
                if release_date.date() == now.date() and now.hour >= 20:
                    continue
                return release_date
    except Exception as e:
        print(f"Error reading INDEC dates file: {e}")
    return None


def main() -> None:
    """Run the main program to process IPC data."""
    print(f'Se corrió el main de {__name__}')


if __name__ == '__main__':
    main()