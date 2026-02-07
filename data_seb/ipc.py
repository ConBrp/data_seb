import pandas as pd
import calendar
# import print_calendar

from . import cod

URL_INDEC_DIVISIONES = 'https://www.indec.gob.ar/ftp/cuadros/economia/serie_ipc_divisiones.csv'
URL_INDEC_APERTURAS = 'https://www.indec.gob.ar/ftp/cuadros/economia/serie_ipc_aperturas.csv'


def get_file_indec(tipo: int = 1) -> pd.DataFrame | None:
    """
    Devuelve un DataFrame del tipo seleccionado: 1 para serie_ipc_divisiones, 2 para serie_ipc_aperturas.

    Returns a DataFrame of the selected type: 1 for serie_ipc_divisiones, 2 for serie_ipc_aperturas.

    :param tipo: Tipo a seleccionar (1: serie_ipc_divisiones, 2: serie_ipc_aperturas) / Type to select (1: serie_ipc_divisiones, 2: serie_ipc_aperturas).
    :return: DataFrame con datos de la hoja seleccionada o None si el tipo es inválido / DataFrame with data from the selected sheet or None if the type is invalid.
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
    """
    Devuelve un DataFrame con datos mensuales del IPC, del archivo IPC2000.xlsx.

    Returns a DataFrame with monthly IPC data from the IPC2000.xlsx file.

    :param file_infla_empalmada: Ruta al archivo IPC2000.xlsx / Path to the IPC2000.xlsx file.
    :return: DataFrame 'Fecha', 'Date', 'IPC', 'InflaMensual', 'CantD', 'Mes', 'Año' / DataFrame 'Fecha', 'Date', 'IPC', 'InflaMensual', 'CantD', 'Mes', 'Año'.
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
    """
    Devuelve un DataFrame con datos diarios, para actualizar y capitalizar valores.

    Returns a DataFrame with daily data for updating and capitalizing values.

    :param df: DataFrame con datos de 'IPC', 'InflaMensual', 'Dia', 'CantD' / DataFrame with 'IPC', 'InflaMensual', 'Dia', 'CantD' data.
    :return: DataFrame 'Actualizador', 'Capitalizador' / DataFrame 'Actualizador', 'Capitalizador'.
    """
    df['IPC'] = df['IPC'] / (1 + df['InflaMensual'])
    df['IPC'] = df['IPC'] / df['IPC'].iloc[0]
    df['Actualizador'] = df['IPC'] * (1 + df['InflaMensual']) ** (df['day'] / df['CantD'])

    df['Capitalizador'] = (1 / df['Actualizador'])

    df['Capitalizador'] = df['Capitalizador'] / df['Capitalizador'].iloc[-1]
    return df


def get_ipc_indec() -> pd.DataFrame:
    """
    Devuelve un DataFrame con datos mensuales del IPC, del archivo del INDEC.

    Returns a DataFrame with monthly IPC data from the INDEC file.

    :return: DataFrame 'IPC', 'VarMoM', 'VarYoY', 'InflaMensual', 'Date' / DataFrame 'IPC', 'VarMoM', 'VarYoY', 'InflaMensual', 'Date'.
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


def get_div_ipc(tipo: int = 1) -> pd.DataFrame:
    """
    Devuelve un DataFrame del tipo seleccionado: 1 para las 12 divisiones COICOP, 2 para categorías, 3 para bienes y servicios.

    Returns a DataFrame of the selected type: 1 for the 12 COICOP divisions, 2 for categories, 3 for goods and services.

    :param tipo: Tipo a seleccionar (1: divisiones COICOP, 2: categorías, 3: bienes y servicios) / Type to select (1: COICOP divisions, 2: categories, 3: goods and services).
    :return: DataFrame 'Codigo', 'Descripcion', 'Indice_IPC', 'Date', 'Mes', 'Año' / DataFrame 'Codigo', 'Descripcion', 'Indice_IPC', 'Date', 'Mes', 'Año'.
    """
    general = get_file_indec()
    columnas = ['Codigo', 'Descripcion', 'Periodo', 'Indice_IPC']
    match tipo:
        case 1:
            nacional = general.query("Region == 'Nacional' & Clasificador == 'Nivel general y divisiones COICOP'")[
                columnas].copy().reset_index(drop=1)
        case 2:
            nacional = general.query("Region == 'Nacional' & Clasificador == 'Categorias'")[
                columnas].copy().reset_index(drop=1)
            nacional['Descripcion'] = nacional['Codigo'].copy()
        case 3:
            nacional = general.query("Region == 'Nacional' & Clasificador == 'Bienes y servicios'")[
                columnas].copy().reset_index(drop=1)
            nacional['Descripcion'] = nacional['Codigo'].apply(
                lambda x: 'Bienes' if x == 'B' else ('Servicios' if x == 'S' else 'Other'))
        case _:
            nacional = None
            print('Error en get_div_IPC(tipo: int = 1)')

    nacional['Indice_IPC'] = pd.to_numeric(nacional['Indice_IPC'], errors='coerce')
    nacional = cod.get_date_ipc(nacional) # TODO, puede ser que se haga más fácil y borrar la función.
    return nacional[['Codigo', 'Descripcion', 'Indice_IPC', 'Date_Cod']].copy()


def get_aper_ipc(prepagas: bool = True) -> pd.DataFrame:
    """
    Devuelve un DataFrame con las aperturas del IPC.

    Returns a DataFrame with the IPC openings.

    :param prepagas: Si corrige el código de prepagas o no / If True, correct the prepagas code.
    :return: DataFrame 'Codigo', 'Periodo', 'Indice_IPC', 'Region', 'Descripcion_aperturas' / DataFrame 'Codigo', 'Periodo', 'Indice_IPC', 'Region', 'Descripcion_aperturas'.
    """
    aperturas = get_file_indec(2)
    aperturas['Codigo'] = aperturas['Codigo'].astype(str)
    if prepagas:
        aperturas.loc[aperturas['Codigo'] == '06.4.1', 'Codigo'] = '06.4'
    aperturas['Indice_IPC'] = pd.to_numeric(aperturas['Indice_IPC'], errors='coerce')
    return aperturas[['Codigo', 'Descripcion_aperturas', 'Periodo', 'Indice_IPC', 'Region']].copy()


def get_ponderadores_ipc() -> pd.DataFrame:
    """
    Devuelve un DataFrame con los ponderadores para las categorías según regiones, del archivo ponderadores_ipc.xls.

    Returns a DataFrame with the weights for categories by region from the ponderadores_ipc.xls file.

    :return: DataFrame 'Codigo', 'Descripcion', 'GBA', 'Pampeana', 'Noreste', 'Noroeste', 'Cuyo', 'Patagonia' / DataFrame 'Codigo', 'Descripcion', 'GBA', 'Pampeana', 'Noreste', 'Noroeste', 'Cuyo', 'Patagonia'.
    """
    ponderadores = pd.read_excel(r'C:\Users\berge\Desktop\Me\programs\1X\Data\ponderadores_ipc.xls', header=2).iloc[:-2,
                   :].copy()
    ponderadores.columns = ['Codigo', 'Descripcion', 'GBA', 'Pampeana', 'Noreste', 'Noroeste', 'Cuyo', 'Patagonia']
    ponderadores['Codigo'] = ponderadores['Codigo'].astype(str)
    return ponderadores


def main() -> None:
    """
    Ejecuta el programa principal para procesar datos del IPC.

    Runs the main program to process IPC data.

    :return: None / None.
    """
    print(f'Se corrió el main de {__name__}')


if __name__ == '__main__':
    main()