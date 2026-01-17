import pandas as pd
import print_calendar

from . import ipc

URL_EMAE = 'https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls'
URL_EMAE_A = 'https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_actividad_base2004.xls'


def get_file_oyd(url: str, sheet_name: str, online: bool = True) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los datos de la hoja seleccionada del archivo de oferta y demanda del INDEC.

    Returns a DataFrame with data from the selected sheet of the INDEC's supply and demand file.

    :param url:
    :param sheet_name: Nombre de la hoja a devolver / Name of the sheet to retrieve.
    :param online: Define si se utiliza la URL en línea o un archivo local / If True, use the online URL; otherwise, use a local file.
    :return: DataFrame con datos de la hoja seleccionada / DataFrame with data from the selected sheet.
    """
    return pd.read_excel(url, decimal=',', sheet_name=sheet_name)


def get_emae() -> pd.DataFrame:
    """
    Devuelve un DataFrame con los valores del EMAE indizado con la fecha.

    Returns a DataFrame with EMAE values indexed by date.

    :return: DataFrame 'Original', 'Desestacionalizada', 'Tendencia_ciclo' / DataFrame 'Original', 'Desestacionalizada', 'Tendencia_ciclo'.
    """
    df = pd.read_excel(URL_EMAE, header=[0, 1, 2, 3])
    df = df.iloc[:, [2, 4, 6]].dropna().copy()
    df.columns = ['Original', 'Desestacionalizada', 'Tendencia_ciclo']
    df.index = pd.date_range(start='2004-01-01', periods=df.shape[0], freq='ME')
    return df


def get_emae_actividades() -> pd.DataFrame:
    """
    Devuelve un DataFrame con las actividades del EMAE indizado con la fecha.

    Returns a DataFrame with EMAE activities indexed by date.

    :return: DataFrame con las actividades como columnas / DataFrame with activities as columns.
    """
    df = pd.read_excel(URL_EMAE_A, header=[0, 1, 2, 3, 4])
    df = df.iloc[:, 2:].dropna().copy()
    df.columns = ['Agricultura, ganadería, caza y silvicultura',
                  'Pesca',
                  'Explotación de minas y canteras',
                  'Industria manufacturera',
                  'Electricidad, gas y agua',
                  'Construcción',
                  'Comercio mayorista, minorista y reparaciones',
                  'Hoteles y restaurantes',
                  'Transporte y comunicaciones',
                  'Intermediación financiera',
                  'Actividades inmobiliarias, empresariales y de alquiler',
                  'Administración pública y defensa; planes de seguridad social de afiliación obligatoria',
                  'Enseñanza',
                  'Servicios sociales y de salud',
                  'Otras actividades de servicios comunitarios, sociales y personales',
                  'Impuestos netos de subsidios']
    df.index = pd.date_range(start='2004-01-01', periods=df.shape[0], freq='ME')
    return df


def limpiar_serie_pbi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve un DataFrame limpio de las series del PBI, eliminando promedios anuales y espacios en blanco.

    Returns a clean DataFrame of PBI series, removing annual averages and blank spaces.

    :param df: DataFrame que contiene la serie del PBI con los promedios anuales / DataFrame containing the PBI series with annual averages.
    :return: DataFrame 'PBI' / DataFrame 'PBI'.
    """
    df = df.iloc[1:].dropna().reset_index(drop=True)
    df.columns = ["PBI"]
    df['PBI'] = pd.to_numeric(df['PBI'])
    total = list(range(-1, len(df), 5))[1:]
    buenos = [True if x not in total else False for x in range(len(df))]
    df = df.loc[buenos]
    df.index = pd.date_range(start='2004-01-01', periods=df.shape[0], freq='QE')
    return df


def get_pbi_pcorrientes(url: str, file_infla_empalmada: str = '', sin_estimar: bool = True) -> pd.DataFrame:
    """
    Devuelve un DataFrame con los PBI a precios corrientes trimestrales indizado con la fecha.

    Returns a DataFrame with quarterly PBI at current prices indexed by date.

    :param url:
    :param file_infla_empalmada: Ruta al archivo IPC2000.xlsx para estimaciones / Path to the IPC2000.xlsx file for estimations.
    :param sin_estimar: Si es True, no estima los trimestres faltantes / If True, do not estimate missing quarters.
    :return: DataFrame 'PBI' / DataFrame 'PBI'.
    """
    pib = pd.DataFrame(
        get_file_oyd(url, 'cuadro 8').loc[5])
    pib = limpiar_serie_pbi(pib)
    if sin_estimar:
        return pib
    else:
        # Se estima el PIB para los trimestres faltantes.
        ultimo_pib = pib.iloc[-1].copy()
        ultimo_ipc = ipc.get_ipc(file_infla_empalmada).iloc[-1]
        estimaciones = []
        fecha_actual = ultimo_pib.name
        fecha_final = ultimo_ipc.name

        while fecha_actual < fecha_final:
            # Se calcula la fecha del próximo trimestre.
            fecha_proximo_trimestre = fecha_actual + pd.DateOffset(months=3)
            while not fecha_proximo_trimestre.is_month_end:
                fecha_proximo_trimestre += pd.DateOffset()

            if fecha_proximo_trimestre >= fecha_final:
                fecha_proximo_trimestre = fecha_final

            # Se calcula el IPC relativo al trimestre actual.
            ipc_actual = ipc.get_ipc(file_infla_empalmada).loc[fecha_actual, 'IPC']
            ipc_proximo_trimestre = ipc.get_ipc(file_infla_empalmada).loc[fecha_proximo_trimestre, 'IPC']

            # Se estima el PBI para el próximo trimestre.
            pbi_estimado = ultimo_pib['PBI'] * ipc_proximo_trimestre / ipc_actual

            # Se crea una fila para el DataFrame de estimaciones.
            estimaciones.append(pd.DataFrame({'PBI': pbi_estimado}, index=[fecha_proximo_trimestre]))

            # Se actualiza la fecha actual para el próximo ciclo.
            fecha_actual = fecha_proximo_trimestre
            ultimo_pib['PBI'] = pbi_estimado

        # Se concatenan las estimaciones al DataFrame original de PIB.
        estimaciones_df = pd.concat(estimaciones)
        pib_estimado = pd.concat([pib, estimaciones_df])

        return pib_estimado


def get_pbi_real() -> pd.DataFrame:
    """
    Devuelve un DataFrame con los PBI reales trimestrales indizado con la fecha.

    Returns a DataFrame with quarterly real PBI indexed by date.

    :return: DataFrame 'PBI' / DataFrame 'PBI'.
    """
    df = pd.DataFrame(
        get_file_oyd('https://www.indec.gob.ar/ftp/cuadros/economia/sh_oferta_demanda_12_24.xls', 'cuadro 1').loc[5])
    return limpiar_serie_pbi(df)


def days_in_quarter(date: pd.Timestamp, cant_d: bool = True) -> int:
    """
    Devuelve la cantidad de días de un trimestre, o la cantidad de días transcurridos en uno, según una fecha.

    Returns the number of days in a quarter, or the number of days elapsed in one, based on a date.

    :param date: Fecha de referencia para los cálculos / Reference date for calculations.
    :param cant_d: Si es True, devuelve la cantidad total de días en el trimestre; si es False, los días transcurridos hasta la fecha / If True, return the total number of days in the quarter; if False, the days elapsed up to the date.
    :return: Cantidad de días / Number of days.
    """
    year = date.year
    quarter = date.quarter
    month = date.month
    day = date.day

    # Se determina el trimestre y los meses que abarca.
    match quarter:
        case 1:
            months = [1, 2, 3]
            start_month = 1
        case 2:
            months = [4, 5, 6]
            start_month = 4
        case 3:
            months = [7, 8, 9]
            start_month = 7
        case 4:
            months = [10, 11, 12]
            start_month = 10
        case _:
            months = None
            start_month = None

    # Se suman los días de los tres meses del trimestre.
    days = sum(calendar.monthrange(year, m)[1] for m in months)
    days_corr = sum(calendar.monthrange(year, m)[1] for m in range(start_month, month))
    days_corr += day
    if cant_d:
        return days
    else:
        return days_corr


def main() -> None:
    """
    Ejecuta el programa principal para procesar datos del PBI.

    Runs the main program to process PBI data.

    :return: None / None.
    """
    print(f'Se corrió el main de {__name__}')


if __name__ == '__main__':
    main()