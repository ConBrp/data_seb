import pandas as pd
import calendar

import ipc

URL_OYD = 'https://www.indec.gob.ar/ftp/cuadros/economia/sh_oferta_demanda_12_24.xls' # Ojo que el link del pbi cambia con el trimestre.
URL_EMAE = 'https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_mensual_base2004.xls'
URL_EMAE_A = 'https://www.indec.gob.ar/ftp/cuadros/economia/sh_emae_actividad_base2004.xls'


def get_file_oyd(sheet_name: str, online: bool = True) -> pd.DataFrame:
    return pd.read_excel(URL_OYD, decimal=',', sheet_name=sheet_name)


def get_emae() -> pd.DataFrame:
    """
    Devuelve un df con los valores del EMAE indizado con la fecha.
    :return: df 'Original', 'Desestacionalizada', 'Tendencia_ciclo'.
    """
    df = pd.read_excel(URL_EMAE, header=[0, 1, 2, 3])
    df = df.iloc[:, [2, 4, 6]].dropna().copy()
    df.columns = ['Original', 'Desestacionalizada', 'Tendencia_ciclo']
    df.index = pd.date_range(start='2004-01-01', periods=df.shape[0], freq='ME')
    return df


def get_emae_actividades() -> pd.DataFrame:
    """
    Devuelve un df las actividades del EMAE indizado con la fecha.
    :return: df con las actividades como columnas.
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
    Devuelve un df limpio de las series del pbi, ya que están en filas y con espacios en blanco.
    :param df: El que contiene la serie del PBI con los promedios anuales también.
    :return: df 'PBI'.
    """
    df = df.iloc[1:].dropna().reset_index(drop=True)
    df.columns = ["PBI"]
    df['PBI'] = pd.to_numeric(df['PBI'])
    total = list(range(-1, len(df), 5))[1:]
    buenos = [True if x not in total else False for x in range(len(df))]
    df = df.loc[buenos]
    df.index = pd.date_range(start='2004-01-01', periods=df.shape[0], freq='QE')
    return df


def get_pbi_pcorrientes(file_infla_empalmada: str, sin_estimar: bool = True) -> pd.DataFrame:
    """
    Devuelve un df con los PIB a precios corrientes trimestrales indizado con la fecha.
    :return: df 'PBI'.
    """
    pib = pd.DataFrame(get_file_oyd('cuadro 8').loc[5])
    pib = limpiar_serie_pbi(pib)
    ultimo_pib = pib.iloc[-1]
    ultimo_ipc = ipc.get_ipc(file_infla_empalmada).iloc[-1]
    if sin_estimar:
        return pib
    else:
        # Se estima el PIB para los trimestres faltantes.
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
            ipc_actual = ipc.get_ipc(creds.ipc_script['FILE_INFLA_EMPALMADA']).loc[fecha_actual, 'IPC']
            ipc_proximo_trimestre = ipc.get_ipc(
                creds.ipc_script['FILE_INFLA_EMPALMADA']).loc[fecha_proximo_trimestre, 'IPC']

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
    Devuelve un df con los PIB a reales trimestrales indizado con la fecha.
    :return: df 'PBI'.
    """
    df = pd.DataFrame(get_file_oyd('cuadro 1').loc[5])
    return limpiar_serie_pbi(df)


def days_in_quarter(date: pd.Timestamp, cant_d: bool = True) -> int:
    """
    Devuelve la cantidad de días de un trimestre, o la cantidad de días que van en uno. Pasando una fecha.
    :param date: Fecha de referencia para los cálculos.
    :param cant_d: Si devuelve solo la cantidad de días en el trimestre, lo que ya pasaron hasta la fecha.
    :return: Cantidad de días.
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

def main():
    print(f'Se corrió el main de {__name__}')

if __name__ == '__main__':
    main()