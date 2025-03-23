import pandas as pd
import calendar

from . import cod


def get_cpi(file_cpi1913: str) -> pd.DataFrame:
    """
    Devuelve un DataFrame con el IPC del archivo CPI1913.xlsx, con el CPI base 1967.

    Returns a DataFrame with the CPI from the CPI1913.xlsx file, with the 1967 base CPI.

    :param file_cpi1913: Ruta al archivo CPI1913.xlsx / Path to the CPI1913.xlsx file.
    :return: DataFrame 'CPI', 'InflaMensual', 'CantD', 'Fecha', 'Date', 'Mes', 'Año' / DataFrame 'CPI', 'InflaMensual', 'CantD', 'Fecha', 'Date', 'Mes', 'Año'.
    """
    cpi = pd.read_excel(file_cpi1913, parse_dates=['Fecha'])[['Fecha', 'CPI']].copy()
    # cpi['Fecha'] = pd.to_datetime(cpi['Fecha'], format='%Y-%m-%d')
    cpi['InflaMensual'] = cpi['CPI'].pct_change()
    cpi = cod.get_date(cpi, day=False)
    cpi['CantD'] = cpi.apply(lambda row: calendar.monthrange(row['Año'], row['Mes'])[1], axis=1)
    return cpi[cod.COLS[:-1] + ['CPI', 'InflaMensual', 'CantD']].copy()


def get_act_cap(df: pd.DataFrame, us: bool = False) -> pd.DataFrame:
    """
    Devuelve un DataFrame con datos diarios, para actualizar y capitalizar valores.

    Returns a DataFrame with daily data for updating and capitalizing values.

    :param df: DataFrame con datos de 'CPI', 'InflaMensual', 'Dia', 'CantD' / DataFrame with 'CPI', 'InflaMensual', 'Dia', 'CantD' data.
    :param us: Si agregar 'us' al final de las columnas del DataFrame o no / If True, append 'us' to the DataFrame column names.
    :return: DataFrame 'Actualizador', 'Capitalizador' / DataFrame 'Actualizador', 'Capitalizador'.
    """
    infla_column = 'InflaMensualUS' if us else 'InflaMensual'
    df['CPI'] = df['CPI'] / (1 + df[infla_column])
    df['CPI'] = df['CPI'] / df['CPI'].iloc[0]

    actualizador_column = 'ActualizadorUS' if us else 'Actualizador'
    capitalizador_column = 'CapitalizadorUS' if us else 'Capitalizador'

    df[actualizador_column] = df['CPI'] * (1 + df[infla_column]) ** (df['Dia'] / df['CantD'])
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