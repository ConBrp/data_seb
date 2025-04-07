import pandas as pd

COLS = ['Date', 'Date_Cod', 'day']


def get_date(df: pd.DataFrame, date: str = 'Date', day: bool = True) -> pd.DataFrame:
    """
    Devuelve un DataFrame con código Date.

    Returns a DataFrame with Date code.

    :param df: DataFrame a convertir / DataFrame to convert.
    :param date: Nombre de la columna con fecha / Name of the column with the date.
    :param day: Si agregar día o no / If True, add the day column.
    :return: DataFrame 'Mes', 'Año', 'Date' / DataFrame 'Mes', 'Año', 'Date'.
    """
    if day:
        df['day'] = df[date].dt.day
    if pd.api.types.is_datetime64_dtype(df[date]):
        df['Date_Cod'] = df[date].dt.strftime('%m-%Y')
    else:
        df['Date'] = df['month'].astype(str) + '-' + df['year'].astype(str)
        df['Date_Cod'] = pd.to_datetime(df['Date'], format='%m-%Y').dt.strftime('%m-%Y')
    df['month'] = df[date].dt.month
    df['year'] = df[date].dt.year
    return df


def get_date_ipc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve un DataFrame con código Date con día al final de mes.

    Returns a DataFrame with Date code, setting the day to the end of the month.

    :param df: DataFrame a convertir / DataFrame to convert.
    :return: DataFrame 'Mes', 'Año', 'Date' / DataFrame 'Mes', 'Año', 'Date'.
    """
    df['Periodo'] = pd.to_datetime(df['Periodo'], format='%Y%m')
    df['month'] = df['Periodo'].dt.month
    df['Año'] = df['Periodo'].dt.year
    df['Date'] = df['Periodo'] + pd.offsets.MonthEnd(0)
    return df


def main() -> None:
    """
    Ejecuta el programa principal para procesar datos de fechas.

    Runs the main program to process date data.

    :return: None / None.
    """
    print(f'Se corrió el main de {__name__}')


if __name__ == '__main__':
    main()
