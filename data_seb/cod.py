import pandas as pd

COLS = ['Fecha', 'Date', 'Dia']


def get_date(df: pd.DataFrame, date: str = 'Fecha', day: bool = True) -> pd.DataFrame:
    """
    Devuelve df con código Date.
    :param df: df a convertir.
    :param date: Nombre de la columna con fecha.
    :param day: Si agregar día o no.
    :return: df 'Mes', 'Año', 'Date'
    """
    if day:
        df['Dia'] = df[date].dt.day
    if pd.api.types.is_datetime64_dtype(df['Fecha']):
        df['Date'] = df['Fecha'].dt.strftime('%m-%Y')
    else:
        df['Date'] = df['Mes'].astype(str) + '-' + df['Año'].astype(str)
        df['Date'] = pd.to_datetime(df['Date'], format='%m-%Y').dt.strftime('%m-%Y')
    df['Mes'] = df[date].dt.month
    df['Año'] = df[date].dt.year
    return df


def get_date_ipc(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve df con código Date con día al final de mes.
    :param df: df a convertir.
    :return: df 'Mes', 'Año', 'Date'
    """
    df['Periodo'] = pd.to_datetime(df['Periodo'], format='%Y%m')
    df['Mes'] = df['Periodo'].dt.month
    df['Año'] = df['Periodo'].dt.year
    df['Date'] = df['Periodo'] + pd.offsets.MonthEnd(0)
    return df

def main():
    print(f'Se corrió el main de {__name__}')

if __name__ == '__main__':
    main()
