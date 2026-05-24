import pandas as pd

COLS = ['Date', 'Date_Cod', 'day']


def get_date(df: pd.DataFrame, date: str = 'Date', day: bool = True) -> pd.DataFrame:
    """Return a DataFrame with date columns formatted.

    Args:
        df: DataFrame to convert.
        date: Name of the column containing the date.
        day: If True, extract and add a 'day' column.

    Returns:
        The DataFrame with added 'month', 'year', and 'Date_Cod' columns.
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
    """Return a DataFrame with end-of-month dates and Date_Cod.

    Args:
        df: DataFrame to convert, containing a 'Periodo' column in YYYYMM format.

    Returns:
        The DataFrame with 'month', 'Año', 'Date' (MonthEnd offset), and 'Date_Cod' columns.
    """
    df['Periodo'] = pd.to_datetime(df['Periodo'], format='%Y%m')
    df['month'] = df['Periodo'].dt.month
    df['Año'] = df['Periodo'].dt.year
    df['Date'] = df['Periodo'] + pd.offsets.MonthEnd(0)
    df['Date_Cod'] = df['Date'].dt.strftime('%m-%Y')
    return df


def main() -> None:
    """Run the main program to process date data."""
    print(f'Se corrió el main de {__name__}')


if __name__ == '__main__':
    main()
