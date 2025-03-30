import pandas as pd
import requests


from . import cod


def get_blue(cod_date: bool = False, witch: int = 1) -> pd.DataFrame:
    response = requests.get('https://api.bluelytics.com.ar/v2/evolution.json')
    df = pd.DataFrame(response.json())
    df = df.query("source == 'Blue'").drop('source', axis=1).copy()
    df['date'] = pd.to_datetime(df['date'])
    df.columns = ['Date', 'Seller', 'Buyer']
    df = df.set_index('Date').sort_index()
    match witch:
        case 0:
            if cod_date:
                df['Fecha'] = df.index
                return cod.get_date(df)
            return df
        case 1:
            if cod_date:
                df['Fecha'] = df.index
                return cod.get_date(df)[cod.COLS + ['Seller']].copy()
            return df[['Seller']].copy()
        case _:
            return pd.DataFrame()







