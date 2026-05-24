import pandas as pd
import pytest
from data_seb.cod import get_date, get_date_ipc


def test_get_date_with_day():
    # Input DataFrame with Datetime Date
    df = pd.DataFrame({
        'Date': pd.to_datetime(['2025-01-15', '2025-02-20'])
    })
    
    result = get_date(df, date='Date', day=True)
    
    assert 'day' in result.columns
    assert 'Date_Cod' in result.columns
    assert 'month' in result.columns
    assert 'year' in result.columns
    
    assert list(result['day']) == [15, 20]
    assert list(result['Date_Cod']) == ['01-2025', '02-2025']
    assert list(result['month']) == [1, 2]
    assert list(result['year']) == [2025, 2025]


def test_get_date_without_day():
    df = pd.DataFrame({
        'Date': pd.to_datetime(['2025-03-10'])
    })
    
    result = get_date(df, date='Date', day=False)
    
    assert 'day' not in result.columns
    assert list(result['Date_Cod']) == ['03-2025']


def test_get_date_ipc():
    df = pd.DataFrame({
        'Periodo': ['202501', '202502']
    })
    
    result = get_date_ipc(df)
    
    assert 'month' in result.columns
    assert 'Año' in result.columns
    assert 'Date' in result.columns
    assert 'Date_Cod' in result.columns
    
    assert list(result['month']) == [1, 2]
    assert list(result['Año']) == [2025, 2025]
    
    # Dates should be end of the month
    assert result['Date'].iloc[0] == pd.Timestamp('2025-01-31')
    assert result['Date'].iloc[1] == pd.Timestamp('2025-02-28')
    assert list(result['Date_Cod']) == ['01-2025', '02-2025']
