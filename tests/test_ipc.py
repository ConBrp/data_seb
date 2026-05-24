import json
import os
import pandas as pd
import pytest
from unittest.mock import patch, mock_open
from datetime import datetime
from data_seb.ipc import get_div_ipc, get_ponderadores_ipc, get_next_indec_release_date


@patch('data_seb.ipc.get_file_indec')
def test_get_div_ipc(mock_get_file):
    # Mock data return from INDEC
    mock_df = pd.DataFrame({
        'Codigo': ['01', '02', '03'],
        'Descripcion': ['Alimentos', 'Bebidas', 'Prendas'],
        'Periodo': ['202501', '202501', '202501'],
        'Indice_IPC': ['100.5', '102.3', '101.0'],
        'Region': ['Nacional', 'Nacional', 'GBA'],
        'Clasificador': ['Nivel general y divisiones COICOP', 'Nivel general y divisiones COICOP', 'Nivel general y divisiones COICOP']
    })
    mock_get_file.return_value = mock_df
    
    # Run get_div_ipc
    result = get_div_ipc(tipo=1, region='Nacional')
    
    assert result is not None
    assert len(result) == 2
    assert list(result['Codigo']) == ['01', '02']
    assert list(result['Region']) == ['Nacional', 'Nacional']
    assert list(result['Date_Cod']) == ['01-2025', '01-2025']


@patch('pandas.read_excel')
def test_get_ponderadores_ipc(mock_read_excel):
    # Mock data return from Excel
    mock_df = pd.DataFrame({
        'Col1': ['01', '02', '03', 'Total', 'Footnote'],
        'Col2': ['Food', 'Drinks', 'Clothing', 'Total', 'Footnote'],
        'Col3': [30.0, 10.0, 5.0, 45.0, None],
        'Col4': [32.0, 8.0, 6.0, 46.0, None],
        'Col5': [35.0, 5.0, 5.0, 45.0, None],
        'Col6': [28.0, 12.0, 4.0, 44.0, None],
        'Col7': [29.0, 11.0, 5.0, 45.0, None],
        'Col8': [31.0, 9.0, 6.0, 46.0, None]
    })
    mock_read_excel.return_value = mock_df
    
    result = get_ponderadores_ipc()
    
    assert len(result) == 3  # iloc[:-2] drops the last 2 rows
    assert list(result['Codigo']) == ['01', '02', '03']
    assert list(result.columns) == ['Codigo', 'Descripcion', 'GBA', 'Pampeana', 'Noreste', 'Noroeste', 'Cuyo', 'Patagonia']


def test_get_next_indec_release_date():
    mock_json_content = json.dumps({
        "2026": [
            "2026-05-15",
            "2026-06-15",
            "2026-07-15"
        ]
    })
    
    # Patch open
    m = mock_open(read_data=mock_json_content)
    with patch('builtins.open', m):
        # Freeze time to 2026-05-20
        with patch('data_seb.ipc.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime(2026, 5, 20, 12, 0)
            mock_datetime.strptime = datetime.strptime
            
            next_release = get_next_indec_release_date('fake_file.json')
            
            # The next release after May 20th is June 15th
            assert next_release == datetime(2026, 6, 15)
