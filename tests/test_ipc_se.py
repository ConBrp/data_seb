import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from data_seb.ipc_se import (
    seasonal_adjust_stl,
    seasonal_adjust_x13,
    get_regional_weights,
    aggregate_regions_to_nacional,
    compute_ipcse_indicators,
    get_ipcse,
    REGIONS,
    DIVISION_CODES
)


def test_seasonal_adjust_stl():
    # Create synthetic series (linear trend + monthly seasonal component)
    dates = pd.date_range('2023-01-01', periods=36, freq='ME')
    trend = np.linspace(100, 150, 36)
    seasonal = np.array([5, 10, -5, -10, 0, 5, 10, -5, -10, 0, 5, 2] * 3)
    series = pd.Series(trend + seasonal, index=dates, name='IPC')
    
    adjusted = seasonal_adjust_stl(series)
    
    assert len(adjusted) == len(series)
    assert isinstance(adjusted, pd.Series)
    # The seasonal component should be largely removed, so trend is close
    assert np.allclose(adjusted, trend, atol=10.0)


@patch('data_seb.ipc_se.x13_arima_analysis')
def test_seasonal_adjust_x13_fallback(mock_x13):
    # Mock X-13 to fail to verify STL fallback
    mock_x13.side_effect = Exception("X-13 failed")
    
    dates = pd.date_range('2023-01-01', periods=36, freq='ME')
    series = pd.Series(np.linspace(100, 150, 36), index=dates, name='IPC')
    
    # This should fall back to STL and run successfully
    adjusted = seasonal_adjust_x13(series)
    assert len(adjusted) == len(series)


def test_get_regional_weights():
    ponderadores = pd.DataFrame({
        'Codigo': DIVISION_CODES,
        'Descripcion': ['Div ' + c for c in DIVISION_CODES],
        'GBA': [10.0] * 12
    })
    
    weights = get_regional_weights(ponderadores, 'GBA')
    assert len(weights) == 12
    assert weights.loc['01'] == 10.0


def test_aggregate_regions_to_nacional():
    dates = pd.date_range('2025-01-01', periods=3, freq='ME')
    region_indices = {
        region: pd.Series([100.0, 101.0, 102.0], index=dates)
        for region in REGIONS
    }
    
    national = aggregate_regions_to_nacional(region_indices)
    
    # Since all regions have index [100.0, 101.0, 102.0], the aggregated national
    # index should also be [100.0, 101.0, 102.0] (weights sum to 100)
    assert len(national) == 3
    assert list(national.values) == pytest.approx([100.0, 101.0, 102.0])


def test_compute_ipcse_indicators():
    dates = pd.date_range('2025-01-01', periods=13, freq='ME')
    # index starts at 100, increments by 1 each month
    ipcse = pd.Series(np.linspace(100, 112, 13), index=dates)
    
    df = compute_ipcse_indicators(ipcse)
    
    assert 'IPC_se' in df.columns
    assert 'VarMoM' in df.columns
    assert 'VarYoY' in df.columns
    assert 'InflaMensual' in df.columns
    
    # Check shape
    assert df.shape == (13, 4)


@patch('data_seb.ipc_se.get_regional_division_indices')
@patch('data_seb.ipc_se.get_ponderadores_ipc')
def test_get_ipcse(mock_get_ponderadores, mock_get_div_indices):
    # Mock ponderadores
    ponderadores = pd.DataFrame({
        'Codigo': DIVISION_CODES,
        'Descripcion': ['Div ' + c for c in DIVISION_CODES]
    })
    for r in REGIONS:
        ponderadores[r] = [100.0 / 12.0] * 12  # Equal weights for divisions
    mock_get_ponderadores.return_value = ponderadores
    
    # Generate mock division indices for 30 months
    dates = pd.date_range('2023-01-01', periods=30, freq='ME')
    date_cods = dates.strftime('%m-%Y')
    
    # Return DataFrame for each region
    def side_effect(region):
        rows = []
        for div in DIVISION_CODES:
            for d, dc in zip(dates, date_cods):
                rows.append({
                    'Codigo': div,
                    'Indice_IPC': 100.0 + (d.year - 2023) * 12 + d.month,  # Increasing trend
                    'Date_Cod': dc,
                    'Region': region
                })
        return pd.DataFrame(rows)
        
    mock_get_div_indices.side_effect = side_effect
    
    result = get_ipcse()
    
    assert 'Date' in result.columns
    assert 'Date_Cod' in result.columns
    assert 'IPC_se' in result.columns
    assert 'VarMoM' in result.columns
    
    # Check that Date_Cod is present and formatted
    assert result['Date_Cod'].iloc[0] == '01-2023'
    assert result['Date_Cod'].iloc[-1] == '06-2025'
