"""
IPCSE - Índice de Precios al Consumidor sin estacionalidad

Seasonally adjusted CPI using X-13ARIMA-SEATS (via statsmodels) with STL fallback.
"""

import warnings
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple

from .ipc import get_div_ipc, get_ponderadores_ipc, get_ipc_indec
from statsmodels.tsa.seasonal import STL
from statsmodels.tsa.x13 import x13_arima_analysis

REGIONS = ['GBA', 'Pampeana', 'Noreste', 'Noroeste', 'Cuyo', 'Patagonia']
REGION_WEIGHTS = {
    'GBA': 44.7,
    'Pampeana': 34.2,
    'Noreste': 4.5,
    'Noroeste': 6.9,
    'Cuyo': 5.2,
    'Patagonia': 4.6
}

DIVISION_CODES = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']


def seasonal_adjust_stl(series: pd.Series) -> pd.Series:
    """Apply STL seasonal adjustment.

    Used as fallback when X-13ARIMA-SEATS is not available.

    Args:
        series: Time series with datetime index.

    Returns:
        Seasonally adjusted series.
    """
    if len(series) < 24:
        return series

    series = series.astype(float)
    series = series.dropna()

    if len(series) < 24:
        return series

    stl = STL(series, period=12, robust=True)
    result = stl.fit()

    sa_series = result.trend + result.resid

    sa_series.name = series.name
    return sa_series


def seasonal_adjust_x13(series: pd.Series) -> pd.Series:
    """Apply seasonal adjustment to a time series using X-13ARIMA-SEATS.

    Falls back to STL if X-13ARIMA fails.

    Args:
        series: Time series with datetime index.

    Returns:
        Seasonally adjusted series.
    """
    if len(series) < 26:
        return series

    series = series.astype(float).dropna()

    if len(series) < 26:
        return series

    if type(series.index).__name__ == 'PeriodIndex':
        series_ts = series.copy()
        series_ts.index = series_ts.index.to_timestamp()
    else:
        series_ts = series

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            result = x13_arima_analysis(series_ts)
        return result.seasadj
    except Exception:
        print("  [WARNING] X-13ARIMA-SEATS failed, using STL fallback for seasonal adjustment")
        return seasonal_adjust_stl(series)


def get_regional_division_indices(region: str) -> pd.DataFrame:
    """Get division indices for a specific region.

    Args:
        region: Region name.

    Returns:
        DataFrame with division indices.
    """
    df = get_div_ipc(tipo=1, region=region)
    return df[df['Codigo'].isin(DIVISION_CODES)].copy()


def aggregate_divisions_to_index(div_df: pd.DataFrame, weights: pd.Series) -> pd.Series:
    """Aggregate division indices to a single index using weights.

    Args:
        div_df: DataFrame with 'Codigo', 'Indice_IPC', 'Date_Cod' columns.
        weights: Series with division weights (sum to 100).

    Returns:
        Aggregated index series.
    """
    pivot = div_df.pivot(index='Date_Cod', columns='Codigo', values='Indice_IPC')
    pivot.index = pd.to_datetime(pivot.index, format='%m-%Y')
    pivot = pivot.sort_index()

    weight_vec = weights.reindex(pivot.columns) / 100.0
    weight_vec = weight_vec.fillna(0)

    aggregated = (pivot * weight_vec).sum(axis=1)
    return aggregated


def get_regional_weights(ponderadores: pd.DataFrame, region: str) -> pd.Series:
    """Get division weights for a specific region.

    Args:
        ponderadores: DataFrame from get_ponderadores_ipc().
        region: Region name.

    Returns:
        Series with division weights.
    """
    weights = ponderadores.set_index('Codigo')[region]
    weights = weights[weights.index.isin(DIVISION_CODES)]
    return weights.astype(float)


def aggregate_regions_to_nacional(region_indices: Dict[str, pd.Series]) -> pd.Series:
    """Aggregate regional indices to national using regional participation weights.

    Args:
        region_indices: Dict mapping region name to index series.

    Returns:
        National index series.
    """
    common_idx = None
    for idx in region_indices.values():
        if common_idx is None:
            common_idx = idx.index
        else:
            common_idx = common_idx.intersection(idx.index)

    national = pd.Series(0.0, index=common_idx, name='IPC_se')
    total_weight = 0.0

    for region, weight in REGION_WEIGHTS.items():
        if region in region_indices:
            region_idx = region_indices[region].reindex(common_idx)
            national = national + region_idx * (weight / 100.0)
            total_weight += weight

    if total_weight > 0:
        national = national / (total_weight / 100.0)

    return national.sort_index()


def compute_ipcse_indicators(ipcse: pd.Series) -> pd.DataFrame:
    """Compute VarMoM, VarYoY, and InflaMensual from IPCse series.

    Args:
        ipcse: IPCse index series.

    Returns:
        DataFrame with IPCse and derived indicators.
    """
    df = pd.DataFrame(index=ipcse.index)
    df['IPC_se'] = ipcse
    df['VarMoM'] = ipcse.pct_change() * 100
    df['VarYoY'] = ipcse.pct_change(12) * 100
    df['InflaMensual'] = ipcse.pct_change() * 100
    return df[['IPC_se', 'VarMoM', 'VarYoY', 'InflaMensual']]


def get_ipcse() -> pd.DataFrame:
    """Calculate the seasonally adjusted national IPC (IPCse).

    This implements the IPCse methodology:
    - Get division indices by region (6 regions x 12 divisions).
    - Apply X-13ARIMA-SEATS to each division index that has seasonality.
    - Aggregate seasonally adjusted divisions to regional index.
    - Aggregate regional indices to national using regional participation weights.

    Returns:
        DataFrame with 'IPC_se', 'VarMoM', 'VarYoY', 'InflaMensual', 'Date_Cod'.
    """
    ponderadores = get_ponderadores_ipc()

    regional_adjusted_indices = {}

    for region in REGIONS:
        div_df = get_regional_division_indices(region)

        region_weights = get_regional_weights(ponderadores, region)
        div_indices = {}

        for div_code in DIVISION_CODES:
            div_series = div_df[div_df['Codigo'] == div_code].set_index('Date_Cod')['Indice_IPC']
            div_series.index = pd.to_datetime(div_series.index, format='%m-%Y')
            div_series = div_series.sort_index()
            div_series = div_series[~div_series.index.duplicated(keep='last')]
            div_series = div_series.astype(float)

            if len(div_series) >= 24:
                div_sa = seasonal_adjust_x13(div_series)
                div_indices[div_code] = div_sa
            else:
                div_indices[div_code] = div_series

        div_combined = pd.DataFrame(div_indices)
        div_combined = div_combined.sort_index()

        weight_vec = region_weights.reindex(div_combined.columns) / 100.0
        weight_vec = weight_vec.fillna(0)

        regional_index = (div_combined * weight_vec).sum(axis=1)
        regional_adjusted_indices[region] = regional_index

    national_ipcse = aggregate_regions_to_nacional(regional_adjusted_indices)

    result = compute_ipcse_indicators(national_ipcse)

    final = result.reset_index()
    if 'index' in final.columns:
        final = final.rename(columns={'index': 'Date'})
    elif 'Date_Cod' in final.columns:
        final = final.rename(columns={'Date_Cod': 'Date'})
    
    final['Date_Cod'] = final['Date'].dt.strftime('%m-%Y')
    return final


def get_ipcse_divisiones(region: str = 'Nacional') -> pd.DataFrame:
    """Get seasonally adjusted division indices for a region.

    Args:
        region: Region name ('Nacional' or regional code).

    Returns:
        DataFrame with seasonally adjusted division indices.
    """
    ponderadores = get_ponderadores_ipc()

    div_df = get_regional_division_indices(region) if region != 'Nacional' else get_div_ipc(tipo=1, region=region)
    div_df = div_df[div_df['Codigo'].isin(DIVISION_CODES)]

    region_weights = get_regional_weights(ponderadores, region) if region != 'Nacional' else None

    result_dict = {}
    for div_code in DIVISION_CODES:
        div_series = div_df[div_df['Codigo'] == div_code].set_index('Date_Cod')['Indice_IPC']
        div_series.index = pd.to_datetime(div_series.index, format='%m-%Y')
        div_series = div_series.sort_index()
        div_series = div_series[~div_series.index.duplicated(keep='last')]
        div_series = div_series.astype(float)

        if len(div_series) >= 24:
            div_sa = seasonal_adjust_x13(div_series)
            result_dict[div_code] = div_sa
        else:
            result_dict[div_code] = div_series

    result = pd.DataFrame(result_dict)
    result['Date_Cod'] = result.index.strftime('%m-%Y')
    return result


def main() -> None:
    """Test the IPCse module."""
    print(f"Computing IPCse (this may take a few minutes)...")
    ipcse = get_ipcse()
    print(f"\nLast 5 months of IPCse:")
    print(ipcse.tail())
    print(f"\nIPCse shape: {ipcse.shape}")


if __name__ == '__main__':
    main()