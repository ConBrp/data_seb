"""data-seb package entry point.

Exposes the public API for fetching and processing economic data
from BCRA and INDEC.
"""

from .bcra import (
    get_principales_variables,
    get_official_exchange_rate,
    get_international_reserves,
    get_monetary_base,
    get_m2,
    get_inflation_expectations,
)
from .ipc import (
    get_ipc,
    get_ipc_indec,
    get_div_ipc,
    get_aper_ipc,
    get_ponderadores_ipc,
)
from .ipc_se import (
    get_ipcse,
    get_ipcse_divisiones,
)
from .cpi import get_cpi
from .dolar import get_blue
from .pbi import (
    get_emae,
    get_emae_actividades,
    get_pbi_real,
    get_pbi_pcorrientes,
)
from .cod import get_date, get_date_ipc

__all__ = [
    'get_principales_variables',
    'get_official_exchange_rate',
    'get_international_reserves',
    'get_monetary_base',
    'get_m2',
    'get_inflation_expectations',
    'get_ipc',
    'get_ipc_indec',
    'get_div_ipc',
    'get_aper_ipc',
    'get_ponderadores_ipc',
    'get_ipcse',
    'get_ipcse_divisiones',
    'get_cpi',
    'get_blue',
    'get_emae',
    'get_emae_actividades',
    'get_pbi_real',
    'get_pbi_pcorrientes',
    'get_date',
    'get_date_ipc',
]
