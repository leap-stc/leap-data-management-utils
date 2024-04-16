from .cmip_transforms import CMIPBQInterface, IIDEntry, LogCMIPToBigQuery
from .data_management_transforms import BQInterface, RegisterDatasetToCatalog

__all__ = (
    'BQInterface',
    'RegisterDatasetToCatalog',
    'IIDEntry',
    'CMIPBQInterface',
    'LogCMIPToBigQuery',
)

try:
    from ._version import __version__
except ImportError:
    __version__ = 'unknown'
