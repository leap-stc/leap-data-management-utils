"""
utils that are specific to CMIP data management
"""

import logging
import warnings
from dataclasses import dataclass

import apache_beam as beam
import xarray as xr
import zarr
from dask.utils import parse_bytes
from dynamic_chunks.algorithms import (
    NoMatchingChunks,
    even_divisor_algo,
    iterative_ratio_increase_algo,
)
from pangeo_forge_recipes.transforms import Indexed, T

from leap_data_management_utils.bq_interfaces import CMIPBQInterface, IIDEntry
from leap_data_management_utils.cmip_testing import test_all

# TODO: I am not sure the chunking function belongs here, but it clutters the recipe and I did not want
# To open a whole file for this.
logger = logging.getLogger(__name__)


## Dynamic Chunking Wrapper
def dynamic_chunking_func(ds: xr.Dataset) -> dict[str, int]:
    logger.info(f'Input Dataset for dynamic chunking {ds =}')

    target_chunk_size = '150MB'
    target_chunks_aspect_ratio = {
        'time': 10,
        'x': 1,
        'i': 1,
        'ni': 1,
        'xh': 1,
        'nlon': 1,
        'lon': 1,  # TODO: Maybe import all the known spatial dimensions from xmip?
        'y': 1,
        'j': 1,
        'nj': 1,
        'yh': 1,
        'nlat': 1,
        'lat': 1,
    }
    size_tolerance = 0.5

    # Some datasets are smaller than the target chunk size and should not be chunked at all
    if ds.nbytes < parse_bytes(target_chunk_size):
        target_chunks = dict(ds.dims)

    else:
        try:
            target_chunks = even_divisor_algo(
                ds,
                target_chunk_size,
                target_chunks_aspect_ratio,
                size_tolerance,
                allow_extra_dims=True,
            )

        except NoMatchingChunks:
            warnings.warn(
                'Primary algorithm using even divisors along each dimension failed '
                'with. Trying secondary algorithm.'
                f'Input {ds=}'
            )
            try:
                target_chunks = iterative_ratio_increase_algo(
                    ds,
                    target_chunk_size,
                    target_chunks_aspect_ratio,
                    size_tolerance,
                    allow_extra_dims=True,
                )
            except NoMatchingChunks:
                raise ValueError(
                    'Could not find any chunk combinations satisfying '
                    'the size constraint with either algorithm.'
                    f'Input {ds=}'
                )
            # If something fails
            except Exception as e:
                raise e
        except Exception as e:
            raise e
    logger.info(f'Dynamic Chunking determined {target_chunks =}')
    return target_chunks


# ----------------------------------------------------------------------------------------------
# apache Beam stages
# ----------------------------------------------------------------------------------------------


@dataclass
class LogCMIPToBigQuery(beam.PTransform):
    """
    Logging stage for data written to zarr store
    """

    iid: str
    table_id: str
    retracted: bool = False
    tests_passed: bool = False

    def _log_to_bigquery(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        bq_interface = CMIPBQInterface(table_id=self.table_id)
        iid_entry = IIDEntry(
            iid=self.iid,
            store='gs://' + store.path,  # TODO: Get the full store path from the store object
            retracted=self.retracted,
            tests_passed=self.tests_passed,
        )
        bq_interface.insert_iid(iid_entry)
        return store

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._log_to_bigquery)


@dataclass
class Preprocessor(beam.PTransform):
    """
    Preprocessor for xarray datasets.
    Set all data_variables except for `variable_id` attrs to coord
    Add additional information

    """

    @staticmethod
    def _keep_only_variable_id(item: Indexed[T]) -> Indexed[T]:
        """
        Many netcdfs contain variables other than the one specified in the `variable_id` facet.
        Set them all to coords
        """
        index, ds = item
        print(f'Preprocessing before {ds =}')
        new_coords_vars = [var for var in ds.data_vars if var != ds.attrs['variable_id']]
        ds = ds.set_coords(new_coords_vars)
        print(f'Preprocessing after {ds =}')
        return index, ds

    @staticmethod
    def _sanitize_attrs(item: Indexed[T]) -> Indexed[T]:
        """Removes non-ascii characters from attributes see https://github.com/pangeo-forge/pangeo-forge-recipes/issues/586"""
        index, ds = item
        for att, att_value in ds.attrs.items():
            if isinstance(att_value, str):
                new_value = att_value.encode('utf-8', 'ignore').decode()
                if new_value != att_value:
                    print(
                        f'Sanitized datasets attributes field {att}: \n {att_value} \n ----> \n {new_value}'
                    )
                    ds.attrs[att] = new_value
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return (
            pcoll
            | 'Fix coordinates' >> beam.Map(self._keep_only_variable_id)
            | 'Sanitize Attrs' >> beam.Map(self._sanitize_attrs)
        )


@dataclass
class TestDataset(beam.PTransform):
    """
    Test stage for data written to zarr store
    """

    iid: str

    def _test(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        test_all(store, self.iid)
        return store

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | 'Testing - Running all tests' >> beam.Map(self._test)
