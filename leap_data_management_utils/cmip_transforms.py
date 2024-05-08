"""
utils that are specific to CMIP data management
"""

import datetime
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
from google.cloud import bigquery
from pangeo_forge_recipes.transforms import Indexed, T
from tqdm.auto import tqdm

from leap_data_management_utils.cmip_testing import test_all
from leap_data_management_utils.data_management_transforms import BQInterface

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


@dataclass
class IIDEntry:
    """Single row/entry for an iid
    :param iid: CMIP6 instance id
    :param store: URL to zarr store
    """

    iid: str
    store: str  # TODO: should this allow other objects?
    retracted: bool
    tests_passed: bool

    # Check if the iid conforms to a schema
    def __post_init__(self):
        schema = 'mip_era.activity_id.institution_id.source_id.experiment_id.member_id.table_id.variable_id.grid_label.version'
        facets = self.iid.split('.')
        if len(facets) != len(schema.split('.')):
            raise ValueError(f'IID does not conform to CMIP6 {schema =}. Got {self.iid =}')
        assert self.store.startswith('gs://')
        assert self.retracted in [True, False]
        assert self.tests_passed in [True, False]

        # TODO: Check each facet with the controlled CMIP vocabulary

        # TODO Check store validity?


@dataclass
class IIDResult:
    """Class to handle the results pertaining to a single IID."""

    results: bigquery.table.RowIterator
    iid: str

    def __post_init__(self):
        if self.results.total_rows > 0:
            self.exists = True
            self.rows = [r for r in self.results]
            self.latest_row = self.rows[0]
        else:
            self.exists = False


class CMIPBQInterface(BQInterface):
    """Class to read/write information from BigQuery table
    :param table_id: BigQuery table ID
    :param client: BigQuery client object
    :param result_limit: Maximum number of results to return from query
    """

    def __post_init__(self):
        # TODO how do I handle the schema? This class could be used for any table, but for
        # TODO this specific case I want to prescribe the schema
        # for now just hardcode it
        if not self.schema:
            self.schema = [
                bigquery.SchemaField('instance_id', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('store', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('timestamp', 'TIMESTAMP', mode='REQUIRED'),
                bigquery.SchemaField('retracted', 'BOOL', mode='REQUIRED'),
                bigquery.SchemaField('tests_passed', 'BOOL', mode='REQUIRED'),
            ]
        super().__post_init__()

    def _get_timestamp(self) -> str:
        """Get the current timestamp"""
        return datetime.datetime.utcnow().isoformat()

    def insert_iid(self, IID_entry):
        """Insert a row into the table for a given IID_entry object"""
        fields = {
            'instance_id': IID_entry.iid,
            'store': IID_entry.store,
            'retracted': IID_entry.retracted,
            'tests_passed': IID_entry.tests_passed,
            'timestamp': self._get_timestamp(),
        }
        self.insert(fields)

    def insert_multiple_iids(self, IID_entries: list[IIDEntry]):
        """Insert multiple rows into the table for a given list of IID_entry objects"""
        # FIXME This repeats a bunch of code from the parent class .insert() method
        timestamp = self._get_timestamp()
        rows_to_insert = [
            {
                'instance_id': IID_entry.iid,
                'store': IID_entry.store,
                'retracted': IID_entry.retracted,
                'tests_passed': IID_entry.tests_passed,
                'timestamp': timestamp,
            }
            for IID_entry in IID_entries
        ]
        errors = self.client.insert_rows_json(self._get_table(), rows_to_insert)
        if errors:
            raise RuntimeError(f'Error inserting row: {errors}')

    def _get_iid_results(self, iid: str) -> IIDResult:
        # keep this in case I ever need the row index again...
        # query = f"""
        # WITH table_with_index AS (SELECT *, ROW_NUMBER() OVER ()-1 as row_index FROM `{self.table_id}`)
        # SELECT *
        # FROM `table_with_index`
        # WHERE instance_id='{iid}'
        # """
        """Get the full result object for a given iid"""
        query = f"""
        SELECT *
        FROM `{self.table_id}`
        WHERE instance_id='{iid}'
        ORDER BY timestamp DESC
        LIMIT {self.result_limit}
        """
        results = self._get_query_job(
            query
        ).result()  # TODO: `.result()` is waiting for the query. Should I do this here?
        return IIDResult(results, iid)

    def iid_exists(self, iid: str) -> bool:
        """Check if iid exists in the table"""
        return self._get_iid_results(iid).exists

    def _iid_list_exists_batch(self, iids: list[str]) -> list[str]:
        """More efficient way to check if a list of iids exists in the table
        Passes the entire list to a single SQL query.
        Returns a list of iids that exist in the table
        ```
        """
        if len(iids) > 10000:
            raise ValueError('List of iids is too long. Please work in batches.')

        # source: https://stackoverflow.com/questions/26441928/how-do-i-check-if-multiple-values-exists-in-database
        query = f"""
        SELECT instance_id, store
        FROM {self.table_id}
        WHERE instance_id IN ({",".join([f"'{iid}'" for iid in iids])})
        """
        results = self._get_query_job(query).result()
        # this is a full row iterator, for now just return the iids
        return list(set([r['instance_id'] for r in results]))

    def iid_list_exists(self, iids: list[str]) -> list[str]:
        """More efficient way to check if a list of iids exists in the table
        Passes the entire list in batches into SQL querys for maximum efficiency.
        Returns a list of iids that exist in the table
        """

        # make batches of the input, since bq cannot handle more than 10k elements here
        iids_in_bq = []
        batchsize = 10000
        iid_batches = [iids[i : i + batchsize] for i in range(0, len(iids), batchsize)]
        for iids_batch in tqdm(iid_batches):
            iids_in_bq_batch = self._iid_list_exists_batch(iids_batch)
            iids_in_bq.extend(iids_in_bq_batch)
        return iids_in_bq


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
