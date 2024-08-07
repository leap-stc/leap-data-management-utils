from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from pangeo_forge_esgf.utils import CMIP6_naming_schema
from tqdm.auto import tqdm


@dataclass
class BQInterface:
    """Class to read/write information from BigQuery table
    :param table_id: BigQuery table ID
    :param client: BigQuery client object
    :param result_limit: Maximum number of results to return from query
    """

    table_id: str
    client: Optional[bigquery.client.Client] = None
    result_limit: Optional[int] = 10
    schema: Optional[list] = None

    def __post_init__(self):
        # TODO how do I handle the schema? This class could be used for any table, but for
        # TODO this specific case I want to prescribe the schema
        # for now just hardcode it
        if not self.schema:
            self.schema = [
                bigquery.SchemaField('dataset_id', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('dataset_url', 'STRING', mode='REQUIRED'),
                bigquery.SchemaField('timestamp', 'TIMESTAMP', mode='REQUIRED'),
            ]
        if self.client is None:
            self.client = bigquery.Client()

        # check if table exists, otherwise create it
        try:
            self._get_table()
        except NotFound:
            self.create_table()

    def create_table(self) -> bigquery.table.Table:
        """Create the table if it does not exist"""
        print(f'Creating {self.table_id =}')
        table = bigquery.Table(self.table_id, schema=self.schema)
        self.client.create_table(table)  # Make an API request.

    def _get_table(self) -> bigquery.table.Table:
        """Get the table object"""
        return self.client.get_table(self.table_id)

    def insert(self, fields: dict = {}):
        timestamp = datetime.now().isoformat()

        rows_to_insert = [
            fields | {'timestamp': timestamp}  # timestamp is always overridden
        ]

        errors = self.client.insert_rows_json(self._get_table(), rows_to_insert)
        if errors:
            raise RuntimeError(f'Error inserting row: {errors}')

    def catalog_insert(self, dataset_id: str, dataset_url: str, extra_fields: dict = {}):
        rows_to_insert = [
            {
                'dataset_id': dataset_id,
                'dataset_url': dataset_url,
            }
            | extra_fields
        ]
        self.insert(rows_to_insert)

    def _get_query_job(self, query: str) -> bigquery.job.query.QueryJob:
        return self.client.query(query)

    def get_all(self) -> list[bigquery.table.Row]:
        """Get all rows in the table"""
        query = f"""
        SELECT * FROM {self.table_id};
        """
        results = self._get_query_job(query)
        return results.to_dataframe()

    def get_latest(self) -> list[bigquery.table.Row]:
        """Get the latest row for all iids in the table"""
        # adopted from https://stackoverflow.com/a/1313293
        query = f"""
        WITH ranked_iids AS (
        SELECT i.*, ROW_NUMBER() OVER (PARTITION BY instance_id ORDER BY timestamp DESC) AS rn
        FROM {self.table_id} AS i
        )
        SELECT * FROM ranked_iids WHERE rn = 1;
        """
        results = self._get_query_job(query)
        return results.to_dataframe().drop(columns=['rn'])


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
        schema = CMIP6_naming_schema
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
