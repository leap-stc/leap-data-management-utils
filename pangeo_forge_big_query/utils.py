# Note: All of this code was written by Julius Busecke and copied from this feedstock:
# https://github.com/leap-stc/cmip6-leap-feedstock/blob/main/feedstock/recipe.py#L262

from google.cloud import bigquery
from typing import Optional, List
from google.api_core.exceptions import NotFound
import apache_beam as beam
import zarr
import datetime
from dataclasses import dataclass


@dataclass
class IIDEntry:
    """Single row/entry for an iid
    :param iid: CMIP6 instance id
    :param store: URL to zarr store
    """

    iid: str
    store: str  # TODO: should this allow other objects?

    # Check if the iid conforms to a schema
    def __post_init__(self):
        schema = "mip_era.activity_id.institution_id.source_id.experiment_id.member_id.table_id.variable_id.grid_label.version"
        facets = self.iid.split(".")
        if len(facets) != len(schema.split(".")):
            raise ValueError(
                f"IID does not conform to CMIP6 {schema =}. Got {self.iid =}"
            )
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
                bigquery.SchemaField("instance_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("store", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
            ]
        if self.client is None:
            self.client = bigquery.Client()

        # check if table exists, otherwise create it
        try:
            self.table = self.client.get_table(self.table_id)
        except NotFound:
            self.table = self.create_table()

    def create_table(self) -> bigquery.table.Table:
        """Create the table if it does not exist"""
        print(f"Creating {self.table_id =}")
        table = bigquery.Table(self.table_id, schema=self.schema)
        table = self.client.create_table(table)  # Make an API request.
        return table

    def catalog_insert(self, dataset_id: str, dataset_url: str):
        timestamp = datetime.datetime.now().isoformat()
        table = self.client.get_table(self.table_id)

        rows_to_insert = [
            {
                "dataset_id": dataset_id,
                "dataset_url": dataset_url,
                "timestamp": timestamp,
            }
        ]

        errors = self.client.insert_rows_json(table, rows_to_insert)
        if errors:
            raise RuntimeError(f"Error inserting row: {errors}")

    def insert(self, IID_entry):
        """Insert a row into the table for a given IID_entry object"""
        # Generate a timestamp to add to a bigquery row
        timestamp = datetime.datetime.now().isoformat()
        json_row = {
            "instance_id": IID_entry.iid,
            "store": IID_entry.store,
            "timestamp": timestamp,
        }
        errors = self.client.insert_rows_json(self.table_id, [json_row])
        if errors:
            raise RuntimeError(f"Error inserting row: {errors}")

    def _get_query_job(self, query: str) -> bigquery.job.query.QueryJob:
        """Get result object corresponding to a given iid"""
        # keep this in case I ever need the row index again...
        # query = f"""
        # WITH table_with_index AS (SELECT *, ROW_NUMBER() OVER ()-1 as row_index FROM `{self.table_id}`)
        # SELECT *
        # FROM `table_with_index`
        # WHERE instance_id='{iid}'
        # """
        return self.client.query(query)

    def _get_iid_results(self, iid: str) -> IIDResult:
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

    def iid_list_exists(self, iids: List[str]) -> List[str]:
        """More efficient way to check if a list of iids exists in the table
        Passes the entire list to a single SQL query.
        Returns a list of iids that exist in the table"""
        # source: https://stackoverflow.com/questions/26441928/how-do-i-check-if-multiple-values-exists-in-database
        query = f"""
        SELECT instance_id, store
        FROM {self.table_id}
        WHERE instance_id IN ({",".join([f"'{iid}'" for iid in iids])})
        """
        results = self._get_query_job(query).result()
        # this is a full row iterator, for now just return the iids
        return list(set([r["instance_id"] for r in results]))


# wrapper functions (not sure if this works instead of the repeated copy and paste in the transform below)
def log_to_bq(iid: str, store: zarr.storage.FSStore, table_id: str):
    bq_interface = BQInterface(table_id=table_id)
    iid_entry = IIDEntry(iid=iid, store=store.path)
    bq_interface.insert(iid_entry)


@dataclass
class LogToBigQuery(beam.PTransform):
    """
    Logging stage for data written to zarr store
    """

    iid: str
    table_id: str

    def _log_to_bigquery(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        log_to_bq(self.iid, store, self.table_id)
        return store

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._log_to_bigquery)


@dataclass
class RegisterDatasetToCatalog(beam.PTransform):
    table_id: str
    dataset_id: str

    def _register_dataset_to_catalog(
        self, store: zarr.storage.FSStore
    ) -> zarr.storage.FSStore:
        bq_interface = BQInterface(table_id=self.table_id)
        bq_interface.catalog_insert(dataset_id=self.dataset_id, dataset_url=store.path)
        return store

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._register_dataset_to_catalog)
