# Note: All of this code was written by Julius Busecke and copied from this feedstock:
# https://github.com/leap-stc/cmip6-leap-feedstock/blob/main/feedstock/recipe.py#L262

import datetime
from dataclasses import dataclass
from typing import Optional

import apache_beam as beam
import zarr
from google.api_core.exceptions import NotFound
from google.cloud import bigquery


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
        timestamp = datetime.datetime.now().isoformat()

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


# ----------------------------------------------------------------------------------------------
# apache Beam stages
# ----------------------------------------------------------------------------------------------


@dataclass
class RegisterDatasetToCatalog(beam.PTransform):
    table_id: str
    dataset_id: str

    def _register_dataset_to_catalog(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        bq_interface = BQInterface(table_id=self.table_id)
        bq_interface.catalog_insert(dataset_id=self.dataset_id, dataset_url=store.path)
        return store

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._register_dataset_to_catalog)


@dataclass
class Copy(beam.PTransform):
    target: str

    def _copy(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        import os

        import gcsfs
        import zarr

        # We do need the gs:// prefix?
        # TODO: Determine this dynamically from zarr.storage.FSStore
        source = f'gs://{os.path.normpath(store.path)}/'  # FIXME more elegant. `.copytree` needs trailing slash
        if self.target is False:
            # dont do anything
            return store
        else:
            fs = gcsfs.GCSFileSystem()  # FIXME: How can we generalize this?
            fs.cp(source, self.target, recursive=True)
            # return a new store with the new path that behaves exactly like the input
            # to this stage (so we can slot this stage right before testing/logging stages)
            return zarr.storage.FSStore(self.target)

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | 'Copying Store' >> beam.Map(self._copy)


@dataclass
class InjectAttrs(beam.PTransform):
    inject_attrs: dict

    def _update_zarr_attrs(self, store: zarr.storage.FSStore) -> zarr.storage.FSStore:
        # TODO: Can we get a warning here if the store does not exist?
        attrs = zarr.open(store, mode='a').attrs
        attrs.update(self.inject_attrs)
        # ? Should we consolidate here? We are explicitly doing that later...
        return store

    def expand(
        self, pcoll: beam.PCollection[zarr.storage.FSStore]
    ) -> beam.PCollection[zarr.storage.FSStore]:
        return pcoll | 'Injecting Attributes' >> beam.Map(self._update_zarr_attrs)
