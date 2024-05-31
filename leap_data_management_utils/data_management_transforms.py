# Note: All of this code was written by Julius Busecke and copied from this feedstock:
# https://github.com/leap-stc/cmip6-leap-feedstock/blob/main/feedstock/recipe.py#L262

import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone

import apache_beam as beam
import zarr
from ruamel.yaml import YAML

from leap_data_management_utils.bq_interfaces import BQInterface

yaml = YAML(typ='safe')


def get_github_actions_url() -> str:
    """Return the url of the gh action run"""
    if os.getenv('GITHUB_ACTIONS') == 'true':
        print('Running from within GH actions')
        server_url = os.getenv('GITHUB_SERVER_URL')
        repository = os.getenv('GITHUB_REPOSITORY')
        run_id = os.getenv('GITHUB_RUN_ID')
        commit_hash = os.getenv('GITHUB_SHA')

        if server_url and repository and run_id and commit_hash:
            return f'{server_url}/{repository}/actions/runs/{run_id}'
        else:
            print('One or more environment variables are missing.')
            return 'none'
    else:
        return 'none'


def get_github_commit_url() -> str:
    """Get the GitHub commit URL for the current commit"""
    # Get GitHub Server URL

    # check if this is running from within a github action
    if os.getenv('GITHUB_ACTIONS') == 'true':
        print('Running from within GH actions')
        server_url = os.getenv('GITHUB_SERVER_URL')
        repository = os.getenv('GITHUB_REPOSITORY')
        run_id = os.getenv('GITHUB_RUN_ID')
        commit_hash = os.getenv('GITHUB_SHA')

        if server_url and repository and run_id and commit_hash:
            git_url_hash = f'{server_url}/{repository}/commit/{commit_hash}'
        else:
            print(
                'Could not construct git_url_hash. One or more environment variables are missing.'
            )
            git_url_hash = 'none'

    else:
        # TODO: If the above fails, maybe still try this? Even though that would be a really rare case?
        print('Fallback: Calling git via subprocess')
        github_server_url = 'https://github.com'
        # Get the repository's remote origin URL
        try:
            repo_origin_url = subprocess.check_output(
                ['git', 'config', '--get', 'remote.origin.url'], text=True
            ).strip()

            # Extract the repository path from the remote URL
            repository_path = repo_origin_url.split('github.com/')[-1].replace('.git', '')

            # Get the current commit SHA
            commit_sha = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()

            # Construct the GitHub commit URL
            git_url_hash = f'{github_server_url}/{repository_path}/commit/{commit_sha}'
        except Exception as e:
            print(f'Getting git_url_hash failed with {e}')
            git_url_hash = 'none'
    # Output the GitHub commit URL
    return git_url_hash


def get_catalog_store_urls(catalog_yaml_path: str) -> dict[str, str]:
    with open(catalog_yaml_path) as f:
        catalog_meta = yaml.load(f)
    return {d['id']: d['url'] for d in catalog_meta['stores']}


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
    """Copy a store to a new location. If the target input is False, do nothing."""

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
    inject_attrs: dict = None
    add_provenance: bool = True

    # add a post_init method to add the provenance attributes
    def __post_init__(self):
        if self.inject_attrs is None:
            self.inject_attrs = {}

        if self.add_provenance:
            git_url_hash = get_github_commit_url()
            gh_actions_url = get_github_actions_url()
            timestamp = datetime.now(timezone.utc).isoformat()
            provenance_dict = {
                'pangeo_forge_build_git_hash': git_url_hash,
                'pangeo_forge_gh_actions_url': gh_actions_url,
                'pangeo_forge_build_timestamp': timestamp,
            }
            self.inject_attrs.update(provenance_dict)

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
