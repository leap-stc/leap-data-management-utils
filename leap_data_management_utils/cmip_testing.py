"""
This module contains functions for testing CMIP6 zarr stores.
Its a bit awkward to put them here, but I want to use these functions both in
the apache-beam pipelines and in scripts that run occasionally to re-check all
stores. Due to the way that pangeo-forge works right now, we cannot separate
these in modules in https://github.com/leap-stc/cmip6-leap-feedstock
"""

# ---------------------------------------------------------------------------------------------------------------------------------------------------------
# Test functions (I will need to use these outside too)
# --------------------------------------------------------------------------------------------------------------------------------------------------------
import xarray as xr
import zarr
from pangeo_forge_esgf.utils import CMIP6_naming_schema


def open_dataset(store: zarr.storage.FSStore) -> xr.Dataset:
    return


def test_open_store(store: zarr.storage.FSStore, verbose):
    if verbose:
        print('Testing - Open Store')
        print(f'Written path: {store.path =}')
    ds = xr.open_dataset(
        store.fs.get_mapper(store.path),
        engine='zarr',
        chunks={},
        use_cftime=True,
        consolidated=True,
    )
    if verbose:
        print(ds)
    return ds


def test_time(ds: xr.Dataset, verbose):
    """
    Check time dimension
    For now checks:
    - That time increases strictly monotonically
    - That no large gaps in time (e.g. missing file) are present
    """
    if 'time' not in ds.dims:
        if verbose:
            print('No time dimension found')
    else:
        if verbose:
            print('Testing - Time Dimension')

        # check that the time range is the same as
        # indicated in the ESGF API dataset response

        time_diff = ds.time.diff('time').astype(int)
        # assert that time increases monotonically
        if verbose:
            print(time_diff)
        assert (time_diff > 0).all()

        # assert that there are no large time gaps
        mean_time_diff = time_diff.mean()
        normalized_time_diff = abs((time_diff - mean_time_diff) / mean_time_diff)
        assert (normalized_time_diff < 0.05).all()


def test_attributes(ds: xr.Dataset, iid: str, verbose):
    if verbose:
        print('Testing - Attributes')

    # check completeness of attributes
    iid_schema = CMIP6_naming_schema
    # The cmip datasets do not all have 'member_id' as an attribute.
    # Member id is composed of two (I think) required attributes as:
    # <sub_experiment_id>-<variant_label>
    # See https://docs.google.com/document/d/1h0r8RZr_f3-8egBMMh7aqLwy3snpD6_MrDz1q8n5XUk/edit for more info
    # Construct a dict of schema and values extracted from the dataset attributes
    facets_from_iid = {facet: iid_facet for facet, iid_facet in zip(iid_schema.split('.'), iid.split('.'))}
    # detect member_ids that have a sub_experiment_id
    member_id = facets_from_iid.pop('member_id')
    if '-' in member_id:
        facets_from_iid['sub_experiment_id'], facets_from_iid['variant_label'] = member_id.split('-', 1)
    else:
        facets_from_iid['variant_label'] = member_id
    
    ignore_fields = [
        'version', # (TODO: Why is the version not in all datasets?)
    ]
    
    for facet, facet_value in facets_from_iid.items():
        if facet not in ignore_fields:  # (TODO: Why is the version not in all datasets?)
            if verbose:
                print(f'Checking {facet = } in dataset attributes')
            actual_value = ds.attrs.get(facet)
            if actual_value != facet_value:
                print(f"{iid =}")
                raise AssertionError(
                    f"Attribute mismatch for facet '{facet}': expected '{facet_value}', got '{actual_value}'"
                )
   
    # check that the esgf api response is stored in the dataset attributes
    assert 'pangeo_forge_api_responses' in ds.attrs
    assert 'dataset' in ds.attrs['pangeo_forge_api_responses']
    assert 'files' in ds.attrs['pangeo_forge_api_responses']
    assert len(ds.attrs['pangeo_forge_api_responses']['files']) >= 1


def test_all(store: zarr.storage.FSStore, iid: str, verbose=True) -> zarr.storage.FSStore:
    ds = test_open_store(store, verbose=verbose)
    # attributes need to be tested before because
    # time tests depend on newly added attributes
    test_attributes(ds, iid, verbose=verbose)
    test_time(ds, verbose=verbose)
    return store