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
    if verbose:
        print('Testing - Time Dimension')
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
    iid_schema = 'mip_era.activity_id.institution_id.source_id.experiment_id.variant_label.table_id.variable_id.grid_label.version'
    for facet_value, facet in zip(iid.split('.'), iid_schema.split('.')):
        if 'version' not in facet:  # (TODO: Why is the version not in all datasets?)
            if verbose:
                print(f'Checking {facet = } in dataset attributes')
            assert ds.attrs[facet] == facet_value


def test_all(store: zarr.storage.FSStore, iid: str, verbose=True) -> zarr.storage.FSStore:
    ds = test_open_store(store, verbose=verbose)
    test_time(ds, verbose=verbose)
    test_attributes(ds, iid, verbose=verbose)
    return store
