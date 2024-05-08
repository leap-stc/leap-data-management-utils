import numpy as np
import pytest
import xarray as xr

from leap_data_management_utils.cmip_transforms import IIDEntry, dynamic_chunking_func


class TestIIDEntry:
    def test_IIDEntry(self):
        iid = 'CMIP6.CMIP.NCAR.CESM2.historical.r10i1p1f1.Amon.tas.gr1.v20190308'
        store = 'gs://cmip6/CMIP6/CMIP/NCAR/CESM2/historical/r10i1p1f1/Amon/tas/gr1/v20190308'
        retracted = False
        tests_passed = True
        entry = IIDEntry(iid, store, retracted, tests_passed)
        assert entry.iid == iid
        assert entry.store == store
        assert entry.retracted == retracted
        assert entry.tests_passed == tests_passed

    def test_too_long(self):
        iid = 'CMIP6.CMIP.NCAR.CESM2.historical.r10i1p1f1.Amon.tas.gr1.v20190308.extra'
        store = 'gs://cmip6/CMIP6/CMIP/NCAR/CESM2/historical/r10i1p1f1/Amon/tas/gr1/v20190308'
        retracted = False
        tests_passed = True
        with pytest.raises(ValueError):
            IIDEntry(iid, store, retracted, tests_passed)


class TestDynamicChunks:
    def test_too_small(self):
        ds = xr.DataArray(np.random.rand(4, 6)).to_dataset(name='data')
        chunks = dynamic_chunking_func(ds)
        assert chunks == {'dim_0': 4, 'dim_1': 6}


# TODO Its super hard to test anything involving big query, because AFAIK there is no way to mock it.
