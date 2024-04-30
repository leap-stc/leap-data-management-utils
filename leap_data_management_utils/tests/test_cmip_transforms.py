import pytest

from leap_data_management_utils.cmip_transforms import IIDEntry


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


# TODO Its super hard to test anything involving big query, because AFAIK there is no way to mock it.
