import pandas as pd

from leap_data_management_utils.cmip_catalog import bq_df_to_intake_esm


def test_bq_df_to_intake_esm():
    bq_df = pd.DataFrame(
        {
            'instance_id': [
                'CMIP6.AerChemMIP.MIROC.MIROC6.piClim-NTCF.sub-r1i1p1f1.Amon.tasmin.gn.v20190807',  # modified to test the sub_experiment split
                'CMIP6.AerChemMIP.MIROC.MIROC6.piClim-OC.r1i1p1f1.Amon.rlut.gn.v20190807',
            ],
            'store': [
                'gs://cmip6/CMIP6/AerChemMIP/MIROC/MIROC6/piClim-NTCF/r1i1p1f1/Amon/tasmin/gn/v20190807/',
                'gs://cmip6/CMIP6/AerChemMIP/MIROC/MIROC6/piClim-OC/r1i1p1f1/Amon/rlut/gn/v20190807/',
            ],
            'retracted': [False, False],
            'tests_passed': [True, True],
        }
    )
    intake_df = bq_df_to_intake_esm(bq_df)
    for c in intake_df.columns:
        print(c)
        print(intake_df[c].to_list())

    expected_intake_df = pd.DataFrame(
        {
            'activity_id': ['AerChemMIP', 'AerChemMIP'],
            'institution_id': ['MIROC', 'MIROC'],
            'source_id': ['MIROC6', 'MIROC6'],
            'experiment_id': ['piClim-NTCF', 'piClim-OC'],
            'member_id': ['sub-r1i1p1f1', 'r1i1p1f1'],
            'table_id': ['Amon', 'Amon'],
            'variable_id': ['tasmin', 'rlut'],
            'grid_label': ['gn', 'gn'],
            'sub_experiment_id': ['sub', 'none'],
            'variant_label': ['r1i1p1f1', 'r1i1p1f1'],
            'version': ['v20190807', 'v20190807'],
            'zstore': [
                'gs://cmip6/CMIP6/AerChemMIP/MIROC/MIROC6/piClim-NTCF/r1i1p1f1/Amon/tasmin/gn/v20190807/',
                'gs://cmip6/CMIP6/AerChemMIP/MIROC/MIROC6/piClim-OC/r1i1p1f1/Amon/rlut/gn/v20190807/',
            ],
        }
    )
    assert intake_df.equals(expected_intake_df)
