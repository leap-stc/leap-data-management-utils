from leap_data_management_utils.data_management_transforms import get_catalog_store_urls, get_github_commit_url
import pytest
from ruamel.yaml import YAML

yaml = YAML(typ="safe")


def test_smoke_test():
    assert True
    # This is a bit dumb, but it at least checks the the imports are working
    # again super hard to test code involving bigquery here.


def test_get_github_commit_url():

    url = get_github_commit_url()
    assert url.startswith("https://github.com/leap-stc/leap-data-management-utils")

def test_get_catalog_store_urls(tmp_path):

    # Create a temporary text file
    temp_file = tmp_path / "some-name.yaml"
    stores = [{'id':'a', 'url':'a-url', 'some_other':'stuff'}, {'id':'b', 'url':'b-url'}]
    with open(temp_file, 'w') as f:
        yaml.dump(stores, f)

    # Call the function to read the file
    content = get_catalog_store_urls(temp_file)

    # Assertions
    assert content['a'] == 'a-url'
    assert content['b'] == 'b-url'