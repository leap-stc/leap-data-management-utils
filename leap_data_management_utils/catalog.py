import argparse
import json
import re
import traceback
import typing

import cf_xarray  # noqa: F401
import pydantic
import pydantic_core
import requests
import upath
import xarray as xr
from ruamel.yaml import YAML

yaml = YAML(typ='safe')


def s3_to_https(s3_url: str) -> str:
    # Split the URL into its components
    s3_parts = s3_url.split('/')

    # Get the bucket name from the first part of the URL
    bucket_name = s3_parts[2]

    # Join the remaining parts of the URL to form the path to the file
    path = '/'.join(s3_parts[3:])

    # Return the HTTPS URL in the desired format
    return f'https://{bucket_name}.s3.amazonaws.com/{path}'


def gs_to_https(gs_url: str) -> str:
    return gs_url.replace('gs://', 'https://storage.googleapis.com/')


class XarrayOpenKwargs(pydantic.BaseModel):
    engine: typing.Literal['zarr', 'kerchunk']


default_xarray_open_kwargs = XarrayOpenKwargs(engine='zarr')


class RechunkingItem(pydantic.BaseModel):
    path: str = pydantic.Field(..., description='Path to the rechunked store')
    use_case: str = pydantic.Field(..., description='Use case of the rechunking')


class Store(pydantic.BaseModel):
    id: str = pydantic.Field(..., description='ID of the store')
    name: str = pydantic.Field(None, description='Name of the store')
    url: str = pydantic.Field(..., description='URL of the store')
    rechunking: list[RechunkingItem] | None = pydantic.Field(None)
    public: bool | None = pydantic.Field(None, description='Whether the store is public')
    geospatial: bool | None = pydantic.Field(None, description='Whether the store is geospatial')
    xarray_open_kwargs: XarrayOpenKwargs | None = pydantic.Field(
        default_xarray_open_kwargs, description='Xarray open kwargs for the store'
    )
    last_updated: str | None = pydantic.Field(None, description='Last updated timestamp')


class Link(pydantic.BaseModel):
    label: str = pydantic.Field(..., description='Label of the link')
    url: str = pydantic.Field(..., description='URL of the link')


class LicenseLink(pydantic.BaseModel):
    title: str = pydantic.Field(..., description='Name of the license')
    url: str | None = pydantic.Field(None, description='URL of the license')


class Maintainer(pydantic.BaseModel):
    name: str = pydantic.Field(..., description='Name of the maintainer')
    github: str | None = pydantic.Field(None, description='GitHub username of the maintainer')


class Provider(pydantic.BaseModel):
    name: str = pydantic.Field(..., description='Name of the provider')
    description: str | None = pydantic.Field(None, description='Description of the provider')
    roles: list[str] | None = pydantic.Field(None, description='Roles of the provider')
    url: str | None = pydantic.Field(None, description='URL of the provider')


class Provenance(pydantic.BaseModel):
    providers: list[Provider]
    license: str
    license_link: LicenseLink | None = None


class Feedstock(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(validate_assignment=True)

    title: str = pydantic.Field(..., description='Title of the feedstock')
    description: str = pydantic.Field(..., description='Description of the feedstock')
    maintainers: list[Maintainer]
    provenance: Provenance
    thumbnail: pydantic.HttpUrl | None = pydantic.Field(
        None, description='Thumbnail of the feedstock'
    )
    tags: list[str] | None = pydantic.Field(None, description='Tags of the dataset')
    links: list[Link] | None = None
    stores: list[Store] | None = None
    meta_yaml_url: pydantic.HttpUrl | None = pydantic.Field(
        None, alias='ncviewjs:meta_yaml_url', description='URL of the meta YAML'
    )

    @classmethod
    def from_yaml(cls, path: str):
        content = yaml.load(upath.UPath(path).read_text())

        meta_url_key = next(
            (key for key in ['meta_yaml_url', 'ncviewjs:meta_yaml_url'] if key in content), None
        )

        if meta_url_key:
            meta_url = convert_to_raw_github_url(content[meta_url_key])
            meta = yaml.load(upath.UPath(meta_url).read_text())
            content = content | meta
        return cls.model_validate(content)


def convert_to_raw_github_url(github_url):
    # Check if the URL is already a raw URL
    if 'raw.githubusercontent.com' in github_url:
        return github_url

    # Replace the domain
    raw_url = github_url.replace('github.com', 'raw.githubusercontent.com')

    # Remove '/blob'
    raw_url = raw_url.replace('/blob', '')

    return raw_url


class ValidationError(Exception):
    def __init__(self, errors: list[dict[str, str]] | str) -> None:
        self.errors = errors
        super().__init__(self.errors)


def collect_feedstocks(path: upath.UPath) -> list[str]:
    """Collects all the datasets in the given directory."""

    url = convert_to_raw_github_url(path)
    if not (feedstocks := yaml.load(upath.UPath(url).read_text())['feedstocks']):
        raise FileNotFoundError(f'No YAML files (.yaml or .yml) found in {path}')
    return feedstocks


def format_report(title: str, feedstocks: list[dict], include_traceback: bool = False) -> str:
    report = f'{title} ({len(feedstocks)})\n'
    if not feedstocks:
        report += '  🚀 None found\n'
    else:
        for entry in feedstocks:
            report += f"  📂 {entry['feedstock']}\n"
            if include_traceback:
                report += f"    🔎 {entry['traceback']}\n"
    return report


def get_http_url(store: str) -> str:
    if store.startswith('s3://'):
        url = s3_to_https(store)

    elif store.startswith('gs://'):
        url = gs_to_https(store)
    else:
        url = store

    url = url.strip('/')
    return url


def is_store_public(store: str) -> bool:
    try:
        url = get_http_url(store)
        path = f'{url}/.zmetadata'

        response = requests.get(path)
        response.raise_for_status()
        return True
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f'Resource not found at {path}.')
        else:
            print(f'HTTP error {e.response.status_code} for {path}.')
        return False
    except Exception as e:
        print(f'An error occurred while checking if store {store} is public: {str(e)}')
        return False


def load_store(store: str, engine: str) -> xr.Dataset:
    url = get_http_url(store)
    return xr.open_dataset(url, engine=engine, chunks={}, decode_cf=False)


def is_geospatial(ds: xr.Dataset, is_multiscale: bool) -> bool:
    if is_multiscale:
        return 'x' in ds.dims and 'y' in ds.dims
    cf_axes = ds.cf.axes

    # Regex patterns that match 'lat', 'latitude', 'lon', 'longitude' and also allow prefixes
    lat_pattern = re.compile(r'.*(lat|latitude)$', re.IGNORECASE)
    lon_pattern = re.compile(r'.*(lon|longitude)$', re.IGNORECASE)

    # Gather all coordinate and dimension names
    all_names = set(ds.coords.keys()).union(set(ds.dims))

    # Identify if both latitude and longitude coordinates/dimensions are present
    has_latitude = any(lat_pattern.match(name) for name in all_names)
    has_longitude = any(lon_pattern.match(name) for name in all_names)

    return ('X' in cf_axes and 'Y' in cf_axes) or (has_latitude and has_longitude)


def check_stores(feed: Feedstock) -> None:
    if feed.stores:
        for index, store in enumerate(feed.stores):
            print(f'  🚦 {store.id} ({index + 1}/{len(feed.stores)})')
            check_single_store(store)


def check_single_store(store: Store) -> None:
    multiscale_path = next(
        (entry.path for entry in store.rechunking or [] if entry.use_case == 'multiscales'),
        None,
    )
    is_public = is_store_public(multiscale_path or store.url)
    store.public = is_public
    if is_public:
        # check if the store is geospatial
        if multiscale_path:
            dt = xr.open_datatree(multiscale_path, engine='zarr', chunks={}, decode_cf=False)
            ds = dt['0'].ds
            is_geospatial_store = is_geospatial(ds, True)

        else:
            ds = load_store(
                store.url,
                store.xarray_open_kwargs.engine if store.xarray_open_kwargs else 'zarr',
            )
            is_geospatial_store = is_geospatial(ds, False)
        store.geospatial = is_geospatial_store
        # get last_updated_timestamp
        store.last_updated = ds.attrs.get('pangeo_forge_build_timestamp', None)


def validate_feedstocks(*, feedstocks: list[str]) -> list[Feedstock]:
    errors = []
    valid = []
    catalog = []

    for feedstock in feedstocks:
        try:
            feed = Feedstock.from_yaml(convert_to_raw_github_url(feedstock))
            if feed.stores:
                print('🔄 Checking stores')
                check_stores(feed)

            else:
                print('🚀 No stores found.')
            valid.append({'feedstock': str(feedstock), 'status': 'valid'})
            catalog.append(feed)
        except Exception:
            errors.append({'feedstock': str(feedstock), 'traceback': traceback.format_exc()})

    valid_report = format_report('✅ Valid feedstocks:', valid)
    invalid_report = format_report('❌ Invalid feedstocks:', errors, include_traceback=True)

    print(valid_report)
    print(invalid_report)
    print('\n\n')

    if errors:
        raise ValidationError('Validation failed')

    return catalog


def validate(args):
    if args.single:
        # If single file path is provided, validate just this one feedstock
        try:
            _ = Feedstock.from_yaml(convert_to_raw_github_url(args.single))
            print(
                format_report(
                    '✅ Valid feedstock:', [{'feedstock': str(args.single), 'status': 'valid'}]
                )
            )
        except Exception:
            print(
                format_report(
                    '❌ Invalid feedstock:',
                    [{'feedstock': str(args.single), 'traceback': traceback.format_exc()}],
                    include_traceback=True,
                )
            )
    else:
        # Default behavior, processing all feedstocks from directory
        feedstocks = collect_feedstocks(args.path)
        validate_feedstocks(feedstocks=feedstocks)


def generate(args):
    feedstocks = [args.single] if args.single else collect_feedstocks(args.path)
    catalog = validate_feedstocks(feedstocks=feedstocks)
    output = upath.UPath(args.output).resolve() / 'output'
    output.mkdir(parents=True, exist_ok=True)
    path = (
        output / 'single-feedstock-web-catalog.json'
        if args.single
        else output / 'consolidated-web-catalog.json'
    )
    with open(path, 'w') as f:
        json.dump(catalog, f, indent=2, default=pydantic_core.to_jsonable_python)
        print(f'Catalog written to {path}')


def main():
    parser = argparse.ArgumentParser(description='Utilities for cataloging feedstocks for LEAP')
    subparsers = parser.add_subparsers(help='sub-command help')

    # Subparser for the "validate" command
    parser_validate = subparsers.add_parser('validate', help='Validate the feedstocks')
    group = parser_validate.add_mutually_exclusive_group(required=True)
    group.add_argument('--path', type=str, help='Path to the feedstocks input YAML file')
    group.add_argument(
        '--single', type=str, help='Path to a single feedstock YAML file to validate'
    )
    parser_validate.set_defaults(func=validate)

    # Subparser for the "generate" command
    parser_generate = subparsers.add_parser('generate', help='Generate the catalog')
    group = parser_generate.add_mutually_exclusive_group(required=True)
    group.add_argument(
        '--path', type=str, help='Path to the feedstocks input YAML file or directory'
    )
    group.add_argument('--single', type=str, help='Path to a single feedstock YAML file')
    parser_generate.add_argument(
        '--output', type=str, required=True, help='Path to the output directory'
    )
    parser_generate.set_defaults(func=generate)

    args = parser.parse_args()
    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
