import argparse
import json
import traceback

import pydantic
import pydantic_core
import upath
import yaml


class Store(pydantic.BaseModel):
    id: str = pydantic.Field(..., description='ID of the store')
    name: str = pydantic.Field(None, description='Name of the store')
    url: str = pydantic.Field(..., description='URL of the store')
    rechunking: list[dict[str, str]] | None = pydantic.Field(None, alias='ncviewjs:rechunking')


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
    meta_yaml_url: pydantic.HttpUrl | None = pydantic.Field(None, alias='ncviewjs:meta_yaml_url')

    @classmethod
    def from_yaml(cls, path: str):
        content = yaml.safe_load(upath.UPath(path).read_text())
        if 'ncviewjs:meta_yaml_url' in content:
            meta_url = convert_to_raw_github_url(content['ncviewjs:meta_yaml_url'])
            meta = yaml.safe_load(upath.UPath(meta_url).read_text())
            content = content | meta
        data = cls.model_validate(content)
        return data


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


def collect_feedstocks(path: upath.UPath) -> list[upath.UPath]:
    """Collects all the datasets in the given directory."""

    url = convert_to_raw_github_url(path)
    if not (feedstocks := yaml.safe_load(upath.UPath(url).read_text())['feedstocks']):
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


def validate_feedstocks(*, feedstocks: list[upath.UPath]) -> list[Feedstock]:
    errors = []
    valid = []
    catalog = []

    for feedstock in feedstocks:
        try:
            feed = Feedstock.from_yaml(convert_to_raw_github_url(feedstock))
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
    feedstocks = collect_feedstocks(args.path)
    catalog = validate_feedstocks(feedstocks=feedstocks)
    output = upath.UPath(args.output).resolve() / 'output'
    output.mkdir(parents=True, exist_ok=True)
    with open(f'{output}/consolidated-web-catalog.json', 'w') as f:
        json.dump(catalog, f, indent=2, default=pydantic_core.to_jsonable_python)
        print(f'Catalog written to {output}/consolidated-web-catalog.json')


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
    parser_generate.add_argument(
        '--path', type=str, required=True, help='Path to the feedstocks input YAML file'
    )
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
