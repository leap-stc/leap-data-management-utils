# What is the LEAP Data Catalog?

The LEAP Data Catalog is a centralized repository that consolidates various LEAP datasets into a single JSON file. This JSON catalog is generated from individual YAML files, each representing a dataset. The catalog is currently in development, and a draft version is available at [LEAP Data Catalog](https://catalog.leap.columbia.edu/).

## The Schema

The catalog is generated from individual YAML files. Each dataset, or feedstock, needs two files: `meta.yaml` and `catalog.yaml`. These files can be located in different locations to separate metadata curated during dataset creation (like Pangeo-Forge recipes) from catalog information that enhances the metadata for the LEAP catalog.

### meta.yaml Schema

The `meta.yaml` schema is borrowed from the [Pangeo-Forge](https://pangeo-forge.org/) project. The following fields are required:

| Field         | Type             | Description                                  |
| ------------- | ---------------- | -------------------------------------------- |
| `title`       | String           | The title of the feedstock.                  |
| `description` | String           | A brief description of the feedstock.        |
| `maintainers` | Array of Objects | Information about the dataset's maintainers. |
| `provenance`  | Object           | Information about the dataset's provenance.  |

#### Object Properties for `maintainers`

| Property | Type   | Description                       |
| -------- | ------ | --------------------------------- |
| `name`   | String | Name of the maintainer            |
| `github` | String | GitHub username of the maintainer |

#### Object Properties for `provenance`

| Property       | Type              | Description         |
| -------------- | ----------------- | ------------------- |
| `providers`    | Array of Objects  | List of providers   |
| `license`      | String            | License information |
| `license_link` | Object (optional) | License link        |

### catalog.yaml Schema

The `catalog.yaml` file contains additional information about the dataset. The following fields are required:

| Field           | Type             | Description                                      |
| --------------- | ---------------- | ------------------------------------------------ |
| `meta_yaml_url` | String           | URL to the meta YAML file.                       |
| `thumbnail`     | String           | Thumbnail of the feedstock.                      |
| `tags`          | Array of Strings | Tags associated with the feedstock.              |
| `links`         | Array of Objects | Additional links related to the feedstock.       |
| `stores`        | Array of Objects | Information about where the feedstock is stored. |

#### Object Properties for `links`

| Property | Type   | Description       |
| -------- | ------ | ----------------- |
| `label`  | String | Label of the link |
| `url`    | String | URL of the link   |

#### Object Properties for `stores`

| Property             | Type                        | Description                     |
| -------------------- | --------------------------- | ------------------------------- |
| `id`                 | String                      | ID of the store                 |
| `name`               | String (optional)           | Name of the store               |
| `url`                | String                      | URL of the store                |
| `rechunking`         | Array of Objects (optional) | Rechunking information          |
| `public`             | Boolean (optional)          | Whether the store is public     |
| `geospatial`         | Boolean (optional)          | Whether the store is geospatial |
| `xarray_open_kwargs` | Object (optional)           | Additional xarray open kwargs   |
| `last_updated`       | String (optional)           | Last updated timestamp          |

### Example YAML Files

Here's an example of a `meta.yaml` file:

```yaml
# meta.yaml
title: "LEAP Data Library Prototype"
description: >
  A prototype test for the LEAP Data Library refactor
provenance:
  providers:
    - name: "Julius"
      description: "Just a guy testing some recipes. Nothing to see here."
      roles:
        - producer
        - licensor
  license: "Just a Test"
maintainers:
  - name: "Julius Busecke"
    orcid: "0000-0001-8571-865X"
    github: jbusecke
```

Here's an example of a `catalog.yaml` file:

```yaml
# catalog.yaml
meta_yaml_url: "https://github.com/leap-stc/proto_feedstock/blob/main/feedstock/meta.yaml"
tags:
  - my-custom-tag
  - zarr
stores:
  - id: "small"
    name: "The cool small Proto Dataset"
    url: "gs://leap-scratch/data-library/feedstocks/proto_feedstock/small.zarr"
    rechunking:
      - path: "gs://some-bucket/small.zarr"
        use_case: "multiscales"
  - id: "large"
    name: "The even cooler large Proto Dataset"
    url: "gs://leap-scratch/data-library/feedstocks/proto_feedstock/large.zarr"
```

## CLI Functionality

The `leap-data-management-utils` package provides CLI functionality to validate and generate feedstock catalogs. To use the CLI, you need to have the package installed. You can install the package using the following command:

```bash
python -m pip install leap-data-management-utils[catalog]
```

### Validation

To validate the catalog files, you can use the `validate` command and provide the path to the `catalog.yaml` file as an argument:

```bash
leap-catalog validate --single path/to/catalog.yaml
```

Example:

```bash
leap-catalog validate --single https://github.com/leap-stc/proto_feedstock/blob/main/feedstock/catalog.yaml
```

Output:

```plaintext
✅ Valid feedstock: (1)
  📂 https://github.com/leap-stc/proto_feedstock/blob/main/feedstock/catalog.yaml
```

### Validation via GitHub Actions

Validation of catalog files can also be performed via GitHub Actions using the following workflow:

```yaml
# contents of .github/workflows/validate-catalog.yaml
name: leap-catalog

on:
  pull_request:
    branches:
      - main
  push:
    branches:
      - main
  workflow_dispatch:
  schedule:
    - cron: "0 0 * * *" # every day at midnight

permissions:
  contents: write
  pull-requests: write

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  single-feedstock:
    uses: leap-stc/data-catalog-actions/.github/workflows/reusable-catalog-entry.yml@main
    with:
      python-version: "3.12"
      feedstock-path: "./feedstock/catalog.yaml"
      output-directory: "./"
    secrets:
      APP_ID: ${{ secrets.APP_ID }}
      PRIVATE_KEY: ${{ secrets.PRIVATE_KEY }}
```

> [!IMPORTANT]
> Once you have the workflow file in place, you need to add the following secrets to your repository:
>
> - `APP_ID`: GitHub App ID
> - `PRIVATE_KEY`: GitHub App private key
>   These secrets are required to authenticate the GitHub Action workflow with the GitHub API and can be obtained by contacting [anderson](https://github.com/andersy005)

### How to Add a New Dataset to the LEAP Web Catalog

To add a new dataset to the LEAP web catalog, follow these steps:

1. **Create YAML Files**: Create `meta.yaml` and `catalog.yaml` files for your dataset as shown in the examples above. These can reside in a GitHub repository or any other location accessible via a URL.

> [!NOTE]
> please see [this template](https://github.com/leap-stc/LEAP_template_feedstock) repository for an example

2. **Add Dataset URL**: Add the URL of your dataset's `catalog.yaml` file to this [file](https://github.com/leap-stc/data-management/blob/main/catalog/input.yaml).
3. **Create a Pull Request**: Follow the standard GitHub workflow to create a pull request.

Once your pull request is merged, your dataset will be added to the consolidated JSON catalog, which is then rendered at [LEAP Data Catalog](https://catalog.leap.columbia.edu/).
