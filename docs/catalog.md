# What is the LEAP Data Catalog?

The LEAP Data Catalog is a centralized repository that consolidates various LEAP datasets into a single JSON file. This JSON catalog is generated from individual YAML files, each representing a dataset. This catalog is currently in development and a draft version is available at <https://leap-data-catalog.vercel.app/>.

## The Schema

The catalog is generated from individual YAML files. Each feedstock/dataset needs to have two files: a `meta.yaml` and `catalog.yaml` files. These two files don't have to be located in the same location. The primary reason for this is to allow for the separation of metadata curated as part of a dataset's creation process such as pangeo-forge recipes and the catalog information that extends the metadata to make it useful for LEAP catalog.

1. The `meta.yaml` schema is borrowed from the [Pangeo-Forge]() project. The following fields are required in the `meta.yaml` file:

| Field         | Type             | Description                                  | Object Properties                                                                                                                                                 |
| ------------- | ---------------- | -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `title`       | String           | The title of the feedstock.                  |                                                                                                                                                                   |
| `description` | String           | A brief description of the feedstock.        |                                                                                                                                                                   |
| `maintainers` | Array of Objects | Information about the dataset's maintainers. | `name`: Name of the maintainer (Type: String)<br>`github`: GitHub username of the maintainer (Type: String)                                                       |
| `provenance`  | Object           | Information about the dataset's provenance.  | `providers`: List of providers (Type: Array of Objects)<br>`license`: License information (Type: String)<br>`license_link`: License link (Type: Object, optional) |

2. The `catalog.yaml` contains additional information about the dataset. The following fields are required in the `catalog.yaml` file:

| Field           | Type             | Description                                      | Object Properties                                                                                                                                                                                                   |
| --------------- | ---------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `thumbnail`     | String           | Thumbnail of the feedstock.                      |                                                                                                                                                                                                                     |
| `tags`          | Array of Strings | Tags associated with the feedstock.              |                                                                                                                                                                                                                     |
| `links`         | Array of Objects | Additional links related to the feedstock.       | `label`: Label of the link (Type: String)<br>`url`: URL of the link (Type: String)                                                                                                                                  |
| `stores`        | Array of Objects | Information about where the feedstock is stored. | `id`: ID of the store (Type: String)<br>`name`: Name of the store (Type: String, optional)<br>`url`: URL of the store (Type: String)<br>`rechunking`: Rechunking information (Type: Array of Objects, optional)<br> |
| `meta_yaml_url` | String           | URL to the meta YAML file.                       |

### Example YAML Files

Here's an example of a `meta.yaml` file:

```yaml
# meta.yaml
title: "LEAP Data Library Prototype"
description: >
  A prototype test for the LEAP Data Library refactor
recipes:
  - id: "small"
    object: "recipe:small"
  - id: "large"
    object: "recipe:large"
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

and a `catalog.yaml`

```yaml
# catalog.yaml
# All the information important to cataloging.
"meta_yaml_url": "https://github.com/leap-stc/proto_feedstock/blob/main/feedstock/meta.yaml"
tags:
  - my-custom-tag
  - zarr
stores:
  - id: "small"
    name: "The cool small Proto Dataset"
    url: "gs://leap-scratch/data-library/feedstocks/proto_feedstock/small.zarr"
    "rechunking":
      - path: "gs://some-bucket/small.zarr"
        use_case: "multiscales"

  - id: "large"
    name: "The even cooler large Proto Dataset" # no pyramids
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

for example,

```bash
❯ leap-catalog validate --single https://github.com/leap-stc/proto_feedstock/blob/main/feedstock/catalog.yaml
✅ Valid feedstock: (1)
  📂 https://github.com/leap-stc/proto_feedstock/blob/main/feedstock/catalog.yaml

```

## How to Add a New Dataset to the LEAP Web Catalog

To add a new dataset to the LEAP web catalog, you need to add the URL of the dataset's `catalog.yaml` file to this [file](https://github.com/leap-stc/data-management/blob/main/catalog/input.yaml) and then follow the standard GitHub workflow to create a pull request

Once your pull request is merged, your dataset will be added to the consolidated JSON catalog, which is then rendered at <https://leap-data-catalog.vercel.app/>.
