# What is the LEAP Data Catalog?

The LEAP Data Catalog is a centralized repository that consolidates various LEAP datasets into a single JSON file. This JSON catalog is generated from individual YAML files, each representing a dataset. The catalog is currently in development, and a draft version is available at [LEAP Data Catalog](https://leap-data-catalog.vercel.app/).

## The Schema

The catalog is generated from individual YAML files. Each dataset, or feedstock, needs two files: `meta.yaml` and `catalog.yaml`. These files can be located in different locations to separate metadata curated during dataset creation (like Pangeo-Forge recipes) from catalog information that enhances the metadata for the LEAP catalog.

### meta.yaml Schema

The `meta.yaml` schema is borrowed from the [Pangeo-Forge](https://pangeo-forge.org/) project. The following fields are required:

| Field         | Type             | Description                                  | Object Properties                                                                                                                                                 |
| ------------- | ---------------- | -------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `title`       | String           | The title of the feedstock.                  |                                                                                                                                                                   |
| `description` | String           | A brief description of the feedstock.        |                                                                                                                                                                   |
| `maintainers` | Array of Objects | Information about the dataset's maintainers. | `name`: Name of the maintainer (Type: String)<br>`github`: GitHub username of the maintainer (Type: String)                                                       |
| `provenance`  | Object           | Information about the dataset's provenance.  | `providers`: List of providers (Type: Array of Objects)<br>`license`: License information (Type: String)<br>`license_link`: License link (Type: Object, optional) |

### catalog.yaml Schema

The `catalog.yaml` file contains additional information about the dataset. The following fields are required:

| Field           | Type             | Description                                      | Object Properties                                                                                                                                                                                                                                                                                                                                             |
| --------------- | ---------------- | ------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `meta_yaml_url` | String           | URL to the meta YAML file.                       |                                                                                                                                                                                                                                                                                                                                                               |
| `thumbnail`     | String           | Thumbnail of the feedstock.                      |                                                                                                                                                                                                                                                                                                                                                               |
| `tags`          | Array of Strings | Tags associated with the feedstock.              |                                                                                                                                                                                                                                                                                                                                                               |
| `links`         | Array of Objects | Additional links related to the feedstock.       | `label`: Label of the link (Type: String)<br>`url`: URL of the link (Type: String)                                                                                                                                                                                                                                                                            |
| `stores`        | Array of Objects | Information about where the feedstock is stored. | `id`: ID of the store (Type: String)<br>`name`: Name of the store (Type: String, optional)<br>`url`: URL of the store (Type: String)<br>`rechunking`: Rechunking information (Type: Array of Objects, optional)<br>`public`: Whether the store is public (Type: Boolean, optional)<br>`geospatial`: Whether the store is geospatial (Type: Boolean, optional) |

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

### How to Add a New Dataset to the LEAP Web Catalog

To add a new dataset to the LEAP web catalog, follow these steps:

1. **Create YAML Files**: Create `meta.yaml` and `catalog.yaml` files for your dataset as shown in the examples above. These can reside in a GitHub repository or any other location accessible via a URL.
2. **Add Dataset URL**: Add the URL of your dataset's `catalog.yaml` file to this [file](https://github.com/leap-stc/data-management/blob/main/catalog/input.yaml).
3. **Create a Pull Request**: Follow the standard GitHub workflow to create a pull request.

Once your pull request is merged, your dataset will be added to the consolidated JSON catalog, which is then rendered at [LEAP Data Catalog](https://leap-data-catalog.vercel.app/).
