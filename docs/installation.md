<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Installation

This page shows how to install datarecord for use and for development. datarecord needs Python 3.12 or later.

## Install for use

=== "pixi"

    ```bash
    pixi add --pypi datarecord
    ```

=== "uv"

    ```bash
    uv add datarecord
    ```

=== "conda"

    ```bash
    conda create -n datarecord "python>=3.12" pip
    conda activate datarecord
    pip install datarecord
    ```

=== "pip"

    ```bash
    pip install datarecord
    ```

Two extras add optional parts: `datarecord[pypsa]` adds the PyPSA tool, and `datarecord[remote]` adds records on S3.

datarecord reads its dtypes from mathspec, which is not on PyPI yet. If your installer does not install mathspec with datarecord, install it from GitHub at the commit that `rev` in [`pixi.toml`](https://github.com/energy-models/datarecord/blob/main/pixi.toml) pins:

```bash
pip install "mathspec @ git+https://github.com/energy-models/mathspec.git@<rev>"
```

## Install for development

The repository manages its environment with [pixi](https://pixi.sh), which installs mathspec at the pinned commit for you:

--8<-- "README.md:docs-install-dev"

The [contributing guide](development/contributing.md) lists the workflow and conventions.
