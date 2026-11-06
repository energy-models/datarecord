<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Installation

This page shows how to install datarecord for use and for development. datarecord needs Python 3.12 or later.

## Install for use

=== "pixi"

    ```bash
    pixi add --pypi "datarecord @ git+https://github.com/energy-models/datarecord"
    ```

=== "uv"

    ```bash
    uv add "datarecord @ git+https://github.com/energy-models/datarecord"
    ```

=== "conda"

    ```bash
    conda create -n datarecord "python>=3.12" pip
    conda activate datarecord
    pip install "datarecord @ git+https://github.com/energy-models/datarecord"
    ```

=== "pip"

    ```bash
    pip install "datarecord @ git+https://github.com/energy-models/datarecord"
    ```

Two extras add optional parts. Write `datarecord[pypsa]` in place of `datarecord` for the PyPSA tool, and `datarecord[remote]` for records on S3.

Each form installs from GitHub, because the release on PyPI, `0.1.0a1`, predates the design these pages describe. The install brings `mathspec` with it, at the commit that `pyproject.toml` pins, so there is nothing to install beside it.

## Install for development

The repository manages its environment with [pixi](https://pixi.sh), which installs mathspec at the pinned commit for you:

--8<-- "README.md:docs-install-dev"

The [contributing guide](development/contributing.md) lists the workflow and conventions.
