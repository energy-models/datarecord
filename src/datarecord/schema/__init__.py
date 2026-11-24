# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Declare a record: `Schema` and the specs it holds.

A `Schema` declares the dimensions, properties and attributes a record may
hold, and loads from and dumps to a layer's `manifest.json`.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/reference/schema/)
- [dimensions and labels](https://energy-models.github.io/datarecord/reference/data-model/)
- [properties and categories](https://energy-models.github.io/datarecord/reference/data-model/#properties)
- [multi-dimensions](https://energy-models.github.io/datarecord/reference/data-model/#multi-dimensions)
- [attributes](https://energy-models.github.io/datarecord/reference/data-model/#attributes)
- [the package](https://energy-models.github.io/datarecord/development/package/)
"""

from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, PropertySpec

__all__ = ["AttributeSpec", "Dimension", "PropertySpec", "Schema"]
