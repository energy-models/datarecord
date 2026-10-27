# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Declare a record: `Schema` and the specs it holds.

A `Schema` declares the dims, properties and attributes a record may
hold, and loads from and dumps to a record's `manifest.json`.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/model/schema/)
- [dims and labels](https://energy-models.github.io/datarecord/design/model/dims/)
- [properties and categories](https://energy-models.github.io/datarecord/design/model/properties/)
- [multi-dims](https://energy-models.github.io/datarecord/design/model/multi-dims/)
- [attributes](https://energy-models.github.io/datarecord/design/model/attributes/)
- [the package](https://energy-models.github.io/datarecord/design/package/)
"""

from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, PropertySpec

__all__ = ["AttributeSpec", "Dimension", "PropertySpec", "Schema"]
