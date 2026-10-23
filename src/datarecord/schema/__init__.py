# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""The schema of a record and the declarations it is made of.

Notes
-----
- [the schema](https://energy-models.github.io/datarecord/design/schema/)
- [module layout](https://energy-models.github.io/datarecord/design/module-layout/)
"""

from datarecord.schema.schema import Schema
from datarecord.schema.specs import AttributeSpec, Dimension, Group, PropertySpec

__all__ = ["AttributeSpec", "Dimension", "Group", "PropertySpec", "Schema"]
