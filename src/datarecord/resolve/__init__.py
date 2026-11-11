# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Read a record: `Record` folds the layers of a revision or a directory into relations.

A `Record` answers dimensions, properties and attributes as lazy DuckDB
relations, however many layers it holds.

Notes
-----
- [resolution](https://energy-models.github.io/datarecord/reference/resolution/)
- [reading a record](https://energy-models.github.io/datarecord/reference/record/)
- [the package](https://energy-models.github.io/datarecord/development/package/)
"""

from datarecord.resolve.record import Record

__all__ = ["Record"]
