# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Read a record: `Record` folds the layers of a revision or a directory into relations.

A `Record` answers dims, properties and attributes as lazy DuckDB
relations, however many layers it holds.

Notes
-----
- [resolution](https://energy-models.github.io/datarecord/design/layers/resolution/)
- [reading a record](https://energy-models.github.io/datarecord/design/use/reading/)
- [the package](https://energy-models.github.io/datarecord/design/package/)
"""

from datarecord.resolve.record import Record

__all__ = ["Record"]
