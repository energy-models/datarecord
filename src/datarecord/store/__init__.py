# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Keep a record: `Workspace`, `Store` and `Revision` write and read layers.

A `Workspace` holds the DuckDB database and opens stores and standalone
records. A `Store` holds one location with its manifest and revision tree.
A `Revision` is one node of that tree and reads back as a `Record`.

Notes
-----
- [workspaces and stores](https://energy-models.github.io/datarecord/design/storage/store/)
- [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
- [threads, processes and concurrent writers](https://energy-models.github.io/datarecord/design/storage/concurrency/)
- [writing](https://energy-models.github.io/datarecord/design/use/writing/)
- [the package](https://energy-models.github.io/datarecord/design/package/)
"""

from datarecord.store.store import Revision, Store, Workspace

__all__ = ["Revision", "Store", "Workspace"]
