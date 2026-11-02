# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Keep a record: `Workspace`, `Store`, `Claim` and `Revision` write and read layers.

A `Workspace` holds the DuckDB database and opens stores and standalone
records. A `Store` holds one location, its layers each carrying its manifest.
A `Claim` is a layer pending its commit, and a `Revision` one committed node
of the tree, which reads back as a `Record`.

Notes
-----
- [workspaces and stores](https://energy-models.github.io/datarecord/design/storage/store/)
- [the record format](https://energy-models.github.io/datarecord/design/storage/format/)
- [threads, processes and concurrent writers](https://energy-models.github.io/datarecord/design/storage/concurrency/)
- [writing](https://energy-models.github.io/datarecord/design/use/writing/)
- [the package](https://energy-models.github.io/datarecord/design/package/)
"""

from datarecord.store.manifest import Manifest
from datarecord.store.store import Claim, Revision, Store, Workspace

__all__ = ["Claim", "Manifest", "Revision", "Store", "Workspace"]
