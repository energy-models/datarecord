# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""Where a record lives: the workspace, a store and its revision tree, and the writer.

Notes
-----
- [the store](https://energy-models.github.io/datarecord/design/layers/#the-store)
- [module layout](https://energy-models.github.io/datarecord/design/module-layout/)
"""

from datarecord.store.store import Revision, Store, Workspace

__all__ = ["Revision", "Store", "Workspace"]
