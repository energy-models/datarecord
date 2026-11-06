<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Python API

These pages render the docstring of every public name. Each name in `datarecord.__all__` imports from the top-level package:

```python
from datarecord import (
    AttributeSpec,
    Claim,
    Dimension,
    Flags,
    LazyRelations,
    Manifest,
    PropertySpec,
    Record,
    RecordLike,
    Relations,
    Revision,
    Schema,
    Selection,
    Store,
    WorkingRecord,
    Workspace,
    select,
    settle,
)
```

The tools import from their own modules, such as `from datarecord.tools.pypsa import PyPSA`.

| Page                        | Names                                                                                          |
| --------------------------- | ---------------------------------------------------------------------------------------------- |
| [Record](record.md)         | `Record`, `RecordLike`, `Relations`, `LazyRelations`, `Flags`, `select`, `Selection`, `settle` |
| [Schema](schema.md)         | `Schema`, `Dimension`, `AttributeSpec`, `PropertySpec`                                         |
| [Store](store.md)           | `Workspace`, `Store`, `Claim`, `Revision`, `Manifest`                                          |
| [WorkingRecord](mutable.md) | `WorkingRecord`                                                                                |
| [Tools](tools.md)           | `Tool`, `Requirements`, `Schema`, `Attr`, `UnsupportedRecordError`, `PyPSATool`, `PyPSA`       |
