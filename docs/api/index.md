<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# API Reference

Generated from the docstrings. The docstrings cite the [Design](../design/index.md)
pages by link rather than restating the argument, so a symbol's "why" is one hop
away; [Usage](../usage/index.md) is the worked prose.

Everything below is importable from the top-level package:

```python
from datarecord import (
    AttributeSpec,
    Dimension,
    Directory,
    Flags,
    Frames,
    LazyFrames,
    NewChild,
    Record,
    RecordLike,
    Revision,
    Schema,
    WorkingRecord,
    connect,
    layer_dir,
    write_record,
)
```

| page                               | symbols                                                 |
| ---------------------------------- | ------------------------------------------------------- |
| [Record](record.md)                | `Record`, `RecordLike`, `Frames`, `LazyFrames`, `Flags` |
| [Schema](schema.md)                | `Schema`, `Dimension`, `AttributeSpec`                  |
| [Duck](../design/module-layout.md) | `connect`, `layer_dir`                                  |
| [Layered](layered.md)              | `Revision`, `write_record`                              |
| [WorkingRecord](mutable.md)        | `WorkingRecord`, `NewChild`, `Directory`                |
| [Sources](sources.md)              | `from_sources`, `to_sources`                            |

`from_sources` and `to_sources` are imported from `datarecord.sources`.
