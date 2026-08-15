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
    DirectoryRecord,
    Flags,
    Frames,
    LayeredRecord,
    LazyFrames,
    NewChild,
    Pending,
    Record,
    Revision,
    Schema,
    WorkingRecord,
    connect,
    layer_dir,
    write_record,
)
```

| page | symbols |
| --- | --- |
| [Record](record.md) | `Record`, `Frames`, `LazyFrames`, `Flags` |
| [Schema](schema.md) | `Schema`, `Dimension`, `AttributeSpec` |
| [Directory](directory.md) | `DirectoryRecord`, `connect`, `layer_dir` |
| [Layered](layered.md) | `Revision`, `LayeredRecord`, `write_record` |
| [WorkingRecord](mutable.md) | `WorkingRecord`, `Pending`, `NewChild`, `Directory` |
| [Tools](tools.md) | `Tool`, `Requirements`, the PyPSA tool |
