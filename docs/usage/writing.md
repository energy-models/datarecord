<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Writing a whole record

```python
from datarecord import write_record

write_record(revision.id, source, con)  # as a revision's layer
write_record(None, source, con, uri="out/")  # as a standalone directory
```

`source` is anything satisfying `Record` — including a framework object presenting itself as one, which is what puts read and write on a single seam. An existing layer directory is an error rather than an overwrite: frames are staged into a sibling path and renamed on success, so a source that fails validation leaves no layer rather than half of one ([design](../design/writing.md)).

Every column the schema declares a type for is cast to it on the way out, so a record's files carry the schema's types and a reader can trust them. Validation is structural — a long frame carries [the format's columns](../design/format.md#the-long-schema), and an entity frame carries every dim it is keyed by.

## Adding data to a tool's record

`combine` lays an extension over a record, for data that the tool's schema does not hold ([design](../design/writing.md#combining-records)):

```python
from datarecord import FrameRecord, combine

extension = FrameRecord(
    schema, dim_frames={"limit": limits}, attribute_frames={"limit_constant": constants}
)
write_record(None, combine(PyPSA.to_datarecord(n), extension), con, uri="out/")
```

Every name the extension declares is new, and the dims both sides declare are declared the same.
