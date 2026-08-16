# Writing a whole record

```python
def write_record(
    revision_id: UUID, source: Record, con: DuckDBPyConnection
) -> None: ...
```

Writes `source` as a new layer.
An existing layer directory is an error rather than an overwrite or a merge, so a whole-record write can never half-replace what a record holds.

`outputs/` is written only for a source whose [`outputs` mapping](record.md) is non-empty, so a record with no results produces a layer with no `outputs/` rather than an empty directory.

Keys are looked up one at a time and each file written before the next is built, so a lazily-building source does one read per file written rather than one per key up front.
Frames are staged into a sibling directory and renamed on success, so a frame the fold could not resolve leaves no layer rather than half of one.

Every column the schema declares a type for is cast to it on the way out, so a record's files carry the schema's types and a reader can trust them.
Without that a source may hand over an all-NULL column its dataframe library typed as float, and every reader would re-cast defensively instead.

Validation is structural: a long frame carries [its attribute's own coordinates](format.md#the-long-schema), and an entity frame carries every dim it is keyed by.
Which component types are valid belongs to [the schema's vocabulary](schema.md), which the record layer does not interpret.

An **input attribute the schema does not declare is rejected**, unlike a component type: its [`dims`](schema.md#attributespec) are what say which columns its file carries, so an undeclared one has no shape to write it in and would leave a file no reader could derive the columns of.
A **result is exempt** — [`Tool.results`](tools.md) derives which attributes count as results from a framework's own registry, so an unknown name is an error for an input and simply unknowable for a result.

A source handing over a wider frame — every declared dim, all-NULL where unused — is **narrowed to the attribute's coordinates** on the way out rather than rejected, since the extra columns carry no information the schema does not already have.

Because a [`Record`](record.md) is the input, anything satisfying the protocol can be written — including a framework object presenting itself as one, which is what puts read and write on a single seam.
