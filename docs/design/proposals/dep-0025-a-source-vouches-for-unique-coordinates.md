<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# DEP 25: a source vouches for unique coordinates

| DEP     | 25                                      |
| ------- | --------------------------------------- |
| Title   | a source vouches for unique coordinates |
| Author  | Jonas Hoersch                           |
| Status  | Implemented (2026-10-28)                |
| Created | 2026-10-28                              |
| Version | Unreleased                              |

## Abstract

`write_layer` refuses an attribute file holding two rows that name the same dims at one coordinate, and finds them by hashing every row of the file. A source that can prove no such pair exists says so through a protocol of its own, `UniqueCoordinates`, with one member, `is_unique(attribute) -> bool`, and the writer skips that one check where it is told True. The PyPSA tool answers True where every pandas index its rows are built from is unique, which it reads in milliseconds.

## Motivation

`write_layer` writes each attribute, reads the file back, and `_refuse_unordered` runs `refuse_twice` over its coordinates and `breakpoint`: 64-bit hashes grouped in passes of at most 100M rows, a hash held twice confirmed on the keys. On an hourly PyPSA network of 80,000 generators, `p_max_pu` is 700M long rows, and that check takes about 26 s of a 136 s write.

For that attribute the answer is known before a row is written. Its rows are a pivoted frame melted entity by entity, one row per column and snapshot, so they hold no twin wherever the frame's columns and the snapshots are unique. pandas keeps that as `Index.is_unique`. The writer has no way to be told.

## Specification

### `UniqueCoordinates`

```python
@runtime_checkable
class UniqueCoordinates(Protocol):
    def is_unique(self, attribute: str) -> bool: ...
```

- **`is_unique(a)` True claims that no two rows of `a` agree on every coordinate of `a` and on `breakpoint`, NULL agreeing with NULL**, exactly the pairs `refuse_twice` refuses. False claims nothing.
- **A source satisfies it by shape**, beside `LayerData` or `RecordLike`. A source that does not is never vouched for, a `LayerSource` among them.
- **The writer asks once per attribute it writes** and skips `refuse_twice` there on True. The crossing-row check (`unsettled`), `refuse_unheld`, the type and shape checks and every axis check run as before.
- **The adapter forwards it.** `_RecordLikeAsLayerData.is_unique(a)` answers `isinstance(source, UniqueCoordinates) and source.is_unique(a)`, so a `RecordLike` handed to `store.root`, `rev.child` or `ws.write` vouches as itself.
- **The answer is about the relation handed over in that write**, and the layer stores nothing of it.

### What a `Record` vouches for

`Record.is_unique(a)` is True for every attribute, so `ws.write(record, uri)`, `store.root(record)` and `rev.child(record)` skip the check, a `WorkingRecord` included through its pinned copy.

- **No two rows of a layer agree on every coordinate column**, NULL agreeing with NULL. The format requires it and `write_layer` checks each layer it writes. A directory written by hand with two such rows is an invalid store, which `Record.at(uri)` reads as it reads any directory: a `Record` over it is trusted as a source vouching wrongly is ([trust](#trust)).
- **The resolved read fans out no row.** `_as_stored` gives each owner-map entry one stored form per dim, the owned label where its flags say the layer named it and NULL where they say it left it NULL, so a raw row matches an entry at most once, and its resolved coordinate is the entry's owned key. The entries a row of one layer matches are that layer's, and no two layers' entries own one key, since a layer's own row drops every parent entry agreeing with it on the dims it names. One layer's entries at one owned key are its own row there and the overlap rows it owns where it crosses a parent row, which reach rows of different named sets; such a layer holds keys both ways, and `_most_specific` keeps the more specific row or fails the read.
- **A staged layer holds one row per key**, since an edit replaces by key rather than appending, and its crossing rows fail the read (`_crossings`).

A staged layer, `w.edits`, does not vouch when written itself: its rows are one per key, but staged edits are small and gain little.

### What PyPSA vouches for

`_NetworkSource.is_unique(a)` is True where all of the following hold, for every type contributing rows to `a`. Names are compared as strings, as `_colliding_names` compares them.

1. **The snapshot labels are unique.** `_melt_dynamic` melts by position and looks each label up, so two equal labels put two rows of one entity at one snapshot. A multi-period network whose timesteps repeat is numbered by position and always passes.
2. **The type's static index is unique**, `(scenario, name)` on a stochastic network. `_scalar_rows` gives one row per index entry, `_shared_across_scenarios` keeps an entity's scenario rows or folds them to one, and `_port_long_rel`'s join on the bus column repeats a name's rows once per repeat.
3. **Each contributing dynamic frame's columns are unique and a subset of that index**, a MultiIndex `(scenario, name)` on a stochastic network. A column naming another type's member lands two types' rows on one entity, which `_colliding_names` misses, since it reads static indexes alone.
4. **No entity attaches one bus at two ports**, for a per-port attribute, whose rows are keyed by `(entity, bus)`. The connection axis refuses such a pair on a deterministic network, but `_connection_frame` collapses it on a stochastic one.
5. **No scenario or name holds `\x1f`** on a stochastic network, the separator `_melt_dynamic` splits on.
6. **A weighting's axis labels are unique**: the snapshot labels, `investment_period_weightings.index` or `scenario_weightings.index`.
7. **An attribute other than a weighting is declared over `scenario`**, on a stochastic network. `_shared_across_scenarios` keeps an entity's row of each scenario where its values differ, and `_union_select` projects them to the attribute's own coordinates, so one not over `scenario`, a static input or a custom column among them, lands them at one coordinate.

Two things need no condition. An entity's scalar and series rows never meet: `_as_long_rel` leaves out the scalar of every name the dynamic frame has a column for, and a scalar's `snapshot` is NULL where a melted row's is a label. Names shared across entity types are refused by `_colliding_names` before `to_datarecord` returns a source.

`_ResultsSource.is_unique(a)` holds by the same conditions over this solve's outputs, for a result `base` holds no rows of. For one it does, the rows restated as NULL are anti-joined against this solve's on every key, so they never meet them, but they are twin-free only where `base`'s rows are. There it answers the conditions and `isinstance(base, UniqueCoordinates) and base.is_unique(a)`, True for every `Record`.

### Trust

A source that vouches wrongly writes a layer holding two rows at a coordinate. Nothing refuses it afterwards. A read of an attribute two layers contest fails at that coordinate when fetched (`_most_specific`); an uncontested one hands both rows over, which `build` meets as a duplicate in its pivot and a SQL consumer sums twice. The layer is write-once, so the repair is a new layer from a corrected source.

The writer accepts that risk. It already trusts a source's values: a melt putting a value at the wrong snapshot is refused nowhere. A claim of unique coordinates is the same kind of trust, made by code that built the rows in the same process from the frames it reads the indexes off, never by a caller's argument. A test holds every claim the tool makes against the check it skips ([implementation notes](#implementation-notes)).

No switch re-runs the check for a vouched attribute. A user who distrusts a tool's claim distrusts its values as much, and a switch would be a write-path argument restating, against the source, what the source knows.

## Rationale

**A protocol of its own, not a member of `LayerData`.** `protocols.py` declares no optional member, and a runtime-checkable protocol cannot have one: `isinstance` requires every member, and `layer_data` dispatches on `isinstance(source, LayerData)`. A member added to `LayerData` would send each source lacking it, `StagedSource` and `_FileLayer` among them, to the `RecordLike` adapter, so each would have to answer False to stay a `LayerData`. Nor would it reach PyPSA: `_NetworkSource` is a `RecordLike`, read through the adapter. A member of `RecordLike` reaches it, at the cost of every record, `Record` and a framework's own included, answering a write-path question on a protocol that is read-only. A protocol of its own leaves both untouched, makes absence the safe default, and the adapter forwards it as it forwards the rest. `LayerData` keeps one enumerate-and-read pair per kind, without a third member holding for one kind.

**Per attribute.** One type's repeated name spoils only the attributes it contributes to, so a network with one bad frame still skips the check everywhere else.

**A `Record` vouches.** Answering False would spare a hand-written directory holding twins from being flattened unchecked, at the cost of every whole-record write and every results layer's restated rows hashing every row. That directory is no valid store, and a `Record` over it is the same trust as a source that lies, so the check buys nothing a valid store needs.

**What it costs.** The guarantee that no attribute file holds two rows naming the same dims at one coordinate becomes conditional on the source. A misspelt `is_unique` is no claim, the safe direction, and a type checker sees the member only where a source declares the protocol.

## Usage

```python
source = PyPSA.to_datarecord(n, ws.con)
source.is_unique("p_max_pu")  # True: unique snapshots, names and columns
root = store.root(source)  # refuse_twice skipped for p_max_pu
```

A source of one's own vouches by shape:

```python
class WideFrames:  # a RecordLike over one wide frame per attribute
    def is_unique(self, attribute: str) -> bool:
        wide = self.frames[attribute]
        return wide.index.is_unique and wide.columns.is_unique
```

## Implementation notes

Paths are under `src/datarecord/`.

1. `UniqueCoordinates` in `protocols.py`; `_RecordLikeAsLayerData.unique` forwarding; `write_layer` passes the answer to `_refuse_unordered`, which skips `refuse_twice` on True.
2. `_NetworkSource.unique` by the seven conditions over the columns `attributes` reads for each attribute, from `_attribute_names`, `_result_rels` and `_WEIGHTINGS`, the per-type checks read once per source; `_ResultsSource.unique` as above.
3. `Record.unique` answering True.
4. Tests: on every PyPSA fixture network, an attribute vouched for passes `refuse_twice` on its written file; a network breaking each condition answers False for the attributes it touches and its write is refused as today; a source vouching wrongly writes, which pins the trust; a record of layers, a key held both as a label and as NULL and a crossing override among them, written whole passes `refuse_twice`.
5. Pages: [writing](../use/writing.md) and [PyPSA](../tools/pypsa.md), as below; `CHANGELOG.md` under `[Unreleased]`.

On [writing](../use/writing.md), [Shape](../use/writing.md#shape) gains, after the rule on two rows covering one coordinate, that the first of its two cases goes unchecked for an attribute its source vouches unique for. [`LayerData`](../use/writing.md#layerdata) ends in a `UniqueCoordinates` section: its one member and what True claims, that the adapter forwards it, what a `Record` vouches for, and the trust paragraph above. [PyPSA](../tools/pypsa.md) gains a section listing the seven conditions, after [snapshots and weightings](../tools/pypsa.md#snapshots-and-weightings). [The package layout](../package.md) lists `UniqueCoordinates` in `protocols.py`.

Checks that hold once built:

- **The 80k-generator write drops by the check's ~26 s**, measured on `p_max_pu`.
- **Every attribute PyPSA vouches for on the fixture networks passes `refuse_twice`.**

## Rejected Ideas

**Other pass sizes.** The check costs 26-38 s at every pass size tried, where one hashing scan of the file costs 1 s, so the grouping dominates whatever the passes.

**HyperLogLog** (`approx_count_distinct`). An estimate against the row count can suggest twins but never prove their absence.

**A Bloom filter.** DuckDB has none at SQL level. The community `bitfilters` extension only builds a filter whole and then probes it, where finding a twin needs each row tested against the rows before it, and as a window aggregate it rebuilds the filter per row.

**Sort and compare**, `lag(h) OVER (ORDER BY h)` over the 64-bit hash and a confirm of equal neighbours. Measured on the 700M-row `p_max_pu` file, it takes 35 s against 16–27 s for the grouped passes, at 7.9 GiB peak memory against 3.5 GiB, and only under an 8 GB `memory_limit`; without one it passed 14 GiB. Sorting alone takes 23 s, so no variant of it wins.

**A flag on the relation.** A `DuckDBPyRelation` carries no metadata, and a wrapper type would break rows being relations throughout the write path.

**A `write_layer` argument**, such as `unique=` naming attributes. It parts the claim from the rows it is about, has every caller of `store.root`, `rev.child` and `ws.write` restate what the source already knows, and lets a caller vouch for a source it did not build.

**A member of `LayerData` or of `RecordLike`.** See [rationale](#rationale).

## Open Issues

None.
