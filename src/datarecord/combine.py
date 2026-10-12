# SPDX-FileCopyrightText: datarecord contributors
#
# SPDX-License-Identifier: MIT

"""One record from a base and an extension that adds names to it.

A framework's tool presents its own object as a `RecordLike`, and the schema of
that record is the tool's. Data a consumer derives beside it - the inputs of
constraints the framework keeps out of its model, say - has no place in the
tool's schema. `combine` lays such an extension over the base: the schemas are
united, every name the extension declares is new, and the dims they share are
declared identically. `FrameRecord` is the extension's own shape, a schema and
ready-made frames.

Notes
-----
- [combining records](https://energy-models.github.io/datarecord/design/writing/#combining-records)
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import narwhals as nw

from datarecord.record import EMPTY, Flags, Frames, LazyFrames, RecordLike
from datarecord.schema import Schema


@dataclass(frozen=True)
class FrameRecord:
    """A `RecordLike` over ready-made frames: the shape an extension takes.

    Each mapping holds native frames of any backend narwhals reads (pandas,
    polars, pyarrow, DuckDB), keyed as the `RecordLike` member of the same name.
    `flags` answers nothing, as a tool's own record does: `write_record`
    persists frames, not flags.

    Notes
    -----
    - [combining records](https://energy-models.github.io/datarecord/design/writing/#combining-records)
    """

    schema: Schema
    dim_frames: Mapping[str, Any] = field(default_factory=dict)
    entity_type_frames: Mapping[str, Any] = field(default_factory=dict)
    group_frames: Mapping[str, Any] = field(default_factory=dict)
    attribute_frames: Mapping[str, Any] = field(default_factory=dict)
    output_frames: Mapping[str, Any] = field(default_factory=dict)

    @staticmethod
    def _frames(mapping: Mapping[str, Any]) -> Frames:
        if not mapping:
            return EMPTY
        return LazyFrames(
            tuple(mapping), lambda key: nw.from_native(mapping[key]).lazy()
        )

    @property
    def dims(self) -> Frames:
        return self._frames(self.dim_frames)

    @property
    def entity_types(self) -> Frames:
        return self._frames(self.entity_type_frames)

    @property
    def groups(self) -> Frames:
        return self._frames(self.group_frames)

    @property
    def attributes(self) -> Frames:
        return self._frames(self.attribute_frames)

    @property
    def outputs(self) -> Frames:
        return self._frames(self.output_frames)

    def flags(self, ctype: str) -> dict[str, Flags]:
        return {}


def _disjoint(kind: str, base: Mapping[str, Any], extension: Mapping[str, Any]) -> None:
    shared = sorted(set(base) & set(extension))
    if shared:
        msg = (
            f"the extension declares the {kind} {shared}, which the base declares "
            "already; an extension adds names, it never redefines one"
        )
        raise ValueError(msg)


def combine_schemas(base: Schema, extension: Schema) -> Schema:
    """The union of two schemas that agree on every dim they share.

    Raises
    ------
    ValueError
        If the versions differ, a shared dim is declared differently, or both
        declare one attribute, result, group, trait or `meta` key.

    Notes
    -----
    - [combining records](https://energy-models.github.io/datarecord/design/writing/#combining-records)
    """
    if base.version != extension.version:
        msg = f"the base is schema version {base.version}, the extension {extension.version}"
        raise ValueError(msg)
    differing = sorted(
        dim
        for dim in set(base.dimensions) & set(extension.dimensions)
        if base.dimensions[dim] != extension.dimensions[dim]
    )
    if differing:
        msg = f"the extension declares the shared dims {differing} differently"
        raise ValueError(msg)
    for kind in ("attributes", "results", "groups", "traits", "meta"):
        _disjoint(kind, getattr(base, kind), getattr(extension, kind))
    partial = (
        None
        if base.partial is None and extension.partial is None
        else (base.partial or frozenset()) | (extension.partial or frozenset())
    )
    return Schema(
        version=base.version,
        dimensions=base.dimensions | extension.dimensions,
        attributes=base.attributes | extension.attributes,
        results=base.results | extension.results,
        groups=base.groups | extension.groups,
        traits=base.traits | extension.traits,
        partial=partial,
        meta=base.meta | extension.meta,
    )


def _widened(
    base: nw.LazyFrame,
    extension: nw.LazyFrame,
    key: tuple[str, ...],
    columns: tuple[str, ...],
    where: str,
) -> nw.LazyFrame:
    """`base` with the extension's `columns` joined on `key`, NULL where it has none.

    A declared column the extension's frame lacks is one it sets no value of.
    """
    left = base.collect(backend="pyarrow")
    written = [c for c in columns if c in extension.collect_schema().names()]
    right = extension.select(*key, *written).collect(backend="pyarrow")
    unknown = right.join(left.select(*key), on=list(key), how="anti")
    if len(unknown):
        msg = (
            f"the extension sets {written} on {where} labels the base does not "
            f"hold: {unknown.select(*key).rows()[:5]}"
        )
        raise ValueError(msg)
    return left.join(right, on=list(key), how="left").lazy()


class _Combined:
    """The `RecordLike` `combine` returns."""

    def __init__(self, base: RecordLike, extension: RecordLike) -> None:
        self._base = base
        self._extension = extension
        self._schema = combine_schemas(base.schema, extension.schema)

    @property
    def schema(self) -> Schema:
        return self._schema

    def _united(self, kind: str) -> Frames:
        base, extension = getattr(self._base, kind), getattr(self._extension, kind)
        _disjoint(f"{kind} frames", base, extension)
        keys = (*base, *extension)
        return LazyFrames(
            keys, lambda key: base[key] if key in base else extension[key]
        )

    def _widening(self, kind: str, key_of, columns_of) -> Frames:
        base, extension = getattr(self._base, kind), getattr(self._extension, kind)
        keys = (*base, *(k for k in extension if k not in base))

        def build(key: str) -> nw.LazyFrame:
            if key not in extension:
                return base[key]
            if key not in base:
                return extension[key]
            return _widened(
                base[key], extension[key], key_of(key), columns_of(key), key
            )

        return LazyFrames(keys, build)

    @property
    def dims(self) -> Frames:
        schema = self._extension.schema
        return self._widening("dims", schema.axis_key, schema.attributes_on)

    @property
    def entity_types(self) -> Frames:
        columns = tuple(
            name
            for name, spec in self._extension.schema.attributes.items()
            if spec.dims == frozenset({"entity"})
        )
        return self._widening("entity_types", lambda _: ("entity",), lambda _: columns)

    @property
    def groups(self) -> Frames:
        return self._united("groups")

    @property
    def attributes(self) -> Frames:
        return self._united("attributes")

    @property
    def outputs(self) -> Frames:
        return self._united("outputs")

    def flags(self, ctype: str) -> dict[str, Flags]:
        return self._base.flags(ctype) | self._extension.flags(ctype)


def combine(base: RecordLike, extension: RecordLike) -> RecordLike:
    """`base` with everything `extension` adds: new dims, groups and attributes.

    Framework-neutral: neither side need be a tool's record. The schemas are
    united by `combine_schemas`. A frame of a key only one side holds is that
    side's. A dim or entity type both hold gets the extension's columns joined
    onto the base's rows by key - an attribute addressed by an axis alone, or by
    `entity` alone, lives in that frame - and every label the extension sets a
    value on must be one the base holds. Groups, attributes and outputs are new
    names, so their frames never meet.

    Raises
    ------
    ValueError
        Where the schemas collide (`combine_schemas`), or the extension sets a
        value on a label the base does not hold. The second is checked when the
        frame is read.

    Notes
    -----
    - [combining records](https://energy-models.github.io/datarecord/design/writing/#combining-records)
    """
    return _Combined(base, extension)
