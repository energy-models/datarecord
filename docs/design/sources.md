<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Tables by declared name

This page explains how a record meets a solver or a modelling framework, and why it meets them that way.
Data comes in, and a record goes out, as tables keyed by the names its [schema](schema.md) declares.
`from_sources` and `to_sources` are the two directions ([API](../api/sources.md)).

| declared as                       | its table                             |
| --------------------------------- | ------------------------------------- |
| a dimension                       | its labels, one column named after it |
| a [relation](schema.md#relations) | its rows, one column per coordinate   |
| a parameter (attribute)           | its coordinates and `value`           |

## Declared names

The names are the contract, and nothing sits between a record and its consumer.
`Schema.from_mathspec` builds a schema from a mathspec declarations file, so the dims, relations and attributes of a record carry the names of its dimensions, relations and parameters.
A dimension the spec declares `ordered` is left out of [`partial`](schema.md#partial-the-granularity-of-an-override), so a layer restates a series along it whole.
A spec that reads those parameters takes the same names in its `sources`.
So specsolve's `solve(spec, sources)` takes what `to_sources(record)` returns, and the record needs no knowledge of the spec.

The table follows the declaration, not the file layout.
On disk, an attribute over one dim alone is a column of that dim's axis file ([where a value lives](format.md#where-a-value-lives)).
Here it is a `(dim, value)` table like any other parameter, so a consumer does not see how a record stores it.
The `attribute` and `breakpoint` columns of a [long row](format.md#the-long-schema) are storage too, and the tables do not carry them.

## Broadcast expansion

A record holds a constant once: one row, NULL along each dim it [broadcasts](record.md#the-broadcast-rule) over.
A consumer of declared names reads a NULL coordinate as a label that matches nothing, so `to_sources` writes the broadcast out as rows:

- **A NULL coordinate becomes every label of its dim.** A `p_max_pu` of `1.0` stored with `generator` and `snapshot` NULL comes out as one row per generator and snapshot.
- **A row that names a label outranks a NULL row there.** A layer may hold a default and its exceptions side by side. Where two rows cover one coordinate, the row that names more of the broadcast dims wins.
- **A NULL value has no row.** A coordinate with no value is absent, which is how the consumer reads a missing value.
- **A parameter that no layer wrote has no table.** `to_sources` does not fill in its [`default`](schema.md#attributespec). The spec decides whether the parameter is needed ([requirements](#requirements)).

The other direction keeps the broadcast form.
A table given to `from_sources` may leave out a coordinate it broadcasts over, and `write_record` writes that column NULL.
A constant over every snapshot has no `snapshot` column, and it stays one row on disk.

## Requirements

datarecord does not check a record against a consumer.
The spec states what a solve needs, and specsolve checks the tables against it when it attaches the data.
It refuses a parameter the spec reads and the record does not hold, and a label outside the labels of a dimension, by name ([the data contract](https://specsolve.readthedocs.io/en/latest/reference/data/)).

A record read without a spec has no requirement to meet.
The same holds for [types](schema.md#types): a record does not narrow an attribute to a type, and the spec says which attributes a type uses.

## Answers

A solve's answers are stored as a record of their own, whose schema the producer defines.
That record is linked to the input record by the revision id of the input node.

## Framework objects

datarecord converts to and from no modelling framework.
A converter outside the package turns a framework's own object into tables by declared name.
For PyPSA, `sources(n)` in specsolve's `differential/pypsa/prep.py` does this for mathspec's `pypsa.yaml`.
Rebuilding a framework's object from a record, a `pypsa.Network` for example, is a converter's job too, and datarecord provides none.

This keeps every framework import out of the package ([module layout](module-layout.md)).
