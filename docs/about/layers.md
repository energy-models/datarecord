<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# Layers

A layer's data is written once, and the rules for overriding values, restating axes and removing labels each follow from that choice.

## Why a layer is written once

A layer is the data one revision adds on top of its parent, and revisions form a tree ([layers](../reference/layers.md)). Once `store.commit` writes a layer, nothing changes its data. A `WorkingRecord` stages edits until a commit writes them as a new child.

The rejected alternative is a layer edited in place. Its descendants would then read a base that can shift under them, so branching from it would be unsafe. Every materialised revision below it would also need its resolved files rewritten on each edit.

With an immutable base, both problems go. Any revision can be a parent, so a scenario branches from any revision without a copy. A materialised revision has its owner map and resolved axes written under `resolved/`. Those files derive only from immutable layers, so they never go stale, and a read returns the same rows with or without them.

The cost is depth. Every change is a new layer, and a read folds the layers from the root or from the nearest materialised ancestor. In a fresh workspace that fold grows with the depth of the tree until you materialise ([branch and materialise](../howto/branch-and-materialise.md)).

## Why `partial` sits on the axis

`partial` is the set of dims along which a layer overrides one value per label. Along any other dim, a layer restates the whole extent of each map row it touches. With `snapshot` outside `partial`, changing one hour of a generator's `p_max_pu` restates that generator's whole series.

The reason lies in how a framework reads a series. A framework that stores a constant apart from a series, as PyPSA does, cannot receive half a series. If a layer could patch one snapshot, a read would mix the layer's values at the snapshots it wrote with its parent's at the rest. That holds for every attribute over `snapshot`, so the rule belongs to the dim and not to each attribute.

`partial` and an attribute's `dims` set different things. `dims` states which dims an attribute varies over, so `p_nom_opt`, a capacity an optimiser fixes before the scenario is known, is declared over `entity` alone. `partial` states how finely a layer may patch. A flag on the dim alone would make every attribute vary per scenario, the first-stage decision included. Keeping the two apart lets the writer refuse a `p_nom_opt` row that sets a scenario.

The rejected alternatives are `partial` per attribute, and every dim in `partial`. Per attribute, the owner map's index would vary by attribute, where the fold needs one fixed index. Each dim in `partial` widens that index, and every read of every attribute scans the wider index. A dim left out adds work once per edit, bounded by the length of its axis. The PyPSA tool puts `entity`, `bus`, `scenario` and `connection` in `partial`, and leaves `snapshot` and `carrier` out.

## Why a layer restates an axis whole

A layer that changes an axis writes its file whole: every label, with its properties, in order. A revision's axis is the deepest file for that dim on its path from the root.

The rejected alternative patches an axis label by label, with a tombstone row for each removed label. No single file then states which labels an axis has, or in which order. The order matters, because the PyPSA tool numbers a component's ports by the order of its connections. Every materialised revision and staged fold would derive that order from the history, and a patched axis would need an owner map of its own.

With one file per axis, a materialised revision or a staged fold always lists the same labels, in the same order, as a full fold. The writer refuses a restatement that reorders its parent's labels, so an edit renumbers no port.

The cost is storage that grows with the axis and not with the edit. On a record of 60,000 entities and 150,000 connections, a one-entity property edit wrote 0.30 MB where a label patch wrote 0.8 kB. An axis is small beside the series over it, so the cost stays small beside a solve's results.

## Why the owner map contains map rows as written

The owner map is the fold's result: per attribute, each map row a layer wrote and the layer the map assigns it to ([resolution](../reference/resolution.md)). datarecord keeps each map row as the layer wrote it, NULL included. Where rows of two layers cross, the fold adds one overlap row that sets the labels of both. The map assigns that row to the deeper layer.

The rejected alternative expands each NULL to the labels of its dim when the fold runs. The map then grows with those labels, so one broadcast over 60,000 entities and two scenarios takes 120,000 rows. A materialised map also contains the expansion, so it has no row for a label a child adds later:

<figure class="diagram">
--8<-- "docs/static/about-layers-keys.svg"
</figure>

Kept as written, the parent's NULL means every label the axis has at the revision you read, `solar` included. A label added below a materialised revision, or staged in a `WorkingRecord`, gets the broadcast value as it would if the revision were not materialised.

A second alternative keeps a depth on each row and works out at every read which layer's row applies. Every read would then repeat work the fold does once, and a reader would have to handle layers.

On unusual trees, this design reads more slowly than expansion would. Where many layers override one attribute along ever different sets of dims, the crossing pairs multiply. In a benchmark of 32 layers that mostly cross each other, the first read took 1.5 times as long as with expansion. A workflow rarely builds such a tree.

## Why removing a label is a restatement

A layer removes a label by restating its axis without it. Removing a label also removes its values on every attribute and every multi-dim tuple that sets it ([layers](../reference/layers.md)).

The rejected alternatives add a second mechanism: a tombstone row per removed label, or a list of removals in the manifest. The fold would have to apply it as well as the restatement. With removal as a restatement, datarecord reads every axis by one rule: the deepest file for its dim. The labels a layer removes are its parent's axis minus its own file.

The writer refuses a layer whose values, tuples or properties refer to a removed label. `remove` refuses a label that a property still refers to. Every row a read returns sets only labels the record has, so a reader that joins a value to its axis finds no orphan.

Along a dim in `partial`, a label added again after its removal gets back none of the values written for it below the removal. Under PyPSA's `partial`, a new bus that takes an old bus's name inherits neither the old bus's carrier nor its series. A broadcast value still applies to it, because a NULL means every label the axis has. Sibling branches keep a label that one branch removes.

## What you can now judge

You can now judge which dims to put in `partial`, what an axis edit costs and when to materialise a deep tree. [Edit a record](../howto/edit-a-record.md) shows the edits that write a layer.
