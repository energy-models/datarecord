<!--
SPDX-FileCopyrightText: datarecord contributors

SPDX-License-Identifier: CC-BY-4.0
-->

# PyPSA tool

The PyPSA tool translates between a record and a `pypsa.Network`, so that you can solve a record with PyPSA and commit the results as a revision. It needs the optional extra `datarecord[pypsa]`.

!!! note

    We aim to hand the PyPSA tool to the PyPSA community, so its tutorial, how-to guides and reference are grouped here to move with it.

## Tutorial

- [Solve a record with PyPSA](tutorial.md) takes you from an empty store to a solved record and its results.

## How-to guides

Each guide finishes one task, and assumes you have taken the tutorial.

- [Use the PyPSA tool](use-the-tool.md) to export a network, check it with `verify`, solve it and commit the results.
- [Export a model workflow's network](export-a-workflow-network.md) to write the network a workflow hands its solver as a record in a directory.
- [Solve a record in a model workflow](solve-in-a-workflow.md) to verify, solve and commit a scenario or a myopic run from that directory.

## Reference

- [The PyPSA tool](reference.md) lists what the tool reads from a record and what it writes back.
