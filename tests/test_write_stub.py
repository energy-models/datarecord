"""The diff-based patch write fails loudly, superseded by `MutableStore` (§11)."""

import pytest

from datarecord import DataRecord


def test_add_patch_is_not_implemented(con):
    """Deriving a patch from two framework objects is not the write path (§11)."""
    record = DataRecord.create(con)
    with pytest.raises(NotImplementedError, match="MutableStore"):
        record.add_patch(None, None)
