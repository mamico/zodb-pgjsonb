"""Tests for reference extraction (no PostgreSQL needed)."""

from zodb_pgjsonb.storage import _extract_refs


class TestExtractRefs:
    def test_empty_dict(self):
        assert _extract_refs({}) == []

    def test_no_refs(self):
        assert _extract_refs({"title": "Hello", "count": 42}) == []

    def test_single_ref(self):
        state = {"author": {"@ref": "0000000000000003"}}
        assert _extract_refs(state) == [3]

    def test_ref_with_class(self):
        state = {"author": {"@ref": ["0000000000000003", "myapp.models.User"]}}
        assert _extract_refs(state) == [3]

    def test_nested_refs(self):
        state = {
            "author": {"@ref": "0000000000000003"},
            "folder": {"@ref": "000000000000000a"},
        }
        refs = _extract_refs(state)
        assert sorted(refs) == [3, 10]

    def test_refs_in_list(self):
        state = {
            "items": [
                {"@ref": "0000000000000001"},
                {"@ref": "0000000000000002"},
            ]
        }
        assert sorted(_extract_refs(state)) == [1, 2]

    def test_deeply_nested(self):
        state = {
            "data": {
                "nested": {
                    "deep": {"@ref": "00000000000000ff"}
                }
            }
        }
        assert _extract_refs(state) == [255]

    def test_mixed_content(self):
        state = {
            "title": "Hello",
            "author": {"@ref": "0000000000000003"},
            "tags": ["a", "b"],
            "metadata": {
                "created": {"@dt": "2025-01-01T00:00:00"},
                "parent": {"@ref": "0000000000000001"},
            },
        }
        assert sorted(_extract_refs(state)) == [1, 3]

    def test_scalar_state(self):
        assert _extract_refs(42) == []

    def test_list_state(self):
        state = [{"@ref": "0000000000000001"}, "hello"]
        assert _extract_refs(state) == [1]

    def test_none_state(self):
        assert _extract_refs(None) == []
