# ABOUTME: Tests for the safe-name validation guard shared across storage
# ABOUTME: Covers allowed characters, path-style names, and traversal rejection

import pytest

from brooklet.storage.names import validate_safe_name


class TestValidateSafeName:
    def test_accepts_plain_name(self):
        validate_safe_name("events", "topic")  # no raise

    def test_accepts_path_style_name(self):
        validate_safe_name("scout/stats", "topic")  # slashes allowed

    def test_accepts_hyphens_underscores_dots(self):
        validate_safe_name("a-b_c.d", "group")  # no raise

    def test_rejects_unsafe_characters(self):
        with pytest.raises(ValueError, match="safe characters"):
            validate_safe_name("bad name!", "topic")

    def test_rejects_path_traversal(self):
        with pytest.raises(ValueError, match="path traversal"):
            validate_safe_name("../escape", "topic")

    def test_label_appears_in_message(self):
        with pytest.raises(ValueError, match="group must contain"):
            validate_safe_name("nope$", "group")
