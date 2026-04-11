# ABOUTME: Mechanical enforcement of codebase conventions.
# ABOUTME: These tests verify structural invariants, not functionality.

from pathlib import Path


def test_all_src_py_files_have_aboutme():
    """Every .py file in src/brooklet/ must start with a 2-line ABOUTME comment."""
    src_dir = Path("src/brooklet")
    py_files = [p for p in src_dir.rglob("*.py") if p.name != "__init__.py"]
    assert py_files, "No .py files found in src/brooklet/"

    for path in py_files:
        lines = path.read_text().splitlines()
        assert len(lines) >= 2, f"{path}: file too short for ABOUTME"
        assert lines[0].startswith("# ABOUTME:"), f"{path}: line 1 must start with '# ABOUTME:'"
        assert lines[1].startswith("# ABOUTME:"), f"{path}: line 2 must start with '# ABOUTME:'"


def test_all_test_py_files_have_aboutme():
    """Every test .py file must start with a 2-line ABOUTME comment."""
    test_dir = Path("tests")
    py_files = [p for p in test_dir.rglob("*.py") if p.name != "__init__.py"]
    assert py_files, "No .py files found in tests/"

    for path in py_files:
        lines = path.read_text().splitlines()
        assert lines, f"{path}: file is empty, must have ABOUTME header"
        assert lines[0].startswith("# ABOUTME:"), f"{path}: line 1 must start with '# ABOUTME:'"
        assert len(lines) >= 2 and lines[1].startswith("# ABOUTME:"), (
            f"{path}: line 2 must start with '# ABOUTME:'"
        )
