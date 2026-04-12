# ABOUTME: Tests for config precedence chain — resolve_stream_dir() with layered overrides
# ABOUTME: Covers CLI flag, .brooklet.toml walk-up, env var, user config, and git-root default

from unittest.mock import patch

import pytest

from brooklet.config import ConfigError, find_config_file, resolve_stream_dir


class TestResolvePrecedence:
    """Verify the 5-layer precedence: flag > local toml > env > user config > git root."""

    def test_cli_flag_wins_over_everything(self, tmp_path, monkeypatch):
        """Layer 1: explicit CLI flag takes highest priority."""
        # Set up a .brooklet.toml that should be ignored
        toml = tmp_path / ".brooklet.toml"
        toml.write_text('stream_dir = "/should/be/ignored"\n')

        monkeypatch.setenv("BROOKLET_DIR", "/also/ignored")
        monkeypatch.chdir(tmp_path)

        explicit = tmp_path / "explicit"
        result = resolve_stream_dir(cli_flag=explicit)
        assert result == explicit

    def test_local_toml_wins_over_env_var(self, tmp_path, monkeypatch):
        """Layer 2: .brooklet.toml overrides BROOKLET_DIR env var."""
        local_dir = tmp_path / "local-streams"
        local_dir.mkdir()

        toml = tmp_path / ".brooklet.toml"
        toml.write_text(f'stream_dir = "{local_dir}"\n')

        monkeypatch.setenv("BROOKLET_DIR", "/should/be/ignored")
        monkeypatch.chdir(tmp_path)

        result = resolve_stream_dir()
        assert result == local_dir

    def test_local_toml_relative_path_resolves_from_config_parent(self, tmp_path, monkeypatch):
        """Relative paths in .brooklet.toml resolve relative to the config file's parent."""
        toml = tmp_path / ".brooklet.toml"
        toml.write_text('stream_dir = "my-streams"\n')

        monkeypatch.chdir(tmp_path)

        result = resolve_stream_dir()
        assert result == tmp_path / "my-streams"

    def test_env_var_wins_over_user_config(self, tmp_path, monkeypatch):
        """Layer 3: BROOKLET_DIR env var overrides user-wide config."""
        env_dir = tmp_path / "env-streams"
        env_dir.mkdir()

        # No local .brooklet.toml
        monkeypatch.setenv("BROOKLET_DIR", str(env_dir))
        monkeypatch.chdir(tmp_path)

        # Mock out user config to avoid interference from real ~/.config
        with patch("brooklet.config._user_config_path") as mock_ucp:
            mock_ucp.return_value = tmp_path / "nonexistent" / "config.toml"
            result = resolve_stream_dir()

        assert result == env_dir

    def test_user_config_wins_over_default(self, tmp_path, monkeypatch):
        """Layer 4: ~/.config/brooklet/config.toml overrides the git-root default."""
        user_dir = tmp_path / "user-streams"
        user_dir.mkdir()

        user_config = tmp_path / "config" / "brooklet" / "config.toml"
        user_config.parent.mkdir(parents=True)
        user_config.write_text(f'stream_dir = "{user_dir}"\n')

        # No local toml, no env var
        monkeypatch.delenv("BROOKLET_DIR", raising=False)
        monkeypatch.chdir(tmp_path)

        with patch("brooklet.config._user_config_path") as mock_ucp:
            mock_ucp.return_value = user_config
            result = resolve_stream_dir()

        assert result == user_dir

    def test_default_is_git_root(self, tmp_path, monkeypatch):
        """Layer 5: falls back to git repo root when no config exists."""
        # Simulate a git repo
        (tmp_path / ".git").mkdir()
        subdir = tmp_path / "src" / "deep"
        subdir.mkdir(parents=True)

        monkeypatch.delenv("BROOKLET_DIR", raising=False)
        monkeypatch.chdir(subdir)

        with patch("brooklet.config._user_config_path") as mock_ucp:
            mock_ucp.return_value = tmp_path / "nonexistent" / "config.toml"
            result = resolve_stream_dir()

        assert result == tmp_path

    def test_default_is_cwd_when_not_in_git_repo(self, tmp_path, monkeypatch):
        """Layer 5 fallback: use cwd if not in a git repo."""
        monkeypatch.delenv("BROOKLET_DIR", raising=False)
        monkeypatch.chdir(tmp_path)

        with patch("brooklet.config._user_config_path") as mock_ucp:
            mock_ucp.return_value = tmp_path / "nonexistent" / "config.toml"
            result = resolve_stream_dir()

        assert result == tmp_path

    def test_none_cli_flag_is_skipped(self, tmp_path, monkeypatch):
        """A None CLI flag falls through to the next layer."""
        toml = tmp_path / ".brooklet.toml"
        toml.write_text('stream_dir = "from-toml"\n')
        monkeypatch.chdir(tmp_path)

        result = resolve_stream_dir(cli_flag=None)
        assert result == tmp_path / "from-toml"


class TestFindConfigFile:
    """Walk-up search for config files, stopping at .git boundary."""

    def test_finds_in_current_dir(self, tmp_path):
        toml = tmp_path / ".brooklet.toml"
        toml.write_text('stream_dir = "."\n')
        result = find_config_file(".brooklet.toml", start=tmp_path)
        assert result == toml

    def test_finds_in_parent_dir(self, tmp_path):
        toml = tmp_path / ".brooklet.toml"
        toml.write_text('stream_dir = "."\n')
        child = tmp_path / "src" / "pkg"
        child.mkdir(parents=True)

        result = find_config_file(".brooklet.toml", start=child)
        assert result == toml

    def test_stops_at_git_root(self, tmp_path):
        """Should not walk above the .git boundary."""
        # Parent has a config, but child has .git — should not find parent's config
        parent_toml = tmp_path / ".brooklet.toml"
        parent_toml.write_text('stream_dir = "parent"\n')

        repo = tmp_path / "repo"
        repo.mkdir()
        (repo / ".git").mkdir()

        result = find_config_file(".brooklet.toml", start=repo)
        assert result is None

    def test_returns_none_when_not_found(self, tmp_path):
        result = find_config_file(".brooklet.toml", start=tmp_path)
        assert result is None

    def test_git_file_counts_as_boundary(self, tmp_path):
        """In a worktree, .git is a file (not dir). Both should count as boundary."""
        parent_toml = tmp_path / ".brooklet.toml"
        parent_toml.write_text('stream_dir = "parent"\n')

        worktree = tmp_path / "worktree"
        worktree.mkdir()
        # .git as a file (worktree pointer)
        (worktree / ".git").write_text("gitdir: /somewhere/.git/worktrees/wt\n")

        result = find_config_file(".brooklet.toml", start=worktree)
        assert result is None

    def test_finds_config_at_git_root_level(self, tmp_path):
        """Config file at the same level as .git should be found."""
        (tmp_path / ".git").mkdir()
        toml = tmp_path / ".brooklet.toml"
        toml.write_text('stream_dir = "."\n')

        subdir = tmp_path / "src"
        subdir.mkdir()

        result = find_config_file(".brooklet.toml", start=subdir)
        assert result == toml


class TestConfigErrors:
    """Error handling for malformed or invalid config files."""

    def test_invalid_toml_raises_config_error(self, tmp_path, monkeypatch):
        """Invalid TOML syntax produces a clear ConfigError, not a raw traceback."""
        toml = tmp_path / ".brooklet.toml"
        toml.write_text("stream_dir = no quotes here\n")
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ConfigError, match=r"\.brooklet\.toml"):
            resolve_stream_dir()

    def test_missing_stream_dir_key_raises_config_error(self, tmp_path, monkeypatch):
        """Config file without stream_dir key raises ConfigError listing found keys."""
        toml = tmp_path / ".brooklet.toml"
        toml.write_text('[other]\nfoo = "bar"\n')
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ConfigError, match="stream_dir"):
            resolve_stream_dir()

    def test_stream_dir_wrong_type_raises_config_error(self, tmp_path, monkeypatch):
        """Non-string stream_dir value produces a clear error."""
        toml = tmp_path / ".brooklet.toml"
        toml.write_text("stream_dir = 42\n")
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ConfigError, match="string"):
            resolve_stream_dir()

    def test_unreadable_config_raises_config_error(self, tmp_path, monkeypatch):
        """Permission-denied on config file produces a clear ConfigError."""
        toml = tmp_path / ".brooklet.toml"
        toml.write_text('stream_dir = "."\n')
        toml.chmod(0o000)
        monkeypatch.chdir(tmp_path)

        try:
            with pytest.raises(ConfigError, match="Cannot read"):
                resolve_stream_dir()
        finally:
            # Restore permissions so tmp_path cleanup works
            toml.chmod(0o644)

    def test_config_error_includes_file_path(self, tmp_path, monkeypatch):
        """Error messages include the offending config file path."""
        toml = tmp_path / ".brooklet.toml"
        toml.write_text("not valid toml {{{\n")
        monkeypatch.chdir(tmp_path)

        with pytest.raises(ConfigError) as exc_info:
            resolve_stream_dir()
        assert str(tmp_path / ".brooklet.toml") in str(exc_info.value)


class TestGitRootDetection:
    """The smart default finds the git repo root."""

    def test_detects_git_dir(self, tmp_path, monkeypatch):
        (tmp_path / ".git").mkdir()
        subdir = tmp_path / "a" / "b"
        subdir.mkdir(parents=True)

        monkeypatch.delenv("BROOKLET_DIR", raising=False)
        monkeypatch.chdir(subdir)

        with patch("brooklet.config._user_config_path") as mock_ucp:
            mock_ucp.return_value = tmp_path / "nonexistent" / "config.toml"
            result = resolve_stream_dir()

        assert result == tmp_path

    def test_detects_git_file_worktree(self, tmp_path, monkeypatch):
        """Git worktrees use a .git file, not a directory."""
        (tmp_path / ".git").write_text("gitdir: /somewhere\n")

        monkeypatch.delenv("BROOKLET_DIR", raising=False)
        monkeypatch.chdir(tmp_path)

        with patch("brooklet.config._user_config_path") as mock_ucp:
            mock_ucp.return_value = tmp_path / "nonexistent" / "config.toml"
            result = resolve_stream_dir()

        assert result == tmp_path
