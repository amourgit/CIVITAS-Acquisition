"""Tests unitaires pour GitHubMapper — pas de dépendance réseau."""
import pytest
from civitas_acquisition.connectors.code_repos.github.mapper import GitHubMapper
from civitas_acquisition.connectors.code_repos.github.models import (
    GitHubFile, GitHubIssue, GitHubPullRequest, GitHubRelease, GitHubCommit, GitHubRepo,
)
import json

INSTANCE_ID = "inst-github-1"


@pytest.fixture
def mapper():
    return GitHubMapper(instance_id=INSTANCE_ID)


class TestMapFile:
    def test_produit_raw_document(self, mapper):
        file = GitHubFile(
            path="README.md", sha="abc123", size=512,
            url="https://api.github.com/repos/org/repo/git/blobs/abc123",
            html_url="https://github.com/org/repo/blob/main/README.md",
            repo_full_name="org/repo", branch="main",
        )
        doc = mapper.map_file(file, b"# Hello World", tree_sha="tree456")
        assert doc.content == b"# Hello World"
        assert doc.content_type == "text/markdown"
        assert doc.source_ref.connector_id == "github"
        assert doc.source_ref.instance_id == INSTANCE_ID
        assert "file" in doc.tags
        assert doc.source_metadata["path"] == "README.md"
        assert doc.source_metadata["repo"] == "org/repo"

    def test_mime_python(self, mapper):
        file = GitHubFile(
            path="src/main.py", sha="def456", size=100,
            url="", html_url="https://github.com/org/repo/blob/main/src/main.py",
            repo_full_name="org/repo", branch="main",
        )
        doc = mapper.map_file(file, b"print('hello')", tree_sha="t1")
        assert doc.content_type == "text/x-python"

    def test_cursor_est_tree_sha(self, mapper):
        file = GitHubFile(
            path="file.txt", sha="blob1", size=10,
            url="", html_url="https://github.com/org/repo/blob/main/file.txt",
            repo_full_name="org/repo", branch="main",
        )
        doc = mapper.map_file(file, b"content", tree_sha="TREE_SHA_XYZ")
        assert doc.cursor is not None
        assert doc.cursor.value == "TREE_SHA_XYZ"

    def test_id_deterministe(self, mapper):
        file = GitHubFile(
            path="file.txt", sha="blob1", size=10,
            url="", html_url="https://github.com/org/repo/blob/main/file.txt",
            repo_full_name="org/repo", branch="main",
        )
        doc1 = mapper.map_file(file, b"content", tree_sha="t1")
        doc2 = mapper.map_file(file, b"content", tree_sha="t1")
        assert doc1.id == doc2.id


class TestMapIssue:
    def _make_issue(self, **overrides) -> GitHubIssue:
        defaults = dict(
            number=42, title="Bug: crash on startup", body="Steps to reproduce...",
            state="open", html_url="https://github.com/org/repo/issues/42",
            created_at="2024-01-10T10:00:00Z", updated_at="2024-01-15T12:00:00Z",
            closed_at=None, labels=("bug", "priority-high"), assignees=("alice",),
            author="bob", comments_count=3, milestone="v2.0", repo_full_name="org/repo",
            comments=[{"user": {"login": "alice"}, "body": "Can confirm.", "created_at": "2024-01-11T09:00:00Z", "updated_at": "2024-01-11T09:00:00Z"}],
        )
        defaults.update(overrides)
        return GitHubIssue(**defaults)

    def test_contenu_json_valide(self, mapper):
        doc = mapper.map_issue(self._make_issue())
        payload = json.loads(doc.content)
        assert payload["number"] == 42
        assert payload["title"] == "Bug: crash on startup"
        assert "bug" in payload["labels"]
        assert len(payload["comments"]) == 1
        assert payload["comments"][0]["author"] == "alice"

    def test_content_type_json(self, mapper):
        doc = mapper.map_issue(self._make_issue())
        assert doc.content_type == "application/json"

    def test_cursor_updated_at(self, mapper):
        doc = mapper.map_issue(self._make_issue())
        assert doc.cursor.value == "2024-01-15T12:00:00Z"
        assert doc.cursor.source_type == "timestamp"

    def test_tags_contiennent_state(self, mapper):
        doc = mapper.map_issue(self._make_issue())
        assert "issue" in doc.tags
        assert "state:open" in doc.tags


class TestMapPullRequest:
    def _make_pr(self, **overrides) -> GitHubPullRequest:
        defaults = dict(
            number=99, title="feat: add dark mode", body="Implements dark mode",
            state="merged", html_url="https://github.com/org/repo/pull/99",
            created_at="2024-01-12T08:00:00Z", updated_at="2024-01-16T14:00:00Z",
            merged_at="2024-01-16T14:00:00Z", base_branch="main", head_branch="feat/dark-mode",
            head_sha="abcdef123456", author="carol", labels=("enhancement",),
            reviewers=("dave",), repo_full_name="org/repo", draft=False,
            reviews=[{"user": {"login": "dave"}, "state": "APPROVED", "body": "LGTM", "submitted_at": "2024-01-16T13:00:00Z"}],
            comments=[], diff="diff --git a/style.css b/style.css\n+.dark { background: #000; }",
        )
        defaults.update(overrides)
        return GitHubPullRequest(**defaults)

    def test_contenu_json_complet(self, mapper):
        doc = mapper.map_pull_request(self._make_pr())
        payload = json.loads(doc.content)
        assert payload["number"] == 99
        assert payload["merged"] is True
        assert payload["diff"] != ""
        assert payload["reviews"][0]["state"] == "APPROVED"

    def test_tags_pull_request(self, mapper):
        doc = mapper.map_pull_request(self._make_pr())
        assert "pull_request" in doc.tags
        assert "state:merged" in doc.tags


class TestMapRelease:
    def _make_release(self) -> GitHubRelease:
        return GitHubRelease(
            id=12345, tag_name="v2.1.0", name="Release 2.1.0",
            body="## What's new\n- Dark mode\n- Performance improvements",
            html_url="https://github.com/org/repo/releases/tag/v2.1.0",
            created_at="2024-01-17T10:00:00Z", published_at="2024-01-17T11:00:00Z",
            author="alice", prerelease=False, draft=False,
            assets=({"name": "app-linux.tar.gz", "size": 10240, "browser_download_url": "https://github.com/.../app-linux.tar.gz"},),
            repo_full_name="org/repo",
        )

    def test_contenu_json(self, mapper):
        doc = mapper.map_release(self._make_release())
        payload = json.loads(doc.content)
        assert payload["tag_name"] == "v2.1.0"
        assert len(payload["assets"]) == 1
        assert "Dark mode" in payload["body"]

    def test_cursor_sequence(self, mapper):
        doc = mapper.map_release(self._make_release())
        assert doc.cursor.value == "12345"
        assert doc.cursor.source_type == "sequence"


class TestMapCommit:
    def _make_commit(self) -> GitHubCommit:
        return GitHubCommit(
            sha="a1b2c3d4e5f6" * 3 + "a1b2",
            message="fix: resolve race condition in worker pool",
            author_name="Eve", author_email="eve@example.com",
            author_date="2024-01-18T09:00:00Z",
            committer_name="Eve", committer_date="2024-01-18T09:05:00Z",
            html_url="https://github.com/org/repo/commit/a1b2c3d4",
            repo_full_name="org/repo", parents=("prev_sha",),
            stats_additions=15, stats_deletions=3, stats_total=18,
            files_changed=[{"filename": "worker.py", "status": "modified", "additions": 15, "deletions": 3, "patch": "@@ -10,3 +10,15 @@"}],
        )

    def test_contenu_json(self, mapper):
        doc = mapper.map_commit(self._make_commit())
        payload = json.loads(doc.content)
        assert "race condition" in payload["message"]
        assert payload["stats"]["additions"] == 15
        assert len(payload["files_changed"]) == 1

    def test_tags_commit(self, mapper):
        doc = mapper.map_commit(self._make_commit())
        assert "commit" in doc.tags
