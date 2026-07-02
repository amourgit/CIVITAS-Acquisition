"""
GitHubMapper — convertit les ressources GitHub natives en RawDocument.

Chaque type de ressource a sa propre méthode de mapping.
La conversion produit un RawDocument canonique avec :
  - content    : bytes représentant la ressource (texte brut ou JSON)
  - content_type : MIME type approprié
  - source_metadata : champs GitHub verbatim utiles pour le downstream

Le downstream (Transformation Platform) utilisera source_metadata
pour enrichir le document. On ne perd rien ici.
"""
from __future__ import annotations

import json
import mimetypes
from typing import Optional

from civitas_acquisition.connectors.code_repos.github.models import (
    GitHubFile,
    GitHubIssue,
    GitHubPullRequest,
    GitHubRelease,
    GitHubCommit,
    GitHubRepo,
)
from civitas_acquisition.contracts.models.cursor import Cursor
from civitas_acquisition.contracts.models.raw_document import RawDocument

# Extensions textuelles connues
_TEXT_EXTENSIONS = frozenset([
    ".py", ".js", ".ts", ".jsx", ".tsx", ".go", ".rs", ".java",
    ".c", ".cpp", ".h", ".hpp", ".cs", ".rb", ".php", ".swift",
    ".kt", ".scala", ".r", ".m", ".lua", ".pl", ".sh", ".bash",
    ".zsh", ".fish", ".ps1", ".bat", ".cmd",
    ".md", ".mdx", ".rst", ".txt", ".log", ".csv", ".tsv",
    ".json", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf",
    ".env", ".xml", ".html", ".htm", ".css", ".scss", ".sass",
    ".sql", ".graphql", ".proto", ".tf", ".hcl",
    ".dockerfile", ".makefile", ".gitignore", ".editorconfig",
])


def _detect_mime_type(path: str) -> str:
    """Détecte le MIME type d'un fichier par son extension."""
    ext = "." + path.rsplit(".", 1)[-1].lower() if "." in path else ""
    if ext in _TEXT_EXTENSIONS:
        # Mapping précis pour les types communs
        _explicit = {
            ".py": "text/x-python",
            ".js": "text/javascript",
            ".ts": "text/typescript",
            ".md": "text/markdown",
            ".json": "application/json",
            ".yaml": "application/yaml",
            ".yml": "application/yaml",
            ".html": "text/html",
            ".css": "text/css",
            ".sql": "text/x-sql",
            ".sh": "text/x-shellscript",
            ".xml": "text/xml",
        }
        return _explicit.get(ext, "text/plain")
    mime, _ = mimetypes.guess_type("file" + ext)
    return mime or "application/octet-stream"


class GitHubMapper:
    """
    Convertit les ressources GitHub natives en RawDocument canoniques.
    Injecte instance_id et connector_id dans chaque document.
    """

    def __init__(self, instance_id: str, connector_id: str = "github") -> None:
        self._instance_id = instance_id
        self._connector_id = connector_id

    # ── Files ─────────────────────────────────────────────────────────────────

    def map_file(
        self,
        file: GitHubFile,
        content: bytes,
        tree_sha: str,
    ) -> RawDocument:
        """Fichier source → RawDocument avec contenu brut."""
        return RawDocument.create(
            instance_id=self._instance_id,
            connector_id=self._connector_id,
            uri=file.html_url,
            content=content,
            content_type=_detect_mime_type(file.path),
            version=file.sha,
            cursor=Cursor(
                value=tree_sha,
                source_type="token",
                connector_id=self._connector_id,
                instance_id=self._instance_id,
            ),
            tags=("file", f"repo:{file.repo_full_name}", f"branch:{file.branch}"),
            source_metadata={
                "resource_type": "file",
                "repo": file.repo_full_name,
                "branch": file.branch,
                "path": file.path,
                "sha": file.sha,
                "size": file.size,
                "tree_sha": tree_sha,
                "html_url": file.html_url,
            },
        )

    # ── Issues ────────────────────────────────────────────────────────────────

    def map_issue(self, issue: GitHubIssue) -> RawDocument:
        """Issue GitHub → RawDocument JSON."""
        payload = {
            "number": issue.number,
            "title": issue.title,
            "body": issue.body or "",
            "state": issue.state,
            "labels": list(issue.labels),
            "assignees": list(issue.assignees),
            "author": issue.author,
            "created_at": issue.created_at,
            "updated_at": issue.updated_at,
            "closed_at": issue.closed_at,
            "milestone": issue.milestone,
            "comments": [
                {
                    "author": c.get("user", {}).get("login", ""),
                    "body": c.get("body", ""),
                    "created_at": c.get("created_at", ""),
                    "updated_at": c.get("updated_at", ""),
                }
                for c in issue.comments
            ],
        }
        content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

        return RawDocument.create(
            instance_id=self._instance_id,
            connector_id=self._connector_id,
            uri=issue.html_url,
            content=content,
            content_type="application/json",
            version=issue.updated_at,
            cursor=Cursor(
                value=issue.updated_at,
                source_type="timestamp",
                connector_id=self._connector_id,
                instance_id=self._instance_id,
            ),
            tags=("issue", f"repo:{issue.repo_full_name}", f"state:{issue.state}"),
            source_metadata={
                "resource_type": "issue",
                "repo": issue.repo_full_name,
                "number": issue.number,
                "state": issue.state,
                "title": issue.title,
                "labels": list(issue.labels),
                "author": issue.author,
                "updated_at": issue.updated_at,
            },
        )

    # ── Pull Requests ─────────────────────────────────────────────────────────

    def map_pull_request(self, pr: GitHubPullRequest) -> RawDocument:
        """Pull Request → RawDocument JSON avec diff."""
        payload = {
            "number": pr.number,
            "title": pr.title,
            "body": pr.body or "",
            "state": pr.state,
            "base_branch": pr.base_branch,
            "head_branch": pr.head_branch,
            "head_sha": pr.head_sha,
            "author": pr.author,
            "labels": list(pr.labels),
            "reviewers": list(pr.reviewers),
            "draft": pr.draft,
            "merged": pr.is_merged,
            "created_at": pr.created_at,
            "updated_at": pr.updated_at,
            "merged_at": pr.merged_at,
            "reviews": [
                {
                    "author": r.get("user", {}).get("login", ""),
                    "state": r.get("state", ""),
                    "body": r.get("body", ""),
                    "submitted_at": r.get("submitted_at", ""),
                }
                for r in pr.reviews
            ],
            "comments": [
                {
                    "author": c.get("user", {}).get("login", ""),
                    "body": c.get("body", ""),
                    "path": c.get("path", ""),
                    "line": c.get("line"),
                    "created_at": c.get("created_at", ""),
                }
                for c in pr.comments
            ],
            "diff": pr.diff or "",
        }
        content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

        return RawDocument.create(
            instance_id=self._instance_id,
            connector_id=self._connector_id,
            uri=pr.html_url,
            content=content,
            content_type="application/json",
            version=pr.updated_at,
            cursor=Cursor(
                value=pr.updated_at,
                source_type="timestamp",
                connector_id=self._connector_id,
                instance_id=self._instance_id,
            ),
            tags=("pull_request", f"repo:{pr.repo_full_name}", f"state:{pr.state}"),
            source_metadata={
                "resource_type": "pull_request",
                "repo": pr.repo_full_name,
                "number": pr.number,
                "state": pr.state,
                "title": pr.title,
                "author": pr.author,
                "merged": pr.is_merged,
                "head_sha": pr.head_sha,
                "updated_at": pr.updated_at,
            },
        )

    # ── Releases ──────────────────────────────────────────────────────────────

    def map_release(self, release: GitHubRelease) -> RawDocument:
        """Release GitHub → RawDocument JSON."""
        payload = {
            "id": release.id,
            "tag_name": release.tag_name,
            "name": release.name or "",
            "body": release.body or "",
            "prerelease": release.prerelease,
            "draft": release.draft,
            "author": release.author,
            "created_at": release.created_at,
            "published_at": release.published_at,
            "assets": [
                {
                    "name": a.get("name", ""),
                    "size": a.get("size", 0),
                    "download_url": a.get("browser_download_url", ""),
                }
                for a in release.assets
            ],
        }
        content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

        return RawDocument.create(
            instance_id=self._instance_id,
            connector_id=self._connector_id,
            uri=release.html_url,
            content=content,
            content_type="application/json",
            version=str(release.id),
            cursor=Cursor(
                value=str(release.id),
                source_type="sequence",
                connector_id=self._connector_id,
                instance_id=self._instance_id,
            ),
            tags=("release", f"repo:{release.repo_full_name}", release.tag_name),
            source_metadata={
                "resource_type": "release",
                "repo": release.repo_full_name,
                "tag_name": release.tag_name,
                "release_id": release.id,
                "prerelease": release.prerelease,
                "published_at": release.published_at,
            },
        )

    # ── Commits ───────────────────────────────────────────────────────────────

    def map_commit(self, commit: GitHubCommit) -> RawDocument:
        """Commit GitHub → RawDocument JSON."""
        payload = {
            "sha": commit.sha,
            "message": commit.message,
            "author": {
                "name": commit.author_name,
                "email": commit.author_email,
                "date": commit.author_date,
            },
            "committer": {
                "name": commit.committer_name,
                "date": commit.committer_date,
            },
            "parents": list(commit.parents),
            "stats": {
                "additions": commit.stats_additions,
                "deletions": commit.stats_deletions,
                "total": commit.stats_total,
            },
            "files_changed": [
                {
                    "filename": f.get("filename", ""),
                    "status": f.get("status", ""),
                    "additions": f.get("additions", 0),
                    "deletions": f.get("deletions", 0),
                    "patch": f.get("patch", ""),
                }
                for f in commit.files_changed[:50]  # cap à 50 fichiers
            ],
        }
        content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

        return RawDocument.create(
            instance_id=self._instance_id,
            connector_id=self._connector_id,
            uri=commit.html_url,
            content=content,
            content_type="application/json",
            version=commit.sha,
            cursor=Cursor(
                value=commit.author_date or commit.committer_date,
                source_type="timestamp",
                connector_id=self._connector_id,
                instance_id=self._instance_id,
            ),
            tags=("commit", f"repo:{commit.repo_full_name}"),
            source_metadata={
                "resource_type": "commit",
                "repo": commit.repo_full_name,
                "sha": commit.sha,
                "author": commit.author_name,
                "date": commit.author_date,
                "stats_total": commit.stats_total,
            },
        )

    # ── Repository metadata ───────────────────────────────────────────────────

    def map_repo(self, repo: GitHubRepo) -> RawDocument:
        """Métadonnées repository → RawDocument JSON."""
        payload = {
            "full_name": repo.full_name,
            "description": repo.description or "",
            "language": repo.language,
            "topics": list(repo.topics),
            "stars": repo.stars,
            "private": repo.private,
            "default_branch": repo.default_branch,
            "url": repo.url,
        }
        content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")

        return RawDocument.create(
            instance_id=self._instance_id,
            connector_id=self._connector_id,
            uri=repo.url,
            content=content,
            content_type="application/json",
            source_metadata={
                "resource_type": "repository",
                "repo": repo.full_name,
                "language": repo.language,
                "stars": repo.stars,
            },
        )
