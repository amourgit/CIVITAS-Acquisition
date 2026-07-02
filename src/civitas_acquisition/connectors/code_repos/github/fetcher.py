"""
GitHubFetcher — récupération de toutes les ressources GitHub.

Une méthode par type de ressource. Chaque méthode est un async generator
qui yield des objets GitHub natifs (pas encore des RawDocuments — c'est
le rôle du Mapper).

Ressources supportées :
  - files       : arbre de fichiers + contenu (via git tree + blob API)
  - issues      : issues + commentaires
  - pull_requests: PRs + reviews + commentaires + diff
  - releases    : releases + assets
  - commits     : commits + stats + fichiers modifiés
  - repo_meta   : métadonnées du repository

Cursor par ressource :
  - files       : SHA du tree HEAD (ne refetch que si HEAD a changé)
  - issues/prs  : since=updated_at (ISO-8601)
  - releases    : since=created_at
  - commits     : since=committer.date
"""
from __future__ import annotations

import base64
import logging
from typing import AsyncIterator, Optional

from civitas_acquisition.connectors.code_repos.github.client import (
    GitHubClient,
    ResourceNotFoundError,
)
from civitas_acquisition.connectors.code_repos.github.models import (
    GitHubFile,
    GitHubIssue,
    GitHubPullRequest,
    GitHubRelease,
    GitHubCommit,
    GitHubRepo,
)

logger = logging.getLogger(__name__)

# Extensions exclues par défaut (binaires non textuels)
DEFAULT_EXCLUDED_EXTENSIONS = frozenset([
    ".png", ".jpg", ".jpeg", ".gif", ".ico", ".svg",
    ".pdf", ".zip", ".tar", ".gz", ".bz2", ".7z",
    ".exe", ".dll", ".so", ".dylib", ".whl",
    ".pyc", ".pyo", ".class",
    ".mp4", ".mp3", ".avi", ".mov", ".wav",
    ".ttf", ".woff", ".woff2", ".eot",
    ".db", ".sqlite",
])

MAX_FILE_SIZE_BYTES = 1 * 1024 * 1024  # 1MB par défaut


class GitHubFetcher:
    """
    Récupère les ressources GitHub depuis l'API REST.

    Toutes les méthodes sont des async generators qui yielden
    des objets GitHub natifs avec leur contenu brut.
    """

    def __init__(
        self,
        client: GitHubClient,
        max_file_size: int = MAX_FILE_SIZE_BYTES,
        excluded_extensions: frozenset[str] = DEFAULT_EXCLUDED_EXTENSIONS,
        include_closed_issues: bool = True,
        include_closed_prs: bool = True,
        include_drafts: bool = False,
        include_prereleases: bool = True,
    ) -> None:
        self._client = client
        self._max_file_size = max_file_size
        self._excluded_extensions = excluded_extensions
        self._include_closed_issues = include_closed_issues
        self._include_closed_prs = include_closed_prs
        self._include_drafts = include_drafts
        self._include_prereleases = include_prereleases

    # ── Repository ────────────────────────────────────────────────────────────

    async def fetch_repo(self, full_name: str) -> Optional[GitHubRepo]:
        """Métadonnées d'un repository."""
        try:
            data = await self._client.get(f"/repos/{full_name}")
            return GitHubRepo.from_api(data)
        except ResourceNotFoundError:
            logger.warning("Repo not found: %s", full_name)
            return None

    async def list_repos(
        self,
        owner: Optional[str] = None,
        org: Optional[str] = None,
    ) -> AsyncIterator[GitHubRepo]:
        """Liste tous les repos d'un user ou d'une org."""
        if org:
            path = f"/orgs/{org}/repos"
        elif owner:
            path = f"/users/{owner}/repos"
        else:
            path = "/user/repos"   # repos de l'utilisateur authentifié

        async for page in self._client.paginate(path, params={"type": "all"}):
            for item in page:
                yield GitHubRepo.from_api(item)

    # ── Files ─────────────────────────────────────────────────────────────────

    async def fetch_files(
        self,
        repo: str,
        branch: str = "HEAD",
        since_tree_sha: Optional[str] = None,
        file_patterns: Optional[list[str]] = None,
    ) -> AsyncIterator[tuple[GitHubFile, bytes]]:
        """
        Récupère tous les fichiers d'un repo via l'API git tree.
        Yields (GitHubFile, content_bytes).

        since_tree_sha : si fourni et que le tree SHA n'a pas changé,
                         ne yield rien (delta optimization).
        """
        # Récupérer le SHA du HEAD
        try:
            ref_data = await self._client.get(f"/repos/{repo}/git/ref/heads/{branch}")
        except ResourceNotFoundError:
            # Essayer de récupérer le branch depuis l'API
            try:
                ref_data = await self._client.get(f"/repos/{repo}/branches/{branch}")
                tree_sha = ref_data["commit"]["commit"]["tree"]["sha"]
                head_sha = ref_data["commit"]["sha"]
            except ResourceNotFoundError:
                logger.warning("Branch %s not found in %s", branch, repo)
                return
        else:
            head_sha = ref_data["object"]["sha"]
            # Récupérer le tree SHA du commit
            commit_data = await self._client.get(f"/repos/{repo}/git/commits/{head_sha}")
            tree_sha = commit_data["tree"]["sha"]

        if since_tree_sha and tree_sha == since_tree_sha:
            logger.debug("Tree SHA unchanged for %s@%s — skipping", repo, branch)
            return

        # Récupérer le tree récursif
        tree_data = await self._client.get(
            f"/repos/{repo}/git/trees/{tree_sha}",
            params={"recursive": "1"},
        )

        if not tree_data:
            return

        blobs = [
            item for item in tree_data.get("tree", [])
            if item["type"] == "blob"
        ]

        logger.info(
            "Fetching %d files from %s@%s (tree=%s)",
            len(blobs), repo, branch, tree_sha[:8],
        )

        for item in blobs:
            path = item["path"]
            size = item.get("size", 0)

            # Filtres
            ext = "." + path.rsplit(".", 1)[-1].lower() if "." in path else ""
            if ext in self._excluded_extensions:
                continue
            if size > self._max_file_size:
                logger.debug("Skipping large file %s (%d bytes)", path, size)
                continue
            if file_patterns and not self._matches_patterns(path, file_patterns):
                continue

            # Récupérer le contenu via l'API blob
            try:
                content = await self._fetch_blob_content(repo, item["sha"], item.get("url", ""))
                if content is None:
                    continue
                file_info = GitHubFile.from_tree_item(item, repo, branch)
                # Stocker le tree_sha dans l'objet via les metadata
                yield file_info, content
            except ResourceNotFoundError:
                logger.debug("Blob not found: %s in %s", path, repo)
            except Exception as exc:
                logger.warning("Error fetching %s: %s", path, exc)

    async def _fetch_blob_content(
        self, repo: str, blob_sha: str, url: str
    ) -> Optional[bytes]:
        """Récupère le contenu d'un blob GitHub (base64 decoded)."""
        blob_url = url or f"/repos/{repo}/git/blobs/{blob_sha}"
        try:
            data = await self._client.get(blob_url)
            if not data:
                return None
            encoding = data.get("encoding", "base64")
            if encoding == "base64":
                return base64.b64decode(data["content"].replace("\n", ""))
            elif encoding == "utf-8":
                return data["content"].encode("utf-8")
            else:
                logger.warning("Unknown blob encoding: %s", encoding)
                return None
        except ResourceNotFoundError:
            return None

    # ── Issues ────────────────────────────────────────────────────────────────

    async def fetch_issues(
        self,
        repo: str,
        since: Optional[str] = None,
        state: str = "all",
    ) -> AsyncIterator[GitHubIssue]:
        """
        Récupère les issues d'un repo avec leurs commentaires.
        since : ISO-8601 timestamp (updated_at >= since)
        Exclut les pull requests (GitHub les liste aussi dans /issues).
        """
        params: dict = {"state": state, "sort": "updated", "direction": "asc"}
        if since:
            params["since"] = since

        async for page in self._client.paginate(f"/repos/{repo}/issues", params=params):
            for item in page:
                # Exclure les PRs (GitHub les inclut dans /issues)
                if "pull_request" in item:
                    continue

                issue = GitHubIssue.from_api(item, repo)

                # Récupérer les commentaires si présents
                if issue.comments_count > 0:
                    comments = await self._client.collect_all(
                        f"/repos/{repo}/issues/{issue.number}/comments"
                    )
                    issue = GitHubIssue(
                        **{**issue.__dict__, "comments": comments}
                    )

                yield issue

    # ── Pull Requests ─────────────────────────────────────────────────────────

    async def fetch_pull_requests(
        self,
        repo: str,
        since: Optional[str] = None,
        state: str = "all",
    ) -> AsyncIterator[GitHubPullRequest]:
        """
        Récupère les PRs avec reviews, commentaires et diff.
        """
        params: dict = {"state": state, "sort": "updated", "direction": "asc"}

        async for page in self._client.paginate(f"/repos/{repo}/pulls", params=params):
            for item in page:
                if not self._include_drafts and item.get("draft", False):
                    continue

                pr = GitHubPullRequest.from_api(item, repo)

                # Filtre since basé sur updated_at
                if since and pr.updated_at < since:
                    continue

                # Reviews
                reviews = await self._client.collect_all(
                    f"/repos/{repo}/pulls/{pr.number}/reviews"
                )

                # Comments (review comments)
                comments = await self._client.collect_all(
                    f"/repos/{repo}/pulls/{pr.number}/comments"
                )

                # Diff (unified diff)
                diff_bytes = b""
                try:
                    diff_bytes = await self._client.get_raw(
                        f"https://api.github.com/repos/{repo}/pulls/{pr.number}",
                    )
                except Exception:
                    pass

                yield GitHubPullRequest(
                    **{
                        **{k: getattr(pr, k) for k in pr.__dataclass_fields__},
                        "reviews": reviews,
                        "comments": comments,
                        "diff": diff_bytes.decode("utf-8", errors="replace") if diff_bytes else None,
                    }
                )

    # ── Releases ──────────────────────────────────────────────────────────────

    async def fetch_releases(
        self,
        repo: str,
        since_id: Optional[int] = None,
    ) -> AsyncIterator[GitHubRelease]:
        """
        Récupère les releases d'un repo.
        since_id : ID de la dernière release connue.
        """
        async for page in self._client.paginate(f"/repos/{repo}/releases"):
            for item in page:
                if not self._include_prereleases and item.get("prerelease", False):
                    continue
                release = GitHubRelease.from_api(item, repo)
                if since_id and release.id <= since_id:
                    continue
                yield release

    # ── Commits ───────────────────────────────────────────────────────────────

    async def fetch_commits(
        self,
        repo: str,
        branch: str = "HEAD",
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> AsyncIterator[GitHubCommit]:
        """
        Récupère les commits d'un repo avec stats et fichiers modifiés.
        """
        params: dict = {"sha": branch}
        if since:
            params["since"] = since
        if until:
            params["until"] = until

        async for page in self._client.paginate(f"/repos/{repo}/commits", params=params):
            for item in page:
                sha = item["sha"]
                # Récupérer les détails complets (stats + files)
                try:
                    detail = await self._client.get(
                        f"/repos/{repo}/commits/{sha}", use_etag=True
                    )
                    if detail:
                        yield GitHubCommit.from_api(detail, repo)
                except ResourceNotFoundError:
                    yield GitHubCommit.from_api(item, repo)

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _matches_patterns(self, path: str, patterns: list[str]) -> bool:
        """Vérifie si un chemin correspond à l'un des glob patterns."""
        import fnmatch
        return any(fnmatch.fnmatch(path, p) for p in patterns)
