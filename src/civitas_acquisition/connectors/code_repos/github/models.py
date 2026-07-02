"""
GitHub-specific value objects.

Ces modèles représentent les ressources GitHub dans leur forme native,
avant mapping vers RawDocument. Ils sont internes au connecteur GitHub
et ne font PAS partie des contracts de la plateforme.

Types de ressources supportés :
  - Repository (métadonnées)
  - File (contenu d'un fichier dans un tree)
  - Issue (avec comments)
  - PullRequest (avec reviews, comments, diff)
  - Release (avec assets)
  - Commit (avec diff/stats)
  - WebhookEvent (événement inbound)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Optional


class GitHubResourceType(Enum):
    """Types de ressources fetchables depuis GitHub."""
    FILE        = auto()
    ISSUE       = auto()
    PULL_REQUEST = auto()
    RELEASE     = auto()
    COMMIT      = auto()
    REPOSITORY  = auto()
    DISCUSSION  = auto()


@dataclass(frozen=True)
class GitHubRepo:
    full_name: str           # "owner/repo"
    default_branch: str
    private: bool
    clone_url: str
    description: Optional[str]
    language: Optional[str]
    topics: tuple[str, ...]
    stars: int
    url: str

    @classmethod
    def from_api(cls, data: dict[str, Any]) -> GitHubRepo:
        return cls(
            full_name=data["full_name"],
            default_branch=data.get("default_branch", "main"),
            private=data.get("private", False),
            clone_url=data.get("clone_url", ""),
            description=data.get("description"),
            language=data.get("language"),
            topics=tuple(data.get("topics", [])),
            stars=data.get("stargazers_count", 0),
            url=data.get("html_url", ""),
        )


@dataclass(frozen=True)
class GitHubFile:
    path: str
    sha: str                  # Blob SHA — change si le contenu change
    size: int
    url: str                  # API URL pour récupérer le contenu
    html_url: str             # URL web
    repo_full_name: str
    branch: str
    encoding: str = "base64"  # GitHub encode toujours en base64

    @property
    def extension(self) -> str:
        return "." + self.path.rsplit(".", 1)[-1] if "." in self.path else ""

    @classmethod
    def from_tree_item(
        cls, item: dict[str, Any], repo_full_name: str, branch: str
    ) -> GitHubFile:
        return cls(
            path=item["path"],
            sha=item["sha"],
            size=item.get("size", 0),
            url=item.get("url", ""),
            html_url=f"https://github.com/{repo_full_name}/blob/{branch}/{item['path']}",
            repo_full_name=repo_full_name,
            branch=branch,
        )


@dataclass(frozen=True)
class GitHubIssue:
    number: int
    title: str
    body: Optional[str]
    state: str                     # "open" | "closed"
    html_url: str
    created_at: str                # ISO-8601
    updated_at: str
    closed_at: Optional[str]
    labels: tuple[str, ...]
    assignees: tuple[str, ...]
    author: str
    comments_count: int
    milestone: Optional[str]
    repo_full_name: str
    comments: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_api(cls, data: dict[str, Any], repo_full_name: str) -> GitHubIssue:
        return cls(
            number=data["number"],
            title=data["title"],
            body=data.get("body"),
            state=data["state"],
            html_url=data["html_url"],
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            closed_at=data.get("closed_at"),
            labels=tuple(lb["name"] for lb in data.get("labels", [])),
            assignees=tuple(a["login"] for a in data.get("assignees", [])),
            author=data["user"]["login"],
            comments_count=data.get("comments", 0),
            milestone=data["milestone"]["title"] if data.get("milestone") else None,
            repo_full_name=repo_full_name,
        )


@dataclass(frozen=True)
class GitHubPullRequest:
    number: int
    title: str
    body: Optional[str]
    state: str                       # "open" | "closed" | "merged"
    html_url: str
    created_at: str
    updated_at: str
    merged_at: Optional[str]
    base_branch: str
    head_branch: str
    head_sha: str
    author: str
    labels: tuple[str, ...]
    reviewers: tuple[str, ...]
    repo_full_name: str
    draft: bool = False
    reviews: list[dict[str, Any]] = field(default_factory=list)
    comments: list[dict[str, Any]] = field(default_factory=list)
    diff: Optional[str] = None       # Unified diff

    @property
    def is_merged(self) -> bool:
        return self.merged_at is not None

    @classmethod
    def from_api(cls, data: dict[str, Any], repo_full_name: str) -> GitHubPullRequest:
        return cls(
            number=data["number"],
            title=data["title"],
            body=data.get("body"),
            state="merged" if data.get("merged_at") else data["state"],
            html_url=data["html_url"],
            created_at=data["created_at"],
            updated_at=data["updated_at"],
            merged_at=data.get("merged_at"),
            base_branch=data["base"]["ref"],
            head_branch=data["head"]["ref"],
            head_sha=data["head"]["sha"],
            author=data["user"]["login"],
            labels=tuple(lb["name"] for lb in data.get("labels", [])),
            reviewers=tuple(
                r["login"] for r in data.get("requested_reviewers", [])
            ),
            draft=data.get("draft", False),
            repo_full_name=repo_full_name,
        )


@dataclass(frozen=True)
class GitHubRelease:
    id: int
    tag_name: str
    name: Optional[str]
    body: Optional[str]
    html_url: str
    created_at: str
    published_at: Optional[str]
    author: str
    prerelease: bool
    draft: bool
    assets: tuple[dict[str, Any], ...]
    repo_full_name: str

    @classmethod
    def from_api(cls, data: dict[str, Any], repo_full_name: str) -> GitHubRelease:
        return cls(
            id=data["id"],
            tag_name=data["tag_name"],
            name=data.get("name"),
            body=data.get("body"),
            html_url=data["html_url"],
            created_at=data["created_at"],
            published_at=data.get("published_at"),
            author=data["author"]["login"],
            prerelease=data.get("prerelease", False),
            draft=data.get("draft", False),
            assets=tuple(data.get("assets", [])),
            repo_full_name=repo_full_name,
        )


@dataclass(frozen=True)
class GitHubCommit:
    sha: str
    message: str
    author_name: str
    author_email: str
    author_date: str
    committer_name: str
    committer_date: str
    html_url: str
    repo_full_name: str
    parents: tuple[str, ...]
    stats_additions: int = 0
    stats_deletions: int = 0
    stats_total: int = 0
    files_changed: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_api(cls, data: dict[str, Any], repo_full_name: str) -> GitHubCommit:
        commit = data.get("commit", {})
        author = commit.get("author", {})
        committer = commit.get("committer", {})
        stats = data.get("stats", {})
        return cls(
            sha=data["sha"],
            message=commit.get("message", ""),
            author_name=author.get("name", ""),
            author_email=author.get("email", ""),
            author_date=author.get("date", ""),
            committer_name=committer.get("name", ""),
            committer_date=committer.get("date", ""),
            html_url=data.get("html_url", ""),
            repo_full_name=repo_full_name,
            parents=tuple(p["sha"] for p in data.get("parents", [])),
            stats_additions=stats.get("additions", 0),
            stats_deletions=stats.get("deletions", 0),
            stats_total=stats.get("total", 0),
            files_changed=list(data.get("files", [])),
        )


@dataclass(frozen=True)
class RateLimitInfo:
    """État courant du rate limit GitHub."""
    limit: int
    remaining: int
    reset_at: float      # Unix timestamp
    used: int
    resource: str        # "core", "search", "graphql"

    @property
    def is_exhausted(self) -> bool:
        return self.remaining == 0

    @property
    def utilization_pct(self) -> float:
        if self.limit == 0:
            return 0.0
        return (self.used / self.limit) * 100

    @classmethod
    def from_headers(cls, headers: dict[str, str]) -> Optional[RateLimitInfo]:
        try:
            return cls(
                limit=int(headers.get("X-RateLimit-Limit", 0)),
                remaining=int(headers.get("X-RateLimit-Remaining", 0)),
                reset_at=float(headers.get("X-RateLimit-Reset", 0)),
                used=int(headers.get("X-RateLimit-Used", 0)),
                resource=headers.get("X-RateLimit-Resource", "core"),
            )
        except (ValueError, KeyError):
            return None
