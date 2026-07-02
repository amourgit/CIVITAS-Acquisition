"""
GitHubOperations — opérations d'écriture sur GitHub.

Inspiré par les actions Activepieces (create-issue, update-issue,
create-branch, create-pull-request, etc.) mais adapté à notre
architecture hexagonale.

Ces opérations permettent à la plateforme CIVITAS d'interagir avec
GitHub en écriture, pas seulement en lecture. Utile pour :
  - Créer des issues depuis des agents
  - Commenter automatiquement des PRs
  - Créer des branches de travail
  - Merger des PRs validées

Toutes les méthodes retournent des dicts bruts de l'API GitHub.
Le caller est responsable du mapping si nécessaire.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from civitas_acquisition.connectors.code_repos.github.client import GitHubClient
from civitas_acquisition.contracts.errors.connector_errors import ConnectorFatalError

logger = logging.getLogger(__name__)


class GitHubOperations:
    """
    Opérations d'écriture sur l'API GitHub.

    Injecte un GitHubClient — n'ouvre/ferme pas de session lui-même.
    """

    def __init__(self, client: GitHubClient) -> None:
        self._client = client

    # ── Issues ────────────────────────────────────────────────────────────────

    async def create_issue(
        self,
        repo: str,
        title: str,
        body: Optional[str] = None,
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
        milestone: Optional[int] = None,
    ) -> dict[str, Any]:
        """
        Crée une nouvelle issue.
        Non-idempotent : chaque appel crée une nouvelle issue.
        Pattern Activepieces : create-issue.ts
        """
        payload: dict[str, Any] = {"title": title}
        if body:        payload["body"]      = body
        if labels:      payload["labels"]    = labels
        if assignees:   payload["assignees"] = assignees
        if milestone:   payload["milestone"] = milestone

        owner, repo_name = _parse_repo(repo)
        return await self._client.post(
            f"/repos/{owner}/{repo_name}/issues",
            body=payload,
        )

    async def update_issue(
        self,
        repo: str,
        issue_number: int,
        title: Optional[str] = None,
        body: Optional[str] = None,
        state: Optional[str] = None,          # "open" | "closed"
        state_reason: Optional[str] = None,   # "completed" | "not_planned" | "reopened"
        labels: list[str] | None = None,
        assignees: list[str] | None = None,
        milestone: Optional[int] = None,
    ) -> dict[str, Any]:
        """
        Met à jour une issue existante. Idempotent.
        Pattern Activepieces : update-issue.ts
        """
        payload: dict[str, Any] = {}
        if title is not None:        payload["title"]        = title
        if body is not None:         payload["body"]         = body
        if state is not None:        payload["state"]        = state
        if state_reason is not None: payload["state_reason"] = state_reason
        if labels is not None:       payload["labels"]       = labels
        if assignees is not None:    payload["assignees"]    = assignees
        if milestone is not None:    payload["milestone"]    = milestone

        if not payload:
            raise ConnectorFatalError("update_issue: at least one field must be provided")

        owner, repo_name = _parse_repo(repo)
        return await self._client.patch(
            f"/repos/{owner}/{repo_name}/issues/{issue_number}",
            body=payload,
        )

    async def lock_issue(
        self,
        repo: str,
        issue_number: int,
        reason: Optional[str] = None,  # "off-topic" | "too heated" | "resolved" | "spam"
    ) -> None:
        """Verrouille une issue. Pattern Activepieces : lock-issue.ts"""
        owner, repo_name = _parse_repo(repo)
        body = {"lock_reason": reason} if reason else {}
        await self._client.post(
            f"/repos/{owner}/{repo_name}/issues/{issue_number}/lock",
            body=body,
        )

    async def unlock_issue(self, repo: str, issue_number: int) -> None:
        """Déverrouille une issue. Pattern Activepieces : unlock-issue.ts"""
        owner, repo_name = _parse_repo(repo)
        await self._client.delete(
            f"/repos/{owner}/{repo_name}/issues/{issue_number}/lock"
        )

    async def add_labels_to_issue(
        self, repo: str, issue_number: int, labels: list[str]
    ) -> list[dict]:
        """Ajoute des labels à une issue. Pattern Activepieces : add-labels-to-issue.ts"""
        owner, repo_name = _parse_repo(repo)
        return await self._client.post(
            f"/repos/{owner}/{repo_name}/issues/{issue_number}/labels",
            body={"labels": labels},
        )

    # ── Comments ──────────────────────────────────────────────────────────────

    async def create_issue_comment(
        self, repo: str, issue_number: int, body: str
    ) -> dict[str, Any]:
        """Ajoute un commentaire à une issue ou PR."""
        owner, repo_name = _parse_repo(repo)
        return await self._client.post(
            f"/repos/{owner}/{repo_name}/issues/{issue_number}/comments",
            body={"body": body},
        )

    async def create_pr_review_comment(
        self,
        repo: str,
        pr_number: int,
        body: str,
        commit_id: str,
        path: str,
        line: Optional[int] = None,
    ) -> dict[str, Any]:
        """Crée un commentaire de review sur une PR. Pattern Activepieces."""
        owner, repo_name = _parse_repo(repo)
        payload: dict[str, Any] = {
            "body": body,
            "commit_id": commit_id,
            "path": path,
        }
        if line: payload["line"] = line
        return await self._client.post(
            f"/repos/{owner}/{repo_name}/pulls/{pr_number}/comments",
            body=payload,
        )

    async def create_commit_comment(
        self, repo: str, commit_sha: str, body: str
    ) -> dict[str, Any]:
        """Crée un commentaire sur un commit. Pattern Activepieces."""
        owner, repo_name = _parse_repo(repo)
        return await self._client.post(
            f"/repos/{owner}/{repo_name}/commits/{commit_sha}/comments",
            body={"body": body},
        )

    # ── Branches ──────────────────────────────────────────────────────────────

    async def create_branch(
        self, repo: str, branch_name: str, from_sha: str
    ) -> dict[str, Any]:
        """
        Crée une nouvelle branche depuis un SHA.
        Pattern Activepieces : create-branch.ts
        """
        owner, repo_name = _parse_repo(repo)
        return await self._client.post(
            f"/repos/{owner}/{repo_name}/git/refs",
            body={
                "ref": f"refs/heads/{branch_name}",
                "sha": from_sha,
            },
        )

    async def delete_branch(self, repo: str, branch_name: str) -> None:
        """Supprime une branche. Pattern Activepieces : delete-branch.ts"""
        owner, repo_name = _parse_repo(repo)
        await self._client.delete(
            f"/repos/{owner}/{repo_name}/git/refs/heads/{branch_name}"
        )

    async def find_branch(self, repo: str, branch_name: str) -> Optional[dict]:
        """Cherche une branche. Retourne None si inexistante."""
        from civitas_acquisition.connectors.code_repos.github.client import ResourceNotFoundError
        owner, repo_name = _parse_repo(repo)
        try:
            return await self._client.get(
                f"/repos/{owner}/{repo_name}/branches/{branch_name}"
            )
        except ResourceNotFoundError:
            return None

    # ── Pull Requests ─────────────────────────────────────────────────────────

    async def create_pull_request(
        self,
        repo: str,
        title: str,
        head: str,
        base: str,
        body: Optional[str] = None,
        draft: bool = False,
        maintainer_can_modify: bool = True,
    ) -> dict[str, Any]:
        """Crée une Pull Request."""
        owner, repo_name = _parse_repo(repo)
        return await self._client.post(
            f"/repos/{owner}/{repo_name}/pulls",
            body={
                "title": title,
                "head": head,
                "base": base,
                "body": body or "",
                "draft": draft,
                "maintainer_can_modify": maintainer_can_modify,
            },
        )

    async def merge_pull_request(
        self,
        repo: str,
        pr_number: int,
        commit_title: Optional[str] = None,
        merge_method: str = "merge",    # "merge" | "squash" | "rebase"
    ) -> dict[str, Any]:
        """Merge une Pull Request."""
        owner, repo_name = _parse_repo(repo)
        payload: dict[str, Any] = {"merge_method": merge_method}
        if commit_title: payload["commit_title"] = commit_title
        return await self._client.post(
            f"/repos/{owner}/{repo_name}/pulls/{pr_number}/merge",
            body=payload,
        )

    # ── Releases ──────────────────────────────────────────────────────────────

    async def create_release(
        self,
        repo: str,
        tag_name: str,
        name: Optional[str] = None,
        body: Optional[str] = None,
        draft: bool = False,
        prerelease: bool = False,
        target_commitish: str = "main",
    ) -> dict[str, Any]:
        """Crée une release GitHub."""
        owner, repo_name = _parse_repo(repo)
        return await self._client.post(
            f"/repos/{owner}/{repo_name}/releases",
            body={
                "tag_name":         tag_name,
                "name":             name or tag_name,
                "body":             body or "",
                "draft":            draft,
                "prerelease":       prerelease,
                "target_commitish": target_commitish,
            },
        )

    # ── GraphQL ───────────────────────────────────────────────────────────────

    async def raw_graphql(
        self, query: str, variables: dict | None = None
    ) -> dict[str, Any]:
        """
        Exécute une requête GraphQL brute.
        Pattern Activepieces : raw-graphql-query.ts
        """
        return await self._client.graphql(query, variables)

    async def get_discussions(
        self,
        owner: str,
        repo: str,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """
        Récupère les discussions GitHub via GraphQL.
        Unavailable via REST API — GraphQL obligatoire.
        Pattern : Activepieces new-discussion-comment.ts
        """
        query = """
        query($owner: String!, $repo: String!, $limit: Int!) {
          repository(owner: $owner, name: $repo) {
            discussions(first: $limit, orderBy: {field: UPDATED_AT, direction: DESC}) {
              nodes {
                id
                title
                body
                url
                author { login }
                createdAt
                updatedAt
                category { name }
                comments(first: 20) {
                  nodes {
                    body
                    author { login }
                    createdAt
                  }
                }
              }
            }
          }
        }
        """
        data = await self._client.graphql(
            query,
            variables={"owner": owner, "repo": repo, "limit": limit},
        )
        return (
            data.get("repository", {})
            .get("discussions", {})
            .get("nodes", [])
        )

    async def find_user(self, username: str) -> Optional[dict[str, Any]]:
        """
        Récupère un utilisateur GitHub via GraphQL.
        Pattern Activepieces : find-user.ts
        """
        query = """
        query($login: String!) {
          user(login: $login) {
            login name email
            bio company location
            followers { totalCount }
            following { totalCount }
            repositories { totalCount }
            createdAt
          }
        }
        """
        data = await self._client.graphql(query, variables={"login": username})
        return data.get("user")

    async def create_discussion_comment(
        self,
        discussion_id: str,
        body: str,
    ) -> dict[str, Any]:
        """
        Crée un commentaire sur une discussion via GraphQL.
        Pattern Activepieces : create-discussion-comment.ts
        """
        mutation = """
        mutation($discussionId: ID!, $body: String!) {
          addDiscussionComment(input: {discussionId: $discussionId, body: $body}) {
            comment {
              id body
              author { login }
              createdAt
            }
          }
        }
        """
        data = await self._client.graphql(
            mutation,
            variables={"discussionId": discussion_id, "body": body},
        )
        return data.get("addDiscussionComment", {}).get("comment", {})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_repo(repo_full_name: str) -> tuple[str, str]:
    parts = repo_full_name.split("/", 1)
    if len(parts) != 2:
        raise ConnectorFatalError(
            f"Invalid repo format '{repo_full_name}' — expected 'owner/repo'"
        )
    return parts[0], parts[1]
