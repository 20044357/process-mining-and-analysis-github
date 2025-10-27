from typing import Dict, Any, Optional

# Mappa: event_type → payload_type normalizzato
_PAYLOAD_TYPE_MAP: Dict[str, str] = {
    "CommitCommentEvent": "CommitComment", "CreateEvent": "Create",
    "DeleteEvent": "Delete", "ForkEvent": "Fork", "GollumEvent": "Gollum",
    "IssueCommentEvent": "IssueComment", "IssuesEvent": "Issue",
    "MemberEvent": "Member", "PublicEvent": "Public",
    "PullRequestEvent": "PullRequest", "PullRequestReviewEvent": "PullRequestReview",
    "PullRequestReviewCommentEvent": "PullRequestReviewComment",
    "PushEvent": "Push", "ReleaseEvent": "Release",
    "SponsorshipEvent": "Sponsorship", "WatchEvent": "Watch",
    "WorkflowDispatchEvent": "WorkflowDispatch", "WorkflowJobEvent": "WorkflowJob",
    "WorkflowRunEvent": "WorkflowRun",
}

def _assign_if_present(target: Dict[str, Any], key: str, value: Any) -> None:
    if value is not None:
        target[key] = value

def extract_event_payload(raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Distilla un singolo evento GHArchive in una forma compatta e coerente.
    Questa è una funzione di dominio pura.
    """
    event_type = raw_event.get("type")
    actor = raw_event.get("actor", {}) or {}
    repo = raw_event.get("repo", {}) or {}
    org = raw_event.get("org", {}) or {}
    created_at = raw_event.get("created_at")
    payload = raw_event.get("payload", {}) or {}

    actor_id = actor.get("id")
    repo_name = repo.get("name")

    if not all([repo_name, event_type, created_at, actor_id]):
        return None

    distilled: Dict[str, Any] = {
        "case_id": repo_name, "activity": event_type, "timestamp": created_at,
        "actor_id": actor_id, "repo_name": repo_name,
        "payload_type": _PAYLOAD_TYPE_MAP.get(str(event_type)),
    }
    _assign_if_present(distilled, "repo_id", repo.get("id"))
    _assign_if_present(distilled, "actor_login", actor.get("login"))
    return distilled