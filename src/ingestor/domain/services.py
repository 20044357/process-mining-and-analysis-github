from typing import Dict, Any, Optional
from .entities import DistilledEvent

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

def extract_event_payload(raw_event: Dict[str, Any]) -> Optional[DistilledEvent]:
    event_type = raw_event.get("type")
    actor = raw_event.get("actor", {}) or {}
    repo = raw_event.get("repo", {}) or {}
    created_at = raw_event.get("created_at")
    actor_id = actor.get("id")
    repo_name = repo.get("name")

    if repo_name is None or event_type is None or created_at is None or actor_id is None:
        return None

    return DistilledEvent(
        case_id=repo_name,
        activity=event_type,
        timestamp=created_at,
        actor_id=actor_id,
        repo_name=repo_name,
        payload_type=_PAYLOAD_TYPE_MAP.get(str(event_type)),
        repo_id=repo.get("id"),          
        actor_login=actor.get("login"),  
    )