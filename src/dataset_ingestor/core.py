from typing import Dict, Any, Optional


# Mappa: event_type → payload_type normalizzato
_PAYLOAD_TYPE_MAP: Dict[str, str] = {
    "CommitCommentEvent": "CommitComment",
    "CreateEvent": "Create",
    "DeleteEvent": "Delete",
    "ForkEvent": "Fork",
    "GollumEvent": "Gollum",
    "IssueCommentEvent": "IssueComment",
    "IssuesEvent": "Issue",
    "MemberEvent": "Member",
    "PublicEvent": "Public",
    "PullRequestEvent": "PullRequest",
    "PullRequestReviewEvent": "PullRequestReview",
    "PullRequestReviewCommentEvent": "PullRequestReviewComment",
    "PushEvent": "Push",
    "ReleaseEvent": "Release",
    "SponsorshipEvent": "Sponsorship",
    "WatchEvent": "Watch",
    "WorkflowDispatchEvent": "WorkflowDispatch",
    "WorkflowJobEvent": "WorkflowJob",
    "WorkflowRunEvent": "WorkflowRun",
}


def _put_if_not_none(target: Dict[str, Any], key: str, value: Any) -> None:
    """
    Inserisce (key → value) in `target` solo se `value` non è None.
    Utile per costruire dizionari 'distillati' senza campi nulli.
    """
    if value is not None:
        target[key] = value


def distill_event(raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Distilla un singolo evento GHArchive in una forma compatta e coerente.

    Campi sempre presenti nel risultato:
      - case_id (repo_name)
      - activity (type)
      - timestamp (created_at)
      - actor_id
      - repo_name
      - payload_type (da _PAYLOAD_TYPE_MAP)

    Campi opzionali aggiunti in base al tipo evento (solo se disponibili), ad es.:
      - Issue/PR: action, issue_number/state, pr_number/merged/additions/...
      - Push: push_ref, push_head, push_commit_count
      - Release: release_id, release_tag_name
      - Fork: fork_full_name, fork_id
      - ecc.

    Note:
      - Evitata estrazione di PII (es. email autore commit).
      - Nessun campo testuale lungo (title/body/comment.body) per evitare bloat;
        se servirà NLP, lo si può reintrodurre in una variante "rich".
    """
    event_type = raw_event.get("type")  # string o None
    actor = raw_event.get("actor", {}) or {}
    repo = raw_event.get("repo", {}) or {}
    org = raw_event.get("org", {}) or {}

    actor_id = actor.get("id")
    actor_login = actor.get("login")                # opzionale (leggibile)
    actor_site_admin = actor.get("site_admin")      # NEW: flag utile per reti sociali

    repo_name = repo.get("name")
    repo_id = repo.get("id")                        # opzionale (stabile)
    # Questi campi non sono sempre nei GHArchive events, ma se compaiono li teniamo:
    repo_created_at = repo.get("created_at")        # NEW
    repo_language = repo.get("language")            # NEW
    repo_description = repo.get("description")      # NEW

    org_login = org.get("login")                    # NEW: utile per distinguere org vs user

    created_at = raw_event.get("created_at")
    payload = raw_event.get("payload", {}) or {}

    # Campi minimi necessari
    if not all([repo_name, event_type, created_at, actor_id]):
        return None

    payload_type = _PAYLOAD_TYPE_MAP.get(str(event_type))

    distilled: Dict[str, Any] = {
        "case_id": repo_name,
        "activity": event_type,
        "timestamp": created_at,
        "actor_id": actor_id,
        "repo_name": repo_name,
        "payload_type": payload_type,
    }
    # Campi comuni opzionali
    _put_if_not_none(distilled, "repo_id", repo_id)
    _put_if_not_none(distilled, "actor_login", actor_login)
    _put_if_not_none(distilled, "actor_site_admin", actor_site_admin)   # NEW
    _put_if_not_none(distilled, "org_login", org_login)                 # NEW
    _put_if_not_none(distilled, "repo_created_at", repo_created_at)     # NEW
    _put_if_not_none(distilled, "repo_language", repo_language)         # NEW
    if isinstance(repo_description, str):
        _put_if_not_none(distilled, "repo_description_len", len(repo_description))  # NEW

    # Normalizzazioni per tipo di evento
    if event_type == "CommitCommentEvent":
        c = payload.get("comment", {}) or {}
        _put_if_not_none(distilled, "comment_id", c.get("id"))
        _put_if_not_none(distilled, "comment_path", c.get("path"))
        _put_if_not_none(distilled, "comment_commit_id", c.get("commit_id"))

    elif event_type == "CreateEvent":
        _put_if_not_none(distilled, "create_ref", payload.get("ref"))
        _put_if_not_none(distilled, "create_ref_type", payload.get("ref_type"))
        _put_if_not_none(distilled, "master_branch", payload.get("master_branch"))

    elif event_type == "DeleteEvent":
        _put_if_not_none(distilled, "delete_ref", payload.get("ref"))
        _put_if_not_none(distilled, "delete_ref_type", payload.get("ref_type"))

    elif event_type == "ForkEvent":
        forkee = payload.get("forkee", {}) or {}
        _put_if_not_none(distilled, "fork_full_name", forkee.get("full_name"))
        _put_if_not_none(distilled, "fork_id", forkee.get("id"))
        # NEW: aggregati utili senza testo
        _put_if_not_none(distilled, "forkee_stars", forkee.get("stargazers_count"))
        _put_if_not_none(distilled, "forkee_forks", forkee.get("forks_count"))

    elif event_type == "GollumEvent":
        pages = payload.get("pages") or []
        _put_if_not_none(distilled, "gollum_page_count", len(pages))

    elif event_type == "IssueCommentEvent":
        _put_if_not_none(distilled, "action", payload.get("action"))
        issue = payload.get("issue", {}) or {}
        comment = payload.get("comment", {}) or {}
        _put_if_not_none(distilled, "issue_number", issue.get("number"))
        _put_if_not_none(distilled, "issue_state", issue.get("state"))
        _put_if_not_none(distilled, "comment_id", comment.get("id"))
        # NEW: timestamps per tempi di attesa
        _put_if_not_none(distilled, "issue_created_at", issue.get("created_at"))
        _put_if_not_none(distilled, "issue_closed_at", issue.get("closed_at"))

    elif event_type == "IssuesEvent":
        _put_if_not_none(distilled, "action", payload.get("action"))
        issue = payload.get("issue", {}) or {}
        _put_if_not_none(distilled, "issue_number", issue.get("number"))
        _put_if_not_none(distilled, "issue_state", issue.get("state"))
        labels = issue.get("labels") or []
        label_names = [l.get("name") for l in labels if isinstance(l, dict) and l.get("name")]
        if label_names:
            _put_if_not_none(distilled, "issue_labels", label_names)
            _put_if_not_none(distilled, "issue_label_count", len(label_names))
        # NEW: timestamps per tempi di attesa
        _put_if_not_none(distilled, "issue_created_at", issue.get("created_at"))
        _put_if_not_none(distilled, "issue_closed_at", issue.get("closed_at"))

    elif event_type == "MemberEvent":
        _put_if_not_none(distilled, "action", payload.get("action"))
        member = payload.get("member", {}) or {}
        _put_if_not_none(distilled, "member_id", member.get("id"))
        _put_if_not_none(distilled, "member_login", member.get("login"))

    elif event_type == "PublicEvent":
        _put_if_not_none(distilled, "action", payload.get("action"))

    elif event_type == "PullRequestEvent":
        _put_if_not_none(distilled, "action", payload.get("action"))
        pr = payload.get("pull_request", {}) or {}
        pr_number = payload.get("number", pr.get("number"))
        _put_if_not_none(distilled, "pr_number", pr_number)
        _put_if_not_none(distilled, "pr_merged", pr.get("merged"))
        _put_if_not_none(distilled, "pr_additions", pr.get("additions"))
        _put_if_not_none(distilled, "pr_deletions", pr.get("deletions"))
        _put_if_not_none(distilled, "pr_changed_files", pr.get("changed_files"))
        # NEW: timestamps per tempi PR
        _put_if_not_none(distilled, "pr_created_at", pr.get("created_at"))
        _put_if_not_none(distilled, "pr_merged_at", pr.get("merged_at"))
        _put_if_not_none(distilled, "pr_closed_at", pr.get("closed_at"))

    elif event_type == "PullRequestReviewEvent":
        _put_if_not_none(distilled, "action", payload.get("action"))
        review = payload.get("review", {}) or {}
        _put_if_not_none(distilled, "review_id", review.get("id"))
        _put_if_not_none(distilled, "review_state", review.get("state"))
        pr = payload.get("pull_request", {}) or {}
        _put_if_not_none(distilled, "pr_number", pr.get("number"))
        # (i timestamp PR sono già coperti nella PR event)

    elif event_type == "PullRequestReviewCommentEvent":
        _put_if_not_none(distilled, "action", payload.get("action"))
        comment = payload.get("comment", {}) or {}
        _put_if_not_none(distilled, "comment_id", comment.get("id"))
        pr = payload.get("pull_request", {}) or {}
        _put_if_not_none(distilled, "pr_number", pr.get("number"))

    elif event_type == "PushEvent":
        _put_if_not_none(distilled, "push_ref", payload.get("ref"))
        _put_if_not_none(distilled, "push_head", payload.get("head"))
        commits = payload.get("commits") or []
        _put_if_not_none(distilled, "push_commit_count", len(commits))
        _put_if_not_none(distilled, "push_size", payload.get("size"))
        # Non estraiamo email/autore/messaggi commit per evitare PII e bloat

    elif event_type == "ReleaseEvent":
        _put_if_not_none(distilled, "action", payload.get("action"))
        rel = payload.get("release", {}) or {}
        _put_if_not_none(distilled, "release_id", rel.get("id"))
        _put_if_not_none(distilled, "release_tag_name", rel.get("tag_name"))

    elif event_type == "SponsorshipEvent":
        _put_if_not_none(distilled, "action", payload.get("action"))
        s = payload.get("sponsorship", {}) or {}
        tier = s.get("tier")
        if isinstance(tier, dict):
            tier = tier.get("name")
        _put_if_not_none(distilled, "sponsorship_tier", tier)
        sponsor = s.get("sponsor") or {}
        _put_if_not_none(distilled, "sponsor_login", sponsor.get("login"))
        _put_if_not_none(distilled, "sponsorship_effective_date", s.get("effective_date"))

    elif event_type == "WatchEvent":
        _put_if_not_none(distilled, "action", payload.get("action"))  # tipicamente "started"

    elif event_type == "WorkflowDispatchEvent":
        _put_if_not_none(distilled, "workflow_name", payload.get("workflow"))
        inputs = payload.get("inputs")
        if isinstance(inputs, dict):
            _put_if_not_none(distilled, "workflow_inputs_keys", sorted(inputs.keys()))

    elif event_type == "WorkflowJobEvent":
        job = payload.get("workflow_job", {}) or {}
        _put_if_not_none(distilled, "workflow_job_status", job.get("status"))
        _put_if_not_none(distilled, "workflow_job_conclusion", job.get("conclusion"))

    elif event_type == "WorkflowRunEvent":
        run = payload.get("workflow_run", {}) or {}
        _put_if_not_none(distilled, "workflow_run_status", run.get("status"))
        _put_if_not_none(distilled, "workflow_run_conclusion", run.get("conclusion"))
        _put_if_not_none(distilled, "workflow_run_event", run.get("event"))

    return distilled
