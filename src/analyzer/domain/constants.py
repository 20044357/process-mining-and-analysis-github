ALL_POSSIBLE_ACTIVITIES = sorted([
    # 1. Eventi di Produzione e Gestione Codice
    "PushEvent",
    "CreateEvent_branch",
    "DeleteEvent_branch",

    # 2. Eventi di Gestione dei Task (Issue)
    "IssuesEvent_opened",
    "IssuesEvent_reopened",
    "IssuesEvent_closed",

    # 3. Eventi di Proposta di Modifica (Pull Request)
    "PullRequestEvent_opened",
    "PullRequestEvent_reopened",
    "PullRequestEvent_closed",
    "PullRequestEvent_synchronize",
    "PullRequestEvent_merged",
    "PullRequestEvent_rejected",

    # 4. Eventi di Collaborazione e Discussione
    "IssueCommentEvent_created",
    "PullRequestReviewCommentEvent_created",
    "CommitCommentEvent_created",

    # 5. Evento di Revisione Formale
    "PullRequestReviewEvent_approved",
    "PullRequestReviewEvent_changes_requested",
    "PullRequestReviewEvent_commented",

    # 6. Eventi di Rilascio
    "ReleaseEvent_published"
])
