import polars as pl

is_repo_creation_event = (
    (pl.col("activity") == "CreateEvent") &
    (pl.col("create_ref_type") == "repository")
)
"""
Predicato atomico che identifica gli eventi di creazione di una repository.
Utilizzato per costruire la tabella di lookup delle date di creazione.
"""

is_bot_actor = (
    pl.col("actor_login").str.ends_with("[bot]").fill_null(False)
)
"""
Predicato che identifica gli attori automatici (bot) 
in base al suffisso convenzionale del campo 'actor_login'.
"""

is_significant_pop_event = (
    ((pl.col("activity") == "WatchEvent") & (pl.col("action") == "started")) |
    (pl.col("activity") == "ForkEvent") |
    ((pl.col("activity") == "ReleaseEvent") & (pl.col("action") == "published"))
)
"""
Predicato per la dimensione *Popolarità Esterna*.
Considera solo eventi che riflettono un interesse genuino verso la repository
(es. nuovi “watch”, fork, pubblicazione di release).
"""

is_significant_eng_event = (
    ((pl.col("activity") == "IssuesEvent") & (pl.col("action").is_in(["opened", "reopened"]))) |
    ((pl.col("activity") == "IssueCommentEvent") & (pl.col("action") == "created"))
)
"""
Predicato per la dimensione *Engagement Community*.
Identifica gli eventi che attivano discussioni o riaprono issue,
rappresentando l’interazione sociale e la partecipazione della community.
"""

is_significant_collab_event = (
    ((pl.col("activity") == "PullRequestEvent") & (pl.col("action").is_in(["opened", "reopened", "synchronize"]))) |
    ((pl.col("activity") == "PullRequestReviewEvent") & (pl.col("action") == "submitted")) |
    ((pl.col("activity") == "PullRequestReviewCommentEvent") & (pl.col("action") == "created")) |
    ((pl.col("activity") == "CommitCommentEvent") & (pl.col("action") == "created"))
)
"""
Predicato per la dimensione *Intensità Collaborativa*.
Rileva le azioni di scrittura e revisione del codice,
includendo pull request, review e commenti a commit.
"""

is_core_workflow_event = (
    (pl.col("activity") == "PushEvent") |
    (
        (pl.col("activity") == "CreateEvent") &
        (pl.col("create_ref_type") == "branch")  
    ) |
    (
        (pl.col("activity") == "DeleteEvent") &
        (pl.col("delete_ref_type") == "branch")  
    ) |

    
    
    (
        (pl.col("activity") == "IssuesEvent") &
        (pl.col("action").is_in(["opened", "reopened", "closed"]))
    ) |

    
    
    (
        (pl.col("activity") == "PullRequestEvent") &
        (pl.col("action").is_in(["opened", "reopened", "closed", "synchronize"]))
    ) |

    

    (
        (pl.col("activity") == "IssueCommentEvent") & (pl.col("action") == "created")
    ) |
    (
        (pl.col("activity") == "PullRequestReviewCommentEvent") & (pl.col("action") == "created")
    ) |
    (
        (pl.col("activity") == "CommitCommentEvent") & (pl.col("action") == "created")
    ) |
    
    
    
    (
        pl.col("activity") == "PullRequestReviewEvent"
    ) |
    
    
    
    (
        (pl.col("activity") == "ReleaseEvent") & (pl.col("action") == "published")
    )
)
