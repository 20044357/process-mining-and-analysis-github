import polars as pl

# ============================================================
# Predicati di dominio — Regole logiche atomiche dell’analisi
# ============================================================

# Questo modulo definisce le espressioni Polars che rappresentano
# le regole di business della pipeline di analisi.
# Tutti i predicati sono espressi come condizioni logiche pure,
# combinabili in query lazy e riutilizzabili in più contesti.

# ------------------------------------------------------------
# FASE 1 — Identificazione eventi e attori di base
# ------------------------------------------------------------

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

# ------------------------------------------------------------
# FASE 2 — Regole semantiche per le dimensioni analitiche
# ------------------------------------------------------------

# --- Popolarità Esterna ---
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

# --- Engagement Community ---
is_significant_eng_event = (
    ((pl.col("activity") == "IssuesEvent") & (pl.col("action").is_in(["opened", "reopened"]))) |
    ((pl.col("activity") == "IssueCommentEvent") & (pl.col("action") == "created"))
)
"""
Predicato per la dimensione *Engagement Community*.
Identifica gli eventi che attivano discussioni o riaprono issue,
rappresentando l’interazione sociale e la partecipazione della community.
"""

# --- Intensità Collaborativa ---
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

# ------------------------------------------------------------
# Identificazione eventi "core" del workflow
# ------------------------------------------------------------
is_core_workflow_event = (
    # 1. Eventi di base
    (pl.col("activity") == "PushEvent") |

    # 2. Issue aperte, riaperte, modificate
    (
        (pl.col("activity") == "IssuesEvent") &
        (pl.col("action").is_in(["opened", "reopened", "edited"]))
    ) |

    # 3. Pull Request (azioni rilevanti)
    (
        (pl.col("activity") == "PullRequestEvent") &
        (pl.col("action").is_in(["opened", "reopened", "edited", "synchronize", "closed"]))
    ) |

    # 4. Commenti (issue, PR e commit)
    (
        (pl.col("activity") == "IssueCommentEvent") &
        (pl.col("action").is_in(["created", "edited"]))
    ) |
    (
        (pl.col("activity") == "PullRequestReviewCommentEvent") &
        (pl.col("action").is_in(["created", "edited"]))
    ) |
    (
        (pl.col("activity") == "CommitCommentEvent") &
        (pl.col("action") == "created")
    ) |

    # 5. Review formali
    (
        (pl.col("activity") == "PullRequestReviewEvent") &
        (pl.col("action") == "created")
    ) |

    # 6. Creazione di branch
    (
        (pl.col("activity") == "CreateEvent") &
        (pl.col("create_ref_type") == "branch")
    )
)
"""
Predicato semantico che identifica gli eventi considerati “core”
per la ricostruzione del workflow di sviluppo di una repository.
Usato per filtrare i log prima della discovery PM4Py.
"""
