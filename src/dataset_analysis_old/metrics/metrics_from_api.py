import os, base64, requests, pandas as pd
from typing import Dict, Any, Set
import pyarrow.dataset as ds
from ..utils import open_parquet_dataset_only


# -----------------------------
# Helpers interni
# -----------------------------
def _make_headers():
    token = os.getenv("GITHUB_TOKEN")
    headers = {"Accept": "application/vnd.github.mercy-preview+json"}
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


def _fetch_repo_data(repo_name, headers):
    base_url = f"https://api.github.com/repos/{repo_name}"
    r = requests.get(base_url, headers=headers)
    if r.status_code != 200:
        return None
    return r.json()


def _fetch_language(repo_data):
    return repo_data.get("language")


def _fetch_topics(base_url, headers):
    r = requests.get(f"{base_url}/topics", headers=headers)
    if r.status_code == 200:
        return r.json().get("names", [])
    return []


def _fetch_readme_quality(base_url, headers):
    r = requests.get(f"{base_url}/readme", headers=headers)
    if r.status_code != 200:
        return None

    readme_json = r.json()
    readme_content = base64.b64decode(readme_json.get("content", "")).decode("utf-8", errors="ignore")
    score = len(readme_content) / 1000.0
    if any(s in readme_content.lower() for s in ["install", "usage", "contributing"]):
        score += 1
    return round(score, 2)


def _fetch_verbosity(base_url, headers):
    r = requests.get(f"{base_url}/contents", headers=headers)
    if r.status_code == 200:
        contents = r.json()
        return len(contents) if isinstance(contents, list) else None
    return None


# -----------------------------
# Funzioni principali
# -----------------------------
def fetch_api_metrics(repo_name: str) -> Dict[str, Any]:
    headers = _make_headers()
    base_url = f"https://api.github.com/repos/{repo_name}"
    repo_data = _fetch_repo_data(repo_name, headers)

    if not repo_data:
        return {"language": None, "topics": [], "readme_quality": None, "verbosity": None}

    return {
        "language": _fetch_language(repo_data),
        "topics": _fetch_topics(base_url, headers),
        "readme_quality": _fetch_readme_quality(base_url, headers),
        "verbosity": _fetch_verbosity(base_url, headers),
    }


def calculate_api_metrics(base_dir: str, repo_ids: Set[int]) -> pd.DataFrame:
    dset = open_parquet_dataset_only(base_dir)
    repo_filter = ds.field("repo_id").isin(list(repo_ids))
    repo_names_tbl = dset.to_table(columns=["repo_id", "repo_name"], filter=repo_filter)
    df_repo_names = repo_names_tbl.to_pandas().drop_duplicates(subset="repo_id")

    results = []
    for rid, rname in zip(df_repo_names["repo_id"], df_repo_names["repo_name"]):
        data = fetch_api_metrics(rname)
        row = {"repo_id": int(rid)}
        row.update(data)
        results.append(row)

    return pd.DataFrame(results)
