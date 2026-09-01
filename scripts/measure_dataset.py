"""Measure the dataset properties the design depends on.

Samples three non-adjacent days across the candidate quarter. Three days
rather than one because a rename is only visible as the same repo id
carrying different names at different times -- a single day cannot show
one at all.

Hours are sub-sampled within each day. That is acceptable *here* and
forbidden in the pipeline: design doc §4.5 bans hour sampling for
training data because a skipped hour can drop a PR's review event and
fabricate an SLA breach. This is a diagnostic probe measuring repo-name
observations over time, not a label, so completeness per hour is not
required.
"""

import collections
import gzip
import json
import sys
from datetime import UTC, date, datetime

import httpx

from almanac.config import Settings
from almanac.explore.measure import BotMatch, classify_bot, duplicate_ratio, rename_events
from almanac.extract.archive import fetch_hour
from almanac.extract.outcome import FetchStatus

SAMPLE_DAYS = [date(2025, 1, 8), date(2025, 2, 12), date(2025, 3, 19)]
SAMPLE_HOURS = [0, 3, 6, 9, 12, 15, 18, 21]


def main() -> int:
    settings = Settings()
    ids: list[str] = []
    repo_obs: list[tuple[int, str, datetime]] = []
    bots: collections.Counter[BotMatch] = collections.Counter()
    regex_only: collections.Counter[str] = collections.Counter()
    outcomes: collections.Counter[FetchStatus] = collections.Counter()
    types: collections.Counter[str] = collections.Counter()
    events = 0

    with httpx.Client(follow_redirects=True) as client:
        for day in SAMPLE_DAYS:
            for hour in SAMPLE_HOURS:
                res = fetch_hour(
                    day,
                    hour,
                    client=client,
                    dest_dir=settings.data_dir / "raw",
                    settings=settings,
                )
                outcomes[res.status] += 1
                if res.status is not FetchStatus.OK or res.path is None:
                    print(f"  {day} {hour:02d}: {res.status.value}", flush=True)
                    continue
                with gzip.open(res.path, "rt", encoding="utf-8") as fh:
                    for line in fh:
                        try:
                            e = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        events += 1
                        ids.append(e["id"])
                        types[e["type"]] += 1
                        login = (e.get("actor") or {}).get("login", "")
                        match = classify_bot(login)
                        bots[match] += 1
                        if match is BotMatch.REGEX:
                            regex_only[login] += 1
                        repo = e.get("repo") or {}
                        if repo.get("id") and repo.get("name"):
                            repo_obs.append(
                                (
                                    repo["id"],
                                    repo["name"],
                                    datetime.fromisoformat(
                                        e["created_at"].replace("Z", "+00:00")
                                    ).astimezone(UTC),
                                )
                            )
            print(f"{day} done, {events} events so far", flush=True)

    renames = rename_events(repo_obs)
    distinct_repos = len({r for r, _, _ in repo_obs})
    print("\n=== MEASURED ===")
    print(f"days sampled        : {[str(d) for d in SAMPLE_DAYS]}")
    print(f"hours per day       : {SAMPLE_HOURS}")
    print(f"fetch outcomes      : { {k.value: v for k, v in outcomes.items()} }")
    print(f"total events        : {events}")
    print(f"distinct event ids  : {len(set(ids))}")
    print(f"duplicate ratio     : {duplicate_ratio(ids):.6f}")
    print(f"bot classification  : { {k.value: v for k, v in bots.items()} }")
    print(f"distinct repos      : {distinct_repos}")
    print(f"RENAMES DETECTED    : {len(renames)}   <-- gates SCD2")
    print("\nsample renames:")
    for r in renames[:15]:
        print(f"  {r.repo_id}  {r.from_name}  ->  {r.to_name}")
    print("\ntop 30 regex-only matches (inspect for false positives):")
    for login, n in regex_only.most_common(30):
        print(f"  {login:40s} {n}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
