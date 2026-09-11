"""Almanac, locally. Committed artifacts, a committed champion, no cloud account."""

import streamlit as st

from almanac.demo import artifacts, panels
from almanac.demo.champion import champion_provenance, load_champion

st.set_page_config(page_title="Almanac — local demo", layout="wide")

st.title("Almanac — work-queue risk, running locally")

_medallion = artifacts.load_medallion()
_provenance = champion_provenance()

st.markdown(
    f"""
This runs **one archived hour per schema era** from GH Archive, scored by the
real registered champion (version `{_provenance["model_version"]}`, run
`{_provenance["run_id"]}`).

**It is not the full dataset.** The platform's measured backfill is
**341,060,851** rows over Q3 2025, for a measured $11.96. Every count below is
computed from the fixture hours, not from that quarter.
"""
)

queue_tab, explain_tab, agent_tab, lake_tab = st.tabs(
    ["Intervention queue", "Why this score", "Agent, verified", "The medallion"]
)

with queue_tab:
    queue = artifacts.load_queue()
    coverage = artifacts.load_coverage()
    left, right = st.columns([3, 2])

    with left:
        st.subheader(f"Ranked by predicted breach risk — {len(queue)} pull requests")
        st.dataframe(
            queue[["rank", "repo_id", "pr_number", "breach_risk", "is_bot_author"]],
            hide_index=True,
            width="stretch",
        )
        st.caption(
            "Surrogate keys and rank position only. Logins and `owner/repo` names "
            "identify real people who never opted into this project, so they are "
            "never rendered — see `docs/pseudonymization.md`."
        )

    with right:
        st.subheader("How many features actually have a value")
        rows = panels.coverage_rows(coverage)
        st.dataframe(rows, hide_index=True, width="stretch")
        st.markdown(
            "**This is point-in-time correctness, and it is not a defect.** A "
            "feature named *to date* may read only events strictly before its own "
            "`as_of`. One archived hour contains almost no prior history, so most "
            "of these are null and the queue is ranking on the few that are not. "
            "At the measured quarter's scale they populate; here you can see "
            "exactly why they do not."
        )

with explain_tab:
    queue = artifacts.load_queue()
    model = st.cache_resource(load_champion)()

    rank = st.number_input("Rank", min_value=1, max_value=int(queue["rank"].max()), value=1, step=1)
    row = queue.loc[queue["rank"] == rank].iloc[0]

    st.metric("Predicted breach risk", f"{row['breach_risk']:.6f}")
    st.dataframe(panels.contributions_for(model, row), hide_index=True, width="stretch")
    st.markdown(
        f"Contributions move from the model's own **baseline** of "
        f"`{panels.baseline_for(model, row):.6f}`, which is *not* a feature — "
        "LightGBM returns one more column than there are features and the last "
        "is the expected value. These are the champion's own numbers, computed "
        "here, not an explanation written about them."
    )
