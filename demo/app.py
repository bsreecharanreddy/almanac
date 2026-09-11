"""Almanac, locally. Committed artifacts, a committed champion, no cloud account."""

import streamlit as st

from almanac.demo import artifacts
from almanac.demo.champion import champion_provenance

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
