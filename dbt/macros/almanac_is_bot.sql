{#
  Whether a login is a bot -- the same three-clause heuristic as
  `almanac.explore.measure.classify_bot`, restated in SQL for Gold.

  The regex is measured, not stylistic (see
  `docs/findings/2026-09-01-bot-classification.md`): a bare `ci$` clause
  matched 869 human surnames, so the CI clauses require a separator or a
  camelCase boundary. It must stay byte-identical to `measure.BOT_REGEX`
  -- `tests/unit/test_bot_macro.py` asserts that and checks the two agree
  on real logins, because Java (`rlike`) and Python regex are not the same
  engine.

  Null in -> null out; call sites that need a definite answer wrap this in
  `coalesce(..., false)`.
#}
{% macro almanac_is_bot(login) -%}
(
    endswith({{ login }}, '[bot]')
    or lower({{ login }}) in ('dependabot', 'renovate', 'github-actions')
    or {{ login }} rlike '(?i:(bot|automation)$)|(?i:[-_.]ci$)|[a-z0-9]CI$'
)
{%- endmacro %}
