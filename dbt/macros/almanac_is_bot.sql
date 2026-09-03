{#
  Whether a login is a bot -- the SQL restatement of
  `measure.classify_bot`, byte-identical to `measure.BOT_REGEX`
  (test_bot_macro.py asserts that and that Java `rlike` agrees with Python
  `re`). Null in -> null out; wrap in `coalesce(..., false)` where a
  definite answer is needed.
#}
{% macro almanac_is_bot(login) -%}
(
    endswith({{ login }}, '[bot]')
    or lower({{ login }}) in ('dependabot', 'renovate', 'github-actions')
    or {{ login }} rlike '(?i:(bot|automation)$)|(?i:[-_.]ci$)|[a-z0-9]CI$'
)
{%- endmacro %}
