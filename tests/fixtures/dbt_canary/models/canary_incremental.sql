{{ config(
    materialized='incremental',
    file_format='delta',
    incremental_strategy='merge',
    unique_key='id'
) }}

select 1 as id, 'canary' as note
