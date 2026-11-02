-- Authors the reader keeps dismissing: at least two dismissed books, and more
-- dismissed than read. One dismissal never hides an author, and an author
-- read often survives a couple of dismissals. A view, not a table, because
-- dismissals happen live in Flask and the next replacement must already
-- skip the author.

{{ config(materialized='view') }}

{% set min_dismissed = 2 %}

with dismissed as (

    select lower(trim(b.primary_creator)) as author_key, count(*) as dismissed_count
    from {{ source('meta', 'not_interested') }} n
    join {{ ref('dim_book') }} b on b.content_id = n.content_id
    where b.primary_creator is not null
    group by 1

),

read as (

    select lower(trim(b.primary_creator)) as author_key, count(*) as read_count
    from {{ ref('fact_reading_history') }} f
    join {{ ref('dim_book') }} b on b.content_id = f.content_id
    group by 1

)

select d.author_key, d.dismissed_count, coalesce(r.read_count, 0) as read_count
from dismissed d
left join read r on r.author_key = d.author_key
where d.dismissed_count >= {{ min_dismissed }}
  and d.dismissed_count > coalesce(r.read_count, 0)
