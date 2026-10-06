-- Every unread book scored against the reader's own history: the book
-- equivalent of the genre and creator signals movie picks use. Two signals:
--
--   similar book: the highly rated read (8+ out of 10) that shares the most
--                 specific subjects with the candidate. Needs at least two
--                 shared subjects; one shared tag ("Business") is too weak.
--   author:       the reader's average rating of this author's books.
--
-- predicted_score is the mean of whichever signals exist, the same rule as
-- compute_predicted_score for movies. Read in one place by the weekly picks
-- (recommendation_op.py) and the live replacement in Flask, so both rank
-- books the same way.

{% set liked_min_rating = 8 %}
{% set min_shared_subjects = 2 %}
{% set min_prefix_match_length = 12 %}

with read_books as (

    select f.content_id, f.rating, f.has_rating, b.title, b.primary_creator
    from {{ ref('fact_reading_history') }} f
    join {{ ref('dim_book') }} b on b.content_id = f.content_id

),

read_title_keys as (

    select distinct {{ book_title_key('title') }} as title_key
    from read_books

),

unread as (

    select b.*, {{ book_title_key('b.title') }} as title_key
    from {{ ref('dim_book') }} b
    where b.content_id not in (select content_id from read_books)

),

-- Unread books. The title check catches a read book that discovery found
-- again under a different Open Library work: same title, or one title
-- extending the other ("Man's Search for Meaning" vs "Man's Search for
-- Meaning adapted for Young Adults"). The prefix rule needs 12+ characters
-- so a short title like "Karma" never hides a different book.
candidates as (

    select u.*
    from unread u
    where not exists (
        select 1 from read_title_keys r
        where r.title_key = u.title_key
           or (least(length(r.title_key), length(u.title_key)) >= {{ min_prefix_match_length }}
               and (r.title_key like u.title_key || '%' or u.title_key like r.title_key || '%'))
    )

),

liked_subjects as (

    select rb.content_id, rb.title, rb.rating, s.subject_id, s.subject_name
    from read_books rb
    join {{ ref('bridge_content_book_subject') }} bcs on bcs.content_id = rb.content_id
    join {{ ref('dim_book_subject') }} s on s.subject_id = bcs.subject_id
    where rb.has_rating
      and rb.rating >= {{ liked_min_rating }}
      and not s.is_generic

),

overlap as (

    select
        c.content_id,
        ls.content_id as liked_content_id,
        ls.title as liked_title,
        ls.rating as liked_rating,
        count(*) as shared_count,
        array_agg(ls.subject_name order by ls.subject_name) as shared_subjects
    from candidates c
    join {{ ref('bridge_content_book_subject') }} bcs on bcs.content_id = c.content_id
    join liked_subjects ls on ls.subject_id = bcs.subject_id
    group by c.content_id, ls.content_id, ls.title, ls.rating
    having count(*) >= {{ min_shared_subjects }}

),

best_match as (

    select distinct on (content_id) *
    from overlap
    order by content_id, shared_count desc, liked_rating desc, liked_title

),

author_signal as (

    select
        lower(trim(primary_creator)) as author_key,
        avg(rating) as avg_rating,
        count(*) as read_count
    from read_books
    where has_rating and primary_creator is not null
    group by lower(trim(primary_creator))

)

select
    c.content_id,
    c.title,
    c.primary_creator,
    c.vote_average,
    c.vote_count,
    bm.liked_content_id as similar_to_content_id,
    bm.liked_title as similar_to_title,
    bm.liked_rating as similar_to_rating,
    coalesce(bm.shared_count, 0) as shared_subject_count,
    coalesce(bm.shared_subjects, array[]::text[]) as shared_subjects,
    round(a.avg_rating::numeric, 1) as author_avg_rating,
    coalesce(a.read_count, 0) as author_read_count,
    (bm.liked_rating is not null)::int + (a.avg_rating is not null)::int as signal_count,
    round((
        (coalesce(bm.liked_rating, 0) + coalesce(a.avg_rating, 0))
        / nullif((bm.liked_rating is not null)::int + (a.avg_rating is not null)::int, 0)
    )::numeric, 1) as predicted_score

from candidates c
left join best_match bm on bm.content_id = c.content_id
left join author_signal a on a.author_key = lower(trim(c.primary_creator))
