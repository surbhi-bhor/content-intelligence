with book_subjects as (

    select genre_value as subject_name
    from {{ ref('stg_books') }}, jsonb_array_elements_text(subjects) as genre_value
    where subjects is not null

),

cleaned as (

    select distinct trim(subject_name) as subject_name
    from book_subjects
    where subject_name is not null and trim(subject_name) <> ''

)

select
    row_number() over (order by subject_name) as subject_id,
    subject_name,
    -- Open Library subjects mix real topics ("Indic Mythology", "Time
    -- travel") with format, audience, marketing and library tags ("Fiction",
    -- "New York Times bestseller", "Large type books"). The second kind is on
    -- most books, so it says nothing about what a book is like. Taste signals
    -- and book similarity ignore these.
    (
        lower(subject_name) in (
            'fiction', 'fiction, general', 'general', 'literature', 'novel', 'novels',
            'juvenile fiction', 'juvenile literature', 'children''s fiction',
            'new york times bestseller', 'new york times reviewed', 'bestsellers',
            'romans, nouvelles', 'romans, nouvelles, etc.', 'roman', 'romans',
            'american literature', 'english literature', 'english fiction',
            'american fiction', 'fiction in english', 'literary', 'literary fiction',
            'fiction, literary', 'adult', 'contemporary', 'classic literature',
            'translations into english',
            'british and irish fiction (fictional works by one author)',
            'american fiction (fictional works by one author)',
            'english fiction (fictional works by one author)'
        )
        or lower(subject_name) ~ '^(collection:|nyt:|reading level|award|accessible book|protected daisy|in library|lending library|large type|popular print|open library)'
    ) as is_generic

from cleaned
