import os
import json
import time
import psycopg2
import requests
from dagster import op, get_dagster_logger

LANGUAGE_NAMES = {"en": "English", "hi": "Hindi", "mr": "Marathi"}
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_PICKS_MODEL = os.getenv("OLLAMA_PICKS_MODEL", "llama3.2:1b")
DAILY_PICKS_COUNT = 10

# ── DB connection ─────────────────────────────────────────────

def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD")
    )

# ── Helpers ───────────────────────────────────────────────────

def get_latest_watch_profile(cur):
    cur.execute("""
        SELECT top_genres_watch, bottom_genres_watch, top_creators_watch, avg_rating_by_type
        FROM meta.taste_profile
        ORDER BY generated_at DESC
        LIMIT 1;
    """)
    row = cur.fetchone()
    if row is None:
        # Same 5-tuple shape as the normal return: the caller unpacks five
        # values before checking summary_text, so a short tuple crashed the
        # op on a fresh database instead of logging "run build_taste_profile".
        return None, [], [], {}, {}
    top_genres, bottom_genres, top_creators, avg_by_type = row

    lines = []
    if avg_by_type:
        movie_tv = {t: a for t, a in avg_by_type.items() if t in ("movie", "tv")}
        if movie_tv:
            parts = [f"{t}: {a} avg" for t, a in movie_tv.items()]
            lines.append("Average rating — " + ", ".join(parts) + ".")
    if top_genres:
        parts = [f"{g['name']} ({g['avg_rating']} avg, {g['count']} rated)" for g in top_genres]
        lines.append("Favorite genres: " + ", ".join(parts) + ".")
    if bottom_genres:
        parts = [f"{g['name']} ({g['avg_rating']} avg, {g['count']} rated)" for g in bottom_genres]
        lines.append("Genres to avoid: " + ", ".join(parts) + ".")
    if top_creators:
        parts = [f"{c['name']} ({c['avg_rating']} avg, {c['count']} rated)" for c in top_creators]
        lines.append("Favorite directors/creators: " + ", ".join(parts) + ".")

    summary_text = " ".join(lines)
    genre_names = [g["name"] for g in top_genres] if top_genres else []
    creator_names = [c["name"] for c in top_creators] if top_creators else []
    genre_avg = {g["name"]: g["avg_rating"] for g in top_genres} if top_genres else {}
    creator_avg = {c["name"]: c["avg_rating"] for c in top_creators} if top_creators else {}
    return summary_text, genre_names, creator_names, genre_avg, creator_avg

def get_preferred_languages(cur) -> list:
    cur.execute("""
        SELECT config_value FROM meta.user_config
        WHERE config_key = 'preferred_languages';
    """)
    row = cur.fetchone()
    return row[0] if row else ["en"]

def get_excluded_genres(cur) -> list:
    cur.execute("""
        SELECT config_value FROM meta.user_config
        WHERE config_key = 'excluded_genres';
    """)
    row = cur.fetchone()
    return row[0] if row else []

# TMDB's vote counts are heavily skewed toward English/Hollywood content -
# a single global vote_count>=10 bar was found to gut the regional
# candidate pool disproportionately (Marathi: 71 unwatched -> 2 survive;
# Hindi: 61 -> 19), starving the 'hi'/'mr' language slots and forcing
# constant fallback to English. Lower for languages known to have a much
# smaller, less-voted TMDB catalog; English keeps the stricter bar since it
# has plenty of well-voted candidates to spare.
_LOW_VOTE_COUNT_LANGUAGES = {"hi", "mr"}
_MIN_VOTE_COUNT_DEFAULT = 10
_MIN_VOTE_COUNT_REGIONAL = 3

# Same problem as vote_count, one bar lower: a flat vote_average>=7 was
# shown to leave 0 legit hi candidates and 1 legit mr candidate
# after the other filters (has-platform, not-dismissed, soap check) - the
# regional TMDB catalog isn't just thinner on votes, its vote_average
# distribution is thinner too. Relaxing to 5.5 for hi/mr yields 7 hi / 3 mr
# candidates, comfortably above the 4/2 language-slot targets; English
# keeps the stricter bar, same rationale as the vote_count split above.
_MIN_VOTE_AVERAGE_DEFAULT = 7
_MIN_VOTE_AVERAGE_REGIONAL = 5.5

def get_candidates(cur, genre_names, languages, limit=80):
    per_lang_limit = max(1, limit // len(languages)) if languages else limit
    candidates = []
    seen_ids = set()
    excluded_genres = get_excluded_genres(cur)

    for lang in languages:
        min_vote_count = _MIN_VOTE_COUNT_REGIONAL if lang in _LOW_VOTE_COUNT_LANGUAGES else _MIN_VOTE_COUNT_DEFAULT
        min_vote_average = _MIN_VOTE_AVERAGE_REGIONAL if lang in _LOW_VOTE_COUNT_LANGUAGES else _MIN_VOTE_AVERAGE_DEFAULT
        genre_filter_sql = "AND g.genre_name = ANY(%s)" if genre_names else ""
        # NOT EXISTS, not a WHERE on the joined genre row - the join/group-by
        # below aggregates ALL of a title's genres into one row, so filtering
        # g.genre_name directly would only drop the one excluded genre link,
        # not the title itself when it also carries a genre the user wants.
        excluded_genre_sql = "AND NOT EXISTS (SELECT 1 FROM marts.bridge_content_genre ebcg JOIN marts.dim_genre eg ON eg.genre_id = ebcg.genre_id WHERE ebcg.content_id = dc.content_id AND eg.genre_name = ANY(%s))" if excluded_genres else ""
        params = [lang, min_vote_count, min_vote_average] + ([genre_names] if genre_names else []) + ([excluded_genres] if excluded_genres else []) + [per_lang_limit]

        cur.execute(f"""
            SELECT
                dc.content_id, dc.title, dc.content_type, dc.primary_creator,
                dc.popularity, dc.vote_average,
                array_agg(DISTINCT g.genre_name) FILTER (WHERE g.genre_name IS NOT NULL) AS genres
            FROM marts.dim_watchable dc
            LEFT JOIN marts.bridge_content_genre bcg ON bcg.content_id = dc.content_id
            LEFT JOIN marts.dim_genre g ON g.genre_id = bcg.genre_id
            WHERE dc.original_language = %s
              AND dc.content_id NOT IN (SELECT content_id FROM marts.fact_watch_history)
              AND dc.content_id NOT IN (SELECT content_id FROM meta.not_interested)
              AND dc.vote_count >= %s
              AND dc.vote_average >= %s
              AND EXISTS (
                  SELECT 1 FROM marts.bridge_content_platform bcp
                  WHERE bcp.content_id = dc.content_id
              )
              -- No daily soaps, TV serials, reality or talk formats. The
              -- rule lives in dim_watchable.is_serial_format so the picks,
              -- the Flask replacement, and any query share one definition.
              AND NOT dc.is_serial_format
              {genre_filter_sql}
              {excluded_genre_sql}
            GROUP BY dc.content_id, dc.title, dc.content_type, dc.primary_creator, dc.popularity, dc.vote_average
            ORDER BY dc.popularity DESC NULLS LAST
            LIMIT %s;
        """, params)

        for cid, title, ctype, creator, popularity, vote_average, genres in cur.fetchall():
            if cid not in seen_ids:
                seen_ids.add(cid)
                candidates.append((cid, title, ctype, creator, lang, genres or [], popularity or 0, vote_average or 0))

    return candidates

def get_language_slots(cur) -> dict:
    cur.execute("""
        SELECT config_value FROM meta.user_config
        WHERE config_key = 'recommendation_language_slots';
    """)
    row = cur.fetchone()
    return row[0] if row else {}

def get_fallback_language(cur) -> str:
    cur.execute("""
        SELECT config_value FROM meta.user_config
        WHERE config_key = 'recommendation_fallback_language';
    """)
    row = cur.fetchone()
    return row[0] if row else None

RANK_ANGLES = {
    1: "genre", 2: "creator_or_language", 3: "regional", 4: "hidden_gem", 5: "contrast",
    6: "genre", 7: "creator_or_language", 8: "regional", 9: "hidden_gem", 10: "contrast",
}

def build_data_grounded_reason(candidate, genre_avg, creator_avg, angle="genre") -> str:
    _, _, content_type, creator, lang, genres, _, _ = candidate
    matched_genre = next((g for g in genres if g in genre_avg), None)
    creator_is_top = bool(creator) and creator in creator_avg
    kind = "show" if content_type == "tv" else "movie"
    lang_name = LANGUAGE_NAMES.get(lang, lang)

    # both signals present is always the strongest thing to say, regardless of angle
    if creator_is_top and matched_genre and angle in ("genre", "creator_or_language"):
        return (
            f"{creator} ({creator_avg[creator]} avg) directing {matched_genre} "
            f"({genre_avg[matched_genre]} avg): your top creator in your top genre."
        )

    if angle == "creator_or_language":
        if creator_is_top:
            return f"{creator}, your top-rated creator at {creator_avg[creator]} avg."
        return f"In {lang_name}, matches your preferred languages."

    if angle == "regional":
        if lang in ("hi", "mr"):
            return f"A {lang_name} pick, matches your interest in regional-language content."
        if matched_genre:
            return f"{matched_genre}, your top-rated genre at {genre_avg[matched_genre]} avg."

    if angle == "hidden_gem":
        # candidate pool already filters vote_average >= 7, so "high quality" is a real claim, not a guess
        if matched_genre:
            return f"A hidden gem in {matched_genre}: high quality, worth discovering."
        if creator_is_top:
            return f"A hidden gem from {creator}: high quality, worth discovering."
        return f"A hidden gem in {lang_name}: high quality, worth discovering."

    if angle == "contrast":
        other_genre = next((g for g in genres if g not in genre_avg), None)
        if other_genre:
            return f"A change of pace: {other_genre} steps outside your usual top genres, worth trying."
        return "A change of pace from your usual picks, worth trying."

    # angle == "genre", or a regional/contrast fallthrough with no genre signal
    if matched_genre:
        return f"{matched_genre}, your top-rated genre at {genre_avg[matched_genre]} avg."
    if creator_is_top:
        return f"{creator}, your top-rated creator at {creator_avg[creator]} avg."
    return f"A highly-rated {kind} in {lang_name}, matching your preferred languages."

def compute_predicted_score(candidate, genre_avg, creator_avg):
    _, _, _, creator, _, genres, _, _ = candidate
    matched_genre = next((g for g in genres if g in genre_avg), None)
    creator_is_top = bool(creator) and creator in creator_avg

    signals = []
    if matched_genre:
        signals.append(genre_avg[matched_genre])
    if creator_is_top:
        signals.append(creator_avg[creator])

    if not signals:
        return None
    return round(sum(signals) / len(signals), 1)

def get_primary_platforms(cur, content_ids) -> dict:
    if not content_ids:
        return {}
    cur.execute("""
        SELECT DISTINCT ON (bcp.content_id) bcp.content_id, dp.platform_name
        FROM marts.bridge_content_platform bcp
        JOIN marts.dim_platform dp ON dp.platform_id = bcp.platform_id
        WHERE bcp.content_id = ANY(%s)
        ORDER BY bcp.content_id, length(dp.platform_name) ASC, dp.platform_name ASC;
    """, (list(content_ids),))
    return dict(cur.fetchall())

def build_shortlist(candidates, genre_avg, creator_avg, limit=40) -> list:
    """Python does all the scoring - the model only picks among what's
    already been ranked, same principle as everywhere else in this pipeline
    (model selects, code computes facts). Cuts the full candidate pool to
    the top N by the same score used everywhere else before the model
    sees them."""
    scored = sorted(
        candidates,
        key=lambda c: (compute_predicted_score(c, genre_avg, creator_avg) or 0, c[6]),
        reverse=True,
    )
    return scored[:limit]

def build_picks_prompt(shortlist, summary_text, genre_avg, creator_avg, platforms) -> str:
    lines = []
    for c in shortlist:
        cid, title, ctype, creator, lang, genres, popularity, vote_average = c
        computed_score = compute_predicted_score(c, genre_avg, creator_avg)
        lines.append(
            f"- content_id: {cid} | title: {title} | content_type: {ctype} | "
            f"primary_creator: {creator or 'unknown'} | genres: {', '.join(genres) if genres else 'unknown'} | "
            f"platform: {platforms.get(cid, 'unknown')} | vote_average: {vote_average} | "
            f"computed_score: {computed_score if computed_score is not None else 'n/a'} | "
            f"original_language: {lang}"
        )
    candidate_block = "\n".join(lines)

    return f"""You are picking tonight's {DAILY_PICKS_COUNT} recommendations from a pre-filtered
shortlist of {len(shortlist)} titles this person hasn't watched yet. Every candidate is
already verified real and available — you are only choosing which
{DAILY_PICKS_COUNT} to feature and in what order, not writing anything about them.

User's taste profile:
{summary_text}

Candidates:
{candidate_block}

Pick exactly {DAILY_PICKS_COUNT}, with real variety — this must be a genuine MIX across
BOTH language and type, not just hitting a total count:
- multiple English, multiple Hindi, and at least 1 Marathi pick, if available
- within each language, include BOTH movies and TV shows where the shortlist
  has both available — do not make one language all-movies and another
  all-TV
- at least 1 "hidden gem" — high vote_average but lower popularity

Reply with ONLY a JSON array of content_id strings in your chosen order, nothing
else, no markdown fences, no reasons, no extra fields:
["movie_123", "tv_456", ...]

Only use content_id values that appear in the candidate list above — never invent one."""

def call_ollama_picks(shortlist, summary_text, genre_avg, creator_avg, platforms, model, log):
    prompt = build_picks_prompt(shortlist, summary_text, genre_avg, creator_avg, platforms)
    start = time.time()
    response = requests.post(
        f"{OLLAMA_BASE_URL}/api/chat",
        json={
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "options": {"temperature": 0.4},
        },
        timeout=120,
    )
    response.raise_for_status()
    elapsed = time.time() - start
    text = response.json()["message"]["content"].strip()
    log.info(f"[ollama] raw output: {text[:500]}")

    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:]
        text = text.strip()

    parsed = json.loads(text)
    if isinstance(parsed, dict):
        # Despite being told "ONLY a JSON array", the model
        # sometimes wraps it in an object anyway (e.g. {"content_id": [...]}).
        # Take the first list-valued field rather than treating the whole
        # dict as one malformed id (which crashed a bare `dict[:5]` before).
        parsed = next((v for v in parsed.values() if isinstance(v, list)), [])
    return parsed, elapsed

def validate_model_picks(raw_ids, shortlist, genre_avg, creator_avg, log) -> list:
    """Every content_id must exist in the shortlist actually sent - never
    trust an id the model returns that it wasn't offered. Invalid/duplicate/
    missing picks get replaced with the next best-scored real candidate, and
    every replacement is logged with the old and new id. No reason is ever
    taken from the model - RANK_ANGLES + build_data_grounded_reason (already
    applied by the caller, based on final rank) is the only source of the
    displayed reason, same as the deterministic path."""
    shortlist_ids = {c[0] for c in shortlist}
    scored_shortlist = sorted(
        shortlist,
        key=lambda c: (compute_predicted_score(c, genre_avg, creator_avg) or 0, c[6]),
        reverse=True,
    )
    used = set()
    validated = []

    for cid in (raw_ids or [])[:DAILY_PICKS_COUNT]:
        if isinstance(cid, dict):
            cid = cid.get("content_id")
        if cid in shortlist_ids and cid not in used:
            used.add(cid)
            validated.append({"content_id": cid})
        else:
            backfill = next((c for c in scored_shortlist if c[0] not in used), None)
            if backfill:
                log.info(f"[ollama] invalid/duplicate content_id {cid!r} replaced with {backfill[0]}")
                used.add(backfill[0])
                validated.append({"content_id": backfill[0]})

    while len(validated) < DAILY_PICKS_COUNT:
        backfill = next((c for c in scored_shortlist if c[0] not in used), None)
        if backfill is None:
            break
        used.add(backfill[0])
        validated.append({"content_id": backfill[0]})

    return validated

def select_picks_deterministic(candidates, genre_avg, creator_avg, count=DAILY_PICKS_COUNT):
    """Fills the same 5 angles the Ollama prompt used to pick for, but by
    real computed scores instead of asking a model - repeated in rounds
    until `count` is reached (2 rounds for the default 10). At this data
    scale (a few hundred candidates) an LLM has nothing to add to
    selection - it can only add latency and non-determinism. Never needs a
    retry or a 'could not parse' fallback: it always returns as many valid
    picks as the real candidate pool can support, or fewer if it's thin.

    After round 1, biases each remaining round toward whichever
    content_type (movie/tv) is currently under-represented among picks so
    far. Without this, rounds naturally cluster on
    whichever type has the deeper candidate pool (English TV), leaving a
    10-pick list that's technically varied by angle but not by type."""
    used = set()
    picks = []

    def pool():
        return [c for c in candidates if c[0] not in used]

    def score(c):
        return compute_predicted_score(c, genre_avg, creator_avg) or 0

    def take(chosen_pool, angle):
        if not chosen_pool:
            return
        movie_count = sum(1 for p, _ in picks if p[2] == "movie")
        tv_count = sum(1 for p, _ in picks if p[2] == "tv")
        under_repped_type = "movie" if movie_count <= tv_count else "tv"
        type_biased = [c for c in chosen_pool if c[2] == under_repped_type]
        best = max(type_biased or chosen_pool, key=score)
        used.add(best[0])
        picks.append((best, angle))

    def one_round():
        # 1. genre: best score among candidates matching a top genre
        take([c for c in pool() if any(g in genre_avg for g in c[5])], "genre")

        # 2. creator match, else best remaining overall (language match by construction -
        # candidates are already restricted to preferred languages)
        creator_pool = [c for c in pool() if c[3] and c[3] in creator_avg]
        take(creator_pool if creator_pool else pool(), "creator_or_language")

        # 3. regional (hi/mr), best score
        take([c for c in pool() if c[4] in ("hi", "mr")], "regional")

        # 4. hidden gem: least mainstream (lowest popularity) among remaining -
        # candidates are already vote_average >= 7 filtered, so "high quality" holds
        gem_pool = pool()
        if gem_pool:
            movie_count = sum(1 for p, _ in picks if p[2] == "movie")
            tv_count = sum(1 for p, _ in picks if p[2] == "tv")
            under_repped_type = "movie" if movie_count <= tv_count else "tv"
            type_biased = [c for c in gem_pool if c[2] == under_repped_type]
            best = min(type_biased or gem_pool, key=lambda c: c[6])
            used.add(best[0])
            picks.append((best, "hidden_gem"))

        # 5. contrast: genres don't overlap top genres at all, else best remaining
        contrast_pool = [c for c in pool() if not any(g in genre_avg for g in c[5])]
        take(contrast_pool if contrast_pool else pool(), "contrast")

    while len(picks) < count and len(pool()) > 0:
        before = len(picks)
        one_round()
        if len(picks) == before:
            break  # pool exhausted mid-round, no point looping again

    return picks[:count]

def fill_language_slot(lang, target_count, picks, candidates, used_ids, lang_by_id, genre_avg, creator_avg) -> list:
    lang_model_picks = [p for p in picks if lang_by_id.get(p["content_id"]) == lang]
    chosen = [p for p in lang_model_picks if p["content_id"] not in used_ids][:target_count]

    while len(chosen) < target_count:
        backfill = next(
            (c for c in candidates
             if c[4] == lang and c[0] not in used_ids
             and c[0] not in {ch["content_id"] for ch in chosen}),
            None
        )
        if backfill is None:
            break
        chosen.append({
            "content_id": backfill[0],
            "reason": build_data_grounded_reason(backfill, genre_avg, creator_avg),
        })

    return chosen

def allocate_by_language(picks, candidates, slots, genre_avg, creator_avg, fallback_lang=None) -> list:
    lang_by_id = {c[0]: c[4] for c in candidates}
    final = []
    used_ids = set()
    shortfall = 0

    for lang, target_count in slots.items():
        chosen = fill_language_slot(lang, target_count, picks, candidates, used_ids, lang_by_id, genre_avg, creator_avg)
        shortfall += target_count - len(chosen)
        final.extend(chosen)
        used_ids.update(c["content_id"] for c in chosen)

    if shortfall > 0 and fallback_lang:
        topup = fill_language_slot(fallback_lang, shortfall, picks, candidates, used_ids, lang_by_id, genre_avg, creator_avg)
        final.extend(topup)
        used_ids.update(c["content_id"] for c in topup)

    return final

def rebalance_content_type(picks, candidates, genre_avg, creator_avg, min_movie_fraction=0.3) -> list:
    """Even with an explicit prompt instruction to mix
    movies and TV per language, the picks model still produced 8 TV / 2
    movies out of 10 despite 8 real English movie candidates sitting
    unused in the shortlist - a known limitation of a small local model
    following soft instructions, same lesson as everywhere else in this
    project. Same fix pattern as allocate_by_language: don't trust the
    model's compliance, rebalance the actual output afterward using real
    scored candidates. Prefers a same-language swap so this doesn't undo
    the language rebalancing that already ran."""
    type_by_id = {c[0]: c[2] for c in candidates}
    lang_by_id = {c[0]: c[4] for c in candidates}
    used_ids = {p["content_id"] for p in picks}

    target_movies = round(len(picks) * min_movie_fraction)
    current_movies = sum(1 for p in picks if type_by_id.get(p["content_id"]) == "movie")
    shortfall = target_movies - current_movies
    if shortfall <= 0:
        return picks

    def score(c):
        return compute_predicted_score(c, genre_avg, creator_avg) or 0

    candidate_by_id = {c[0]: c for c in candidates}
    tv_picks_weakest_first = sorted(
        [p for p in picks if type_by_id.get(p["content_id"]) == "tv"],
        key=lambda p: score(candidate_by_id[p["content_id"]]),
    )

    result = list(picks)
    for weak_pick in tv_picks_weakest_first:
        if shortfall <= 0:
            break
        weak_lang = lang_by_id.get(weak_pick["content_id"])
        replacement = max(
            (c for c in candidates if c[2] == "movie" and c[0] not in used_ids and c[4] == weak_lang),
            key=score, default=None,
        )
        if replacement is None:
            replacement = max(
                (c for c in candidates if c[2] == "movie" and c[0] not in used_ids),
                key=score, default=None,
            )
        if replacement is None:
            continue  # genuinely no movie candidates left anywhere
        idx = result.index(weak_pick)
        result[idx] = {"content_id": replacement[0]}
        used_ids.discard(weak_pick["content_id"])
        used_ids.add(replacement[0])
        shortfall -= 1

    return result

def rebalance_type_within_language(picks, candidates, slots, genre_avg, creator_avg) -> list:
    """rebalance_content_type only enforces a global movie-count FLOOR
    (>=30% overall), which let a language end up 100% one
    type even with the other type sitting unused, because the floor was
    already met by a DIFFERENT language. Concretely: hi/mr's only
    available candidates right now are movies, so allocate_by_language
    (which fills slots in raw popularity order) gave hi+mr 6/6 movies;
    that alone cleared the 30% overall floor, so rebalance_content_type
    never touched en - which backfilled 4/4 TV despite 9 real English
    movie candidates sitting unused, because TV generally out-populates
    movies on popularity. Someone in the mood for an English movie had
    zero on the list. This runs BEFORE the global floor check and
    guarantees at least one of each type per language, but only when
    that language's own candidate pool actually has both types - it
    can't manufacture a Hindi or Marathi TV pick that doesn't exist."""
    by_lang = {}
    for c in candidates:
        by_lang.setdefault(c[4], []).append(c)

    type_by_id = {c[0]: c[2] for c in candidates}
    lang_by_id = {c[0]: c[4] for c in candidates}
    candidate_by_id = {c[0]: c for c in candidates}
    used_ids = {p["content_id"] for p in picks}

    def score(c):
        return compute_predicted_score(c, genre_avg, creator_avg) or 0

    result = list(picks)
    for lang, target_count in slots.items():
        if target_count < 2:
            continue  # nothing to mix with only one slot
        lang_pool = by_lang.get(lang, [])
        available_types = {c[2] for c in lang_pool}
        if len(available_types) < 2:
            continue  # this language genuinely only has one type available - not fixable here

        lang_picks = [p for p in result if lang_by_id.get(p["content_id"]) == lang]
        missing_types = available_types - {type_by_id.get(p["content_id"]) for p in lang_picks}
        if not missing_types:
            continue

        for missing_type in missing_types:
            best_missing = max(
                (c for c in lang_pool if c[2] == missing_type and c[0] not in used_ids),
                key=score, default=None,
            )
            if best_missing is None:
                continue  # every candidate of the missing type is already used elsewhere
            swappable = sorted(
                [p for p in lang_picks if type_by_id.get(p["content_id"]) != missing_type],
                key=lambda p: score(candidate_by_id[p["content_id"]]),
            )
            if not swappable:
                continue
            weakest = swappable[0]
            idx = result.index(weakest)
            result[idx] = {
                "content_id": best_missing[0],
                "reason": build_data_grounded_reason(best_missing, genre_avg, creator_avg),
            }
            used_ids.discard(weakest["content_id"])
            used_ids.add(best_missing[0])
            lang_picks = [p for p in result if lang_by_id.get(p["content_id"]) == lang]

    return result

BOOK_PICKS_COUNT = 5

def get_book_candidates(cur, subject_names, creator_names, limit=30) -> list:
    """Books have no language field and discovered candidates have no
    rating data (OpenLibrary's subject endpoint doesn't return per-work
    ratings) - so this can't reuse get_candidates' vote_count/vote_average/
    language filtering at all. Subject match + author match against the
    reader's own top-rated real subjects/creators is the signal available
    for discovered books; already-enriched books (read via Hardcover, with
    real ratings_average) still get to use that rating when present."""
    cur.execute("""
        SELECT
            dc.content_id, dc.title, dc.primary_creator, dc.vote_average,
            dc.vote_count,
            array_agg(DISTINCT dbs.subject_name) FILTER (WHERE dbs.subject_name IS NOT NULL) AS subjects
        FROM marts.dim_book dc
        LEFT JOIN marts.bridge_content_book_subject bcbs ON bcbs.content_id = dc.content_id
        LEFT JOIN marts.dim_book_subject dbs ON dbs.subject_id = bcbs.subject_id
        WHERE dc.content_id NOT IN (SELECT content_id FROM marts.fact_reading_history)
          AND dc.content_id NOT IN (SELECT content_id FROM meta.not_interested)
        GROUP BY dc.content_id, dc.title, dc.primary_creator, dc.vote_average, dc.vote_count
        LIMIT %s;
    """, (limit,))
    candidates = []
    for cid, title, creator, vote_average, vote_count, subjects in cur.fetchall():
        subjects = subjects or []
        match_count = sum(1 for s in subjects if s in subject_names)
        creator_is_top = bool(creator) and creator in creator_names
        candidates.append((cid, title, creator, vote_average, vote_count, subjects, match_count, creator_is_top))
    return candidates

def select_book_picks(cur, subject_names, creator_names, count=BOOK_PICKS_COUNT) -> list:
    """Author match ranks alongside subject match - same "creator match"
    signal movies/TV already use, now that discovery actually surfaces
    books by top-rated authors (see openlib_op.py). Real rating is the
    tiebreak, only when present - an unrated discovered book is never
    penalized just for lacking OpenLibrary rating data it was never going
    to have."""
    candidates = get_book_candidates(cur, subject_names, creator_names)
    ranked = sorted(
        candidates,
        key=lambda c: (c[6] + (2 if c[7] else 0), c[3] if c[3] is not None else 0),
        reverse=True,
    )
    return ranked[:count]

def build_book_reason(candidate, subject_names) -> str:
    _, title, creator, vote_average, vote_count, subjects, match_count, creator_is_top = candidate
    matched = next((s for s in subjects if s in subject_names), None)
    if creator_is_top and matched:
        return f"By {creator} — your top-rated author, in {matched}."
    if creator_is_top:
        return f"By {creator}, your top-rated author."
    if matched:
        return f"Matches your favorite subject: {matched}."
    if creator:
        return f"By {creator} — worth discovering."
    return "A new title worth discovering."

# ── Op: generate recommendations ────────────────────────────────

@op
def generate_recommendations(context, start=None):
    log = get_dagster_logger()
    conn = get_conn()
    cur = conn.cursor()

    summary_text, genre_names, creator_names, genre_avg, creator_avg = get_latest_watch_profile(cur)
    if summary_text is None:
        log.warning("No taste profile found; run build_taste_profile first.")
        cur.close()
        conn.close()
        return 0

    languages = get_preferred_languages(cur)
    candidates = get_candidates(cur, genre_names, languages)
    if not candidates:
        log.warning("No unwatched candidates found.")
        cur.close()
        conn.close()
        return 0

    id_to_candidate = {c[0]: c for c in candidates}

    # Python does all the data work (candidates, scoring, shortlist) - a local
    # Ollama model only judges variety and picks which 5 to feature, from a
    # shortlist that's already 100% real. It never writes the displayed
    # reason (see validate_model_picks) - every id it returns is checked
    # against the real shortlist before anything reaches the database. Falls
    # back to the proven deterministic selection on any failure (model
    # unreachable, bad JSON, too few valid picks) - no paid API involved
    # anywhere, this is local-only.
    picks = None
    try:
        shortlist = build_shortlist(candidates, genre_avg, creator_avg, limit=40)
        platforms = get_primary_platforms(cur, [c[0] for c in shortlist])
        raw_ids, elapsed = call_ollama_picks(
            shortlist, summary_text, genre_avg, creator_avg, platforms, OLLAMA_PICKS_MODEL, log
        )
        picks = validate_model_picks(raw_ids, shortlist, genre_avg, creator_avg, log)
        log.info(f"Ollama ({OLLAMA_PICKS_MODEL}) selection: {len(picks)}/{DAILY_PICKS_COUNT} picks "
                  f"from a {len(shortlist)}-candidate shortlist in {elapsed:.2f}s")
    except Exception as e:
        log.warning(f"Ollama picks unavailable, using deterministic selection: {e}")
        picks = None

    if not picks:
        ranked_picks = select_picks_deterministic(candidates, genre_avg, creator_avg)
        picks = [{"content_id": c[0]} for c, _ in ranked_picks]
        log.info(f"Deterministic selection: {len(picks)}/{DAILY_PICKS_COUNT} picks from {len(candidates)} candidates")

    if not picks:
        log.warning("No picks could be selected from the candidate pool.")
        cur.close()
        conn.close()
        return 0

    slots = get_language_slots(cur)
    fallback_lang = get_fallback_language(cur)
    if slots:
        before_alloc = {p["content_id"] for p in picks}
        allocated = allocate_by_language(picks, candidates, slots, genre_avg, creator_avg, fallback_lang)
        if allocated:
            picks = allocated
            added = {p["content_id"] for p in picks} - before_alloc
            if added:
                log.info(f"Language allocation backfilled {len(added)} pick(s): {added}")
        else:
            # Allocating returned nothing (slot languages with no candidates):
            # keep the unbalanced picks rather than writing an empty list.
            log.warning(f"Language allocation produced no picks for slots {slots}; keeping unbalanced picks")
    else:
        # Without slot targets allocate_by_language would return an empty
        # list and the DELETE below would leave the picks page blank.
        log.warning("No recommendation_language_slots configured; skipping language allocation")

    before_type_mix = {p["content_id"] for p in picks}
    picks = rebalance_type_within_language(picks, candidates, slots, genre_avg, creator_avg)
    mixed = before_type_mix - {p["content_id"] for p in picks}
    if mixed:
        log.info(f"Within-language type mixing swapped {len(mixed)} pick(s): {mixed}")

    before_rebalance = {p["content_id"] for p in picks}
    picks = rebalance_content_type(picks, candidates, genre_avg, creator_avg)
    swapped = before_rebalance - {p["content_id"] for p in picks}
    if swapped:
        log.info(f"Content-type rebalancing swapped {len(swapped)} TV pick(s) for movies: {swapped}")

    cur.execute("DELETE FROM meta.daily_recommendations_watch;")

    for rank, pick in enumerate(picks[:DAILY_PICKS_COUNT], start=1):
        candidate = id_to_candidate[pick["content_id"]]
        content_id, title, content_type, _, _, _, _, _ = candidate
        angle = RANK_ANGLES.get(rank, "genre")
        reason = pick.get("reason") or build_data_grounded_reason(candidate, genre_avg, creator_avg, angle)
        predicted_score = compute_predicted_score(candidate, genre_avg, creator_avg)
        cur.execute("""
            INSERT INTO meta.daily_recommendations_watch (content_id, title, content_type, rank, reason, predicted_score)
            VALUES (%s,%s,%s,%s,%s,%s);
        """, (content_id, title, content_type, rank, reason, predicted_score))

    # Books are a structurally separate recommendation problem - no
    # language, mostly no rating data on discovered candidates - so they
    # get their own selection function and their own table entirely
    # (meta.daily_recommendations_books) rather than sharing a rank range
    # with movie/TV picks.
    cur.execute("SELECT top_genres_read, top_creators_read FROM meta.taste_profile ORDER BY generated_at DESC LIMIT 1;")
    row = cur.fetchone()
    book_subject_names = [g["name"] for g in (row[0] or [])] if row else []
    book_creator_names = [c["name"] for c in (row[1] or [])] if row else []
    book_picks = select_book_picks(cur, book_subject_names, book_creator_names)
    cur.execute("DELETE FROM meta.daily_recommendations_books;")
    for rank, candidate in enumerate(book_picks, start=1):
        content_id, title, creator, vote_average, vote_count, subjects, match_count, creator_is_top = candidate
        reason = build_book_reason(candidate, book_subject_names)
        cur.execute("""
            INSERT INTO meta.daily_recommendations_books (content_id, title, rank, reason)
            VALUES (%s,%s,%s,%s);
        """, (content_id, title, rank, reason))
    if book_picks:
        log.info(f"Selected {len(book_picks)} book pick(s) via OpenLibrary-sourced candidates")
    else:
        log.info("No book candidates available; run book_discovery_job to find unread books")

    conn.commit()
    cur.close()
    conn.close()
    final_count = len(picks[:DAILY_PICKS_COUNT])
    log.info(f"Generated {final_count} recommendations from {len(candidates)} candidates")
    return final_count
