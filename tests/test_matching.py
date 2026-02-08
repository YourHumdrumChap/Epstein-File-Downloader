from __future__ import annotations

from doj_disclosures.core.matching import KeywordMatcher


def test_keyword_regex_wildcard_and_query() -> None:
    m = KeywordMatcher(
        keywords=["flight log", "re:minor\\s+victim", "esc*"],
        query='"flight" AND NOT "unrelated"',
        fuzzy_enabled=False,
    )
    text = "This flight log mentions a minor victim and escort services."
    hits = m.match(text)
    methods = {h.method for h in hits}
    assert "keyword" in methods
    assert "regex" in methods
    assert "wildcard" in methods
    assert "query" in methods


def test_literal_uses_word_boundaries() -> None:
    m = KeywordMatcher(keywords=["art"], fuzzy_enabled=False)
    hits1 = m.match("This is partial evidence.")
    assert not hits1
    hits2 = m.match("This is art.")
    assert any(h.method == "keyword" for h in hits2)


def test_phrase_matches_across_whitespace_and_near_is_phrase_aware() -> None:
    m = KeywordMatcher(
        keywords=["flight log"],
        query='"flight log" NEAR/5 "minor victim"',
        fuzzy_enabled=False,
    )
    text = "The flight\nlog mentions a minor victim in the notes."
    hits = m.match(text)
    methods = {h.method for h in hits}
    assert "keyword" in methods
    assert "query" in methods

    # Far apart should not satisfy NEAR
    text2 = "flight log " + ("x " * 50) + "minor victim"
    hits2 = m.match(text2)
    assert not any(h.method == "query" for h in hits2)
