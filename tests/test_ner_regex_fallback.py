from __future__ import annotations

from doj_disclosures.core.ner import canonicalize_entity, extract_entities


def test_regex_ner_extracts_and_dedupes() -> None:
    text = """
    [PAGE 1]
    Contact: John.Doe@Example.com or john.doe@example.com.
    Phone: (212) 555-1212 and 212-555-1212.
    SSN: 123-45-6789.
    URL: https://example.com/a?b=1
    """

    ents = extract_entities(text, enabled=True, engine="regex")

    # Email canonicalization should dedupe case.
    emails = [e for e in ents if e["label"] == "EMAIL"]
    assert len(emails) == 1
    assert emails[0]["canonical"] == "john.doe@example.com"
    assert emails[0]["count"] == 2
    assert 1 in emails[0]["page_nos"]

    phones = [e for e in ents if e["label"] == "PHONE"]
    assert len(phones) == 1
    assert phones[0]["canonical"].endswith("5551212")
    assert phones[0]["count"] == 2

    ssn = [e for e in ents if e["label"] == "SSN"]
    assert len(ssn) == 1
    assert ssn[0]["canonical"] == "123456789"


def test_person_canonicalization_strips_honorific() -> None:
    assert canonicalize_entity("Dr. John Smith", label="PERSON") == "john smith"
