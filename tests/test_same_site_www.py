from __future__ import annotations

from doj_disclosures.core.utils import is_same_site


def test_is_same_site_treats_www_as_same() -> None:
    assert is_same_site("https://www.justice.gov/epstein", "https://justice.gov/epstein")
    assert is_same_site("https://justice.gov/epstein", "https://www.justice.gov/epstein")
