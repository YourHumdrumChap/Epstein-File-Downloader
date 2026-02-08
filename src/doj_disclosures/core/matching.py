from __future__ import annotations

import fnmatch
import logging
import re
from dataclasses import dataclass

from rapidfuzz import fuzz

from doj_disclosures.core.utils import chunk_text, snippet_around

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MatchHit:
    method: str
    pattern: str
    score: float
    snippet: str


class KeywordMatcher:
    def __init__(
        self,
        *,
        keywords: list[str],
        query: str = "",
        fuzzy_enabled: bool = True,
        semantic_enabled: bool = False,
        semantic_threshold: float = 0.62,
        stopwords: set[str] | None = None,
    ) -> None:
        self._keywords = [k.strip() for k in keywords if k.strip()]
        self._query = query.strip()
        self._fuzzy = fuzzy_enabled
        self._semantic_enabled = semantic_enabled
        self._semantic_threshold = semantic_threshold
        self._stopwords = stopwords or set()

        self._regexes: list[tuple[str, re.Pattern[str]]] = []
        self._wildcards: list[str] = []
        self._literals: list[str] = []
        self._literal_regexes: list[tuple[str, re.Pattern[str]]] = []
        for kw in self._keywords:
            if kw.startswith("re:"):
                pat = kw[3:].strip()
                try:
                    self._regexes.append((kw, re.compile(pat, flags=re.IGNORECASE)))
                except re.error:
                    continue
            elif "*" in kw or "?" in kw:
                self._wildcards.append(kw)
            else:
                self._literals.append(kw)

        # Precompile literal patterns with word boundaries to reduce false positives
        # from substring matching (e.g., "art" matching "partial").
        for kw in self._literals:
            k = kw.strip()
            if not k:
                continue
            # Phrase: split into word tokens and allow flexible whitespace between.
            tokens = re.findall(r"\w+", k, flags=re.UNICODE)
            if not tokens:
                continue
            if len(tokens) == 1:
                pat = rf"(?<!\w){re.escape(tokens[0])}(?!\w)"
            else:
                pat = rf"(?<!\w){r'\s+'.join(re.escape(t) for t in tokens)}(?!\w)"
            try:
                self._literal_regexes.append((kw, re.compile(pat, flags=re.IGNORECASE | re.UNICODE)))
            except re.error:
                continue

        self._semantic = None
        if semantic_enabled:
            try:
                from doj_disclosures.core.semantic import SemanticMatcher

                self._semantic = SemanticMatcher(threshold=semantic_threshold)
            except Exception as e:
                logger.warning("Semantic mode unavailable: %s", e)
                self._semantic_enabled = False

    def match(self, text: str) -> list[MatchHit]:
        hits: list[MatchHit] = []
        if not text.strip():
            return hits

        # Boundary-aware literal matching (single words + phrases).
        for kw, rx in self._literal_regexes:
            k = kw.lower().strip()
            if k in self._stopwords:
                continue
            for m in rx.finditer(text):
                sn = snippet_around(text, m.start(), m.end()).snippet
                hits.append(MatchHit(method="keyword", pattern=kw, score=1.0, snippet=sn))
                if len(hits) > 200:
                    break

        for pat in self._wildcards:
            for m in re.finditer(r"\b[\w\-']+\b", text, flags=re.UNICODE):
                w = m.group(0)
                if fnmatch.fnmatch(w.lower(), pat.lower()):
                    sn = snippet_around(text, m.start(), m.end()).snippet
                    hits.append(MatchHit(method="wildcard", pattern=pat, score=1.0, snippet=sn))

        for original, rx in self._regexes:
            for m in rx.finditer(text):
                sn = snippet_around(text, m.start(), m.end()).snippet
                hits.append(MatchHit(method="regex", pattern=original, score=1.0, snippet=sn))
                if len(hits) > 200:
                    break

        if self._query:
            hits.extend(BooleanQueryEngine().evaluate(self._query, text))

        if self._fuzzy and self._literals:
            sentences = [s.strip() for s in re.split(r"[\n\.\?\!]+", text) if s.strip()]
            for kw in self._literals[:200]:
                # Fuzzy matching is intentionally conservative to reduce false positives.
                # Skip single-word and very short keywords.
                tokens = re.findall(r"\w+", kw, flags=re.UNICODE)
                if len(tokens) <= 1:
                    continue
                if len(kw.strip()) < 8:
                    continue
                best = 0.0
                best_sent = ""
                for sent in sentences[:1500]:
                    score = fuzz.token_set_ratio(kw.lower(), sent.lower()) / 100.0
                    if score > best:
                        best = score
                        best_sent = sent
                if best >= 0.92:
                    hits.append(MatchHit(method="fuzzy", pattern=kw, score=best, snippet=best_sent[:350]))

        if self._semantic_enabled and self._semantic is not None:
            for chunk in chunk_text(text, max_chars=3000, overlap=200):
                hits.extend(self._semantic.match(chunk, self._keywords))

        seen: set[tuple[str, str, str]] = set()
        unique: list[MatchHit] = []
        for h in sorted(hits, key=lambda x: x.score, reverse=True):
            key = (h.method, h.pattern, h.snippet)
            if key in seen:
                continue
            seen.add(key)
            unique.append(h)
        return unique


class BooleanQueryEngine:
    _token_re = re.compile(
        r'\s*(\(|\)|AND\b|OR\b|NOT\b|NEAR/\d+\b|"[^"]+"|[^\s()]+)\s*',
        flags=re.IGNORECASE,
    )

    def tokenize(self, query: str) -> list[str]:
        return [t for t in self._token_re.findall(query) if t.strip()]

    def evaluate(self, query: str, text: str) -> list[MatchHit]:
        try:
            rpn = self._parse_to_rpn(self.tokenize(query))
            ok, detail = self._eval_rpn(rpn, text)
            if ok:
                return [MatchHit(method="query", pattern=query, score=1.0, snippet=detail)]
            return []
        except Exception:
            return []

    def _parse_to_rpn(self, tokens: list[str]) -> list[str]:
        prec = {"NOT": 3, "NEAR": 2, "AND": 2, "OR": 1}
        out: list[str] = []
        stack: list[str] = []

        def op_key(tok: str) -> str:
            if tok.upper().startswith("NEAR/"):
                return "NEAR"
            return tok.upper()

        for tok in tokens:
            u = tok.upper()
            if tok == "(":
                stack.append(tok)
            elif tok == ")":
                while stack and stack[-1] != "(":
                    out.append(stack.pop())
                if stack and stack[-1] == "(":
                    stack.pop()
            elif u in ("AND", "OR", "NOT") or u.startswith("NEAR/"):
                k = op_key(tok)
                while stack and stack[-1] != "(" and prec.get(op_key(stack[-1]), 0) >= prec.get(k, 0):
                    out.append(stack.pop())
                stack.append(tok)
            else:
                out.append(tok)
        while stack:
            out.append(stack.pop())
        return out

    @staticmethod
    def _term_tokens(term: str) -> list[str]:
        term = term.strip()
        if term.startswith('"') and term.endswith('"'):
            term = term[1:-1]
        return re.findall(r"\w+", term, flags=re.UNICODE)

    def _term_present(self, term: str, text: str) -> bool:
        tokens = self._term_tokens(term)
        if not tokens:
            return False
        if len(tokens) == 1:
            pat = rf"(?<!\w){re.escape(tokens[0])}(?!\w)"
        else:
            pat = rf"(?<!\w){r'\s+'.join(re.escape(t) for t in tokens)}(?!\w)"
        try:
            return re.search(pat, text, flags=re.IGNORECASE | re.UNICODE) is not None
        except re.error:
            return False

    @staticmethod
    def _phrase_positions(words: list[str], phrase: list[str]) -> list[int]:
        if not phrase:
            return []
        if len(phrase) == 1:
            w0 = phrase[0]
            return [i for i, w in enumerate(words) if w == w0]
        positions: list[int] = []
        plen = len(phrase)
        for i in range(0, max(0, len(words) - plen + 1)):
            if words[i : i + plen] == phrase:
                positions.append(i)
        return positions

    def _near_present(self, left: str, right: str, n: int, text: str) -> bool:
        words = re.findall(r"\w+", text.lower(), flags=re.UNICODE)
        left_phrase = [w.lower() for w in self._term_tokens(left)]
        right_phrase = [w.lower() for w in self._term_tokens(right)]
        if not left_phrase or not right_phrase:
            return False
        left_pos = self._phrase_positions(words, left_phrase)
        right_pos = self._phrase_positions(words, right_phrase)
        if not left_pos or not right_pos:
            return False
        # Distance in words between phrase starts.
        for i in left_pos:
            for j in right_pos:
                if abs(i - j) <= n:
                    return True
        return False

    def _eval_rpn(self, rpn: list[str], text: str) -> tuple[bool, str]:
        stack: list[tuple[bool, str]] = []
        for tok in rpn:
            u = tok.upper()
            if u == "NOT":
                a, detail = stack.pop()
                stack.append((not a, f"NOT({detail})"))
            elif u == "AND":
                b, bd = stack.pop()
                a, ad = stack.pop()
                stack.append((a and b, f"({ad} AND {bd})"))
            elif u == "OR":
                b, bd = stack.pop()
                a, ad = stack.pop()
                stack.append((a or b, f"({ad} OR {bd})"))
            elif u.startswith("NEAR/"):
                n = int(u.split("/", 1)[1])
                right_ok, rd = stack.pop()
                left_ok, ld = stack.pop()
                ok = self._near_present(ld, rd, n, text)
                stack.append((ok, f"{ld} NEAR/{n} {rd}"))
            else:
                present = self._term_present(tok, text)
                stack.append((present, tok))
        if not stack:
            return False, ""
        return stack[-1]
