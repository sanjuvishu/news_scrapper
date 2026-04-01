from __future__ import annotations

from typing import Dict, List

from .utils import normalize_text


class SectorClassifier:
    def __init__(self, sector_keywords: Dict[str, List[str]]) -> None:
        self._sector_keywords = sector_keywords

    def classify(self, title: str, summary: str) -> List[str]:
        combined = normalize_text(f"{title} {summary}")
        matched = [
            sector
            for sector, keywords in self._sector_keywords.items()
            if any(keyword in combined for keyword in keywords)
        ]
        return matched or ["others"]

