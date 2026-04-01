from news_ingestor.classifier import SectorClassifier


def test_classifier_matches_sector_keywords() -> None:
    classifier = SectorClassifier(
        {
            "technology": ["ai", "cloud"],
            "banking_finance": ["bank"],
        }
    )
    sectors = classifier.classify("AI startup raises funding", "Cloud expansion announced")
    assert "technology" in sectors


def test_classifier_returns_others_when_no_match() -> None:
    classifier = SectorClassifier({"technology": ["chip"]})
    sectors = classifier.classify("Sports update", "Football finals result")
    assert sectors == ["others"]

