import logging

from news_ingestor.classifier import SectorClassifier
from news_ingestor.parser import FeedParserService


def test_parser_xml_fallback_extracts_entries() -> None:
    classifier = SectorClassifier({"technology": ["ai"]})
    parser = FeedParserService(classifier, logging.getLogger("test"))

    xml = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Sample Feed</title>
    <item>
      <title>AI Growth</title>
      <link>https://example.com/ai-growth</link>
      <description>AI is changing markets</description>
      <pubDate>Wed, 02 Oct 2002 13:00:00 GMT</pubDate>
      <guid>abc-123</guid>
    </item>
  </channel>
</rss>
"""
    docs = parser.parse("https://example.com/rss", xml)
    assert len(docs) == 1
    assert docs[0]["title"] == "AI Growth"
    assert docs[0]["feed_title"] == "Sample Feed"
    assert "technology" in docs[0]["sectors"]

