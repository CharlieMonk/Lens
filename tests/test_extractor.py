"""Tests for ecfr/extractor.py."""

import pytest
from lxml import etree

from ecfr.extractor import get_element_text, SectionBuilder, XMLExtractor


class TestGetElementText:
    """Tests for get_element_text helper function."""

    def test_simple_text(self):
        """Extract text from simple element."""
        elem = etree.fromstring("<p>Hello world</p>")
        assert get_element_text(elem) == "Hello world"

    def test_nested_elements(self):
        """Extract text from nested elements."""
        elem = etree.fromstring("<p>Hello <b>bold</b> world</p>")
        assert get_element_text(elem) == "Hello bold world"

    def test_deeply_nested(self):
        """Extract text from deeply nested elements."""
        elem = etree.fromstring("<p>A <span>B <em>C</em> D</span> E</p>")
        assert get_element_text(elem) == "A B C D E"

    def test_empty_element(self):
        """Handle empty element."""
        elem = etree.fromstring("<p></p>")
        assert get_element_text(elem) == ""

    def test_whitespace_preservation(self):
        """Preserve whitespace in text."""
        elem = etree.fromstring("<p>  spaced  text  </p>")
        assert get_element_text(elem) == "  spaced  text  "

    def test_tail_text(self):
        """Include tail text after child elements."""
        elem = etree.fromstring("<p>Before <b>middle</b> after</p>")
        assert get_element_text(elem) == "Before middle after"


class TestSectionBuilder:
    """Tests for SectionBuilder class."""

    def test_start_section(self):
        """Start a new section."""
        builder = SectionBuilder()
        builder.start_section({"title": "1", "chapter": "I"}, "1.1")

        sections = builder.get_sections()
        assert len(sections) == 1
        assert sections[0]["section"] == "1.1"
        assert sections[0]["title"] == "1"
        assert sections[0]["chapter"] == "I"

    def test_add_text(self):
        """Add text to current section."""
        builder = SectionBuilder()
        builder.start_section({}, "1.1")
        builder.add_text("First paragraph.")
        builder.add_text("Second paragraph.")

        sections = builder.get_sections()
        assert "First paragraph." in sections[0]["text"]
        assert "Second paragraph." in sections[0]["text"]

    def test_set_heading(self):
        """Set heading for current section."""
        builder = SectionBuilder()
        builder.start_section({}, "1.1")
        builder.set_heading("Test Heading")

        sections = builder.get_sections()
        assert sections[0]["heading"] == "Test Heading"

    def test_word_count(self):
        """Calculate word count correctly."""
        builder = SectionBuilder()
        builder.start_section({}, "1.1")
        builder.add_text("One two three four five.")

        sections = builder.get_sections()
        assert sections[0]["word_count"] == 5

    def test_multiple_sections(self):
        """Handle multiple sections."""
        builder = SectionBuilder()

        builder.start_section({"part": "1"}, "1.1")
        builder.add_text("Section one text.")

        builder.start_section({"part": "1"}, "1.2")
        builder.add_text("Section two text.")

        sections = builder.get_sections()
        assert len(sections) == 2
        assert sections[0]["section"] == "1.1"
        assert sections[1]["section"] == "1.2"

    def test_finalize_without_section(self):
        """Finalize does nothing when no current section."""
        builder = SectionBuilder()
        builder.finalize()  # Should not raise
        assert builder.get_sections() == []

    def test_add_text_without_section(self):
        """Add text does nothing when no current section."""
        builder = SectionBuilder()
        builder.add_text("Orphan text")  # Should not raise
        assert builder.get_sections() == []

    def test_context_defaults(self):
        """Missing context values default to empty string."""
        builder = SectionBuilder()
        builder.start_section({}, "1.1")

        sections = builder.get_sections()
        assert sections[0]["title"] == ""
        assert sections[0]["chapter"] == ""
        assert sections[0]["part"] == ""


class TestXMLExtractor:
    """Tests for XMLExtractor class."""

    @pytest.fixture
    def extractor(self):
        """Create an XMLExtractor instance."""
        return XMLExtractor()

    def test_extract_simple_xml(self, extractor):
        """Extract from simple XML."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <DIV8 TYPE="SECTION" N="1.1">
                <HEAD>Test Section</HEAD>
                <P>Test paragraph.</P>
            </DIV8>
        </ROOT>
        '''
        size, sections, word_counts = extractor.extract(xml, title_num=1)

        assert size > 0
        assert len(sections) == 1
        assert sections[0]["section"] == "1.1"
        assert sections[0]["heading"] == "Test Section"
        assert "Test paragraph" in sections[0]["text"]

    def test_extract_multiple_sections(self, extractor):
        """Extract from XML with multiple sections."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <DIV8 TYPE="SECTION" N="1.1">
                <HEAD>First</HEAD>
                <P>Content one.</P>
            </DIV8>
            <DIV8 TYPE="SECTION" N="1.2">
                <HEAD>Second</HEAD>
                <P>Content two.</P>
            </DIV8>
        </ROOT>
        '''
        size, sections, word_counts = extractor.extract(xml)

        assert len(sections) == 2

    def test_extract_with_hierarchy(self, extractor):
        """Extract from XML with chapter/part hierarchy."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <DIV3 TYPE="CHAPTER" N="I">
                <HEAD>Chapter I</HEAD>
                <DIV5 TYPE="PART" N="1">
                    <HEAD>Part 1</HEAD>
                    <DIV8 TYPE="SECTION" N="1.1">
                        <HEAD>Section</HEAD>
                        <P>Content.</P>
                    </DIV8>
                </DIV5>
            </DIV3>
        </ROOT>
        '''
        size, sections, word_counts = extractor.extract(xml, title_num=1)

        assert len(sections) == 1
        assert sections[0]["chapter"] == "I"
        assert sections[0]["part"] == "1"

    def test_extract_word_counts_by_chapter(self, extractor):
        """Track word counts by chapter."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <DIV3 TYPE="CHAPTER" N="I">
                <DIV8 TYPE="SECTION" N="1.1">
                    <HEAD>Sec</HEAD>
                    <P>One two three.</P>
                </DIV8>
            </DIV3>
            <DIV3 TYPE="CHAPTER" N="II">
                <DIV8 TYPE="SECTION" N="2.1">
                    <HEAD>Sec</HEAD>
                    <P>Four five.</P>
                </DIV8>
            </DIV3>
        </ROOT>
        '''
        size, sections, word_counts = extractor.extract(xml)

        assert "I" in word_counts
        assert "II" in word_counts
        assert word_counts["I"] == 3
        assert word_counts["II"] == 2

    def test_extract_strips_section_prefix(self, extractor):
        """Strip § prefix from section numbers."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <DIV8 TYPE="SECTION" N="\xc2\xa7 1.1">
                <HEAD>Test</HEAD>
                <P>Content.</P>
            </DIV8>
        </ROOT>
        '''
        size, sections, word_counts = extractor.extract(xml)

        assert sections[0]["section"] == "1.1"

    def test_extract_returns_xml_size(self, extractor):
        """Return the XML content size."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <P>Test content.</P>
        </ROOT>
        '''
        size, sections, word_counts = extractor.extract(xml)

        assert size == len(xml)


class TestXMLExtractorGovinfo:
    """Tests for XMLExtractor govinfo methods."""

    @pytest.fixture
    def extractor(self):
        return XMLExtractor()

    def test_extract_govinfo_simple(self, extractor):
        """Extract from simple govinfo XML."""
        xml = b'''<?xml version="1.0"?>
        <CFRDOC>
            <SECTION>
                <SECTNO>1.1</SECTNO>
                <SUBJECT>Test Subject</SUBJECT>
                <P>Test content.</P>
            </SECTION>
        </CFRDOC>
        '''
        size, sections, word_counts = extractor.extract_govinfo(xml, title_num=1)

        assert len(sections) == 1
        assert sections[0]["section"] == "1.1"
        assert sections[0]["heading"] == "Test Subject"

    def test_extract_govinfo_chapter_extraction(self, extractor):
        """Extract chapter from govinfo XML."""
        xml = b'''<?xml version="1.0"?>
        <CFRDOC>
            <CHAPTER>
                <HD>CHAPTER III-TEST AGENCY</HD>
            </CHAPTER>
            <SECTION>
                <SECTNO>3.1</SECTNO>
                <SUBJECT>Test</SUBJECT>
                <P>Content.</P>
            </SECTION>
        </CFRDOC>
        '''
        size, sections, word_counts = extractor.extract_govinfo(xml)

        assert sections[0]["chapter"] == "III"

    def test_extract_govinfo_part_extraction(self, extractor):
        """Extract part number from govinfo XML."""
        xml = b'''<?xml version="1.0"?>
        <CFRDOC>
            <PART>
                <HD>PART 100-GENERAL</HD>
            </PART>
            <SECTION>
                <SECTNO>100.1</SECTNO>
                <SUBJECT>Test</SUBJECT>
                <P>Content.</P>
            </SECTION>
        </CFRDOC>
        '''
        size, sections, word_counts = extractor.extract_govinfo(xml)

        assert sections[0]["part"] == "100"

    def test_extract_govinfo_missing_sectno(self, extractor):
        """Skip sections without SECTNO."""
        xml = b'''<?xml version="1.0"?>
        <CFRDOC>
            <SECTION>
                <SUBJECT>No Section Number</SUBJECT>
                <P>Content.</P>
            </SECTION>
        </CFRDOC>
        '''
        size, sections, word_counts = extractor.extract_govinfo(xml)

        assert len(sections) == 0

    def test_extract_govinfo_volumes(self, extractor):
        """Extract from multiple volume XMLs."""
        vol1 = b'''<?xml version="1.0"?>
        <CFRDOC>
            <SECTION>
                <SECTNO>1.1</SECTNO>
                <SUBJECT>Vol1 Section</SUBJECT>
                <P>Volume 1 content.</P>
            </SECTION>
        </CFRDOC>
        '''
        vol2 = b'''<?xml version="1.0"?>
        <CFRDOC>
            <SECTION>
                <SECTNO>2.1</SECTNO>
                <SUBJECT>Vol2 Section</SUBJECT>
                <P>Volume 2 content.</P>
            </SECTION>
        </CFRDOC>
        '''
        size, sections, word_counts = extractor.extract_govinfo_volumes([vol1, vol2])

        assert len(sections) == 2
        assert sections[0]["section"] == "1.1"
        assert sections[1]["section"] == "2.1"

    def test_extract_chunks(self, extractor):
        """Extract from multiple XML chunks."""
        chunk1 = b'''<?xml version="1.0"?>
        <ROOT>
            <DIV8 TYPE="SECTION" N="1.1">
                <HEAD>Chunk 1</HEAD>
                <P>Content 1.</P>
            </DIV8>
        </ROOT>
        '''
        chunk2 = b'''<?xml version="1.0"?>
        <ROOT>
            <DIV8 TYPE="SECTION" N="1.2">
                <HEAD>Chunk 2</HEAD>
                <P>Content 2.</P>
            </DIV8>
        </ROOT>
        '''
        size, sections, word_counts = extractor.extract_chunks([chunk1, chunk2])

        assert len(sections) == 2
