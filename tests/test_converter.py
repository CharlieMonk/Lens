"""Tests for ecfr/converter.py."""

import tempfile
from pathlib import Path

import pytest
from lxml import etree

from ecfr.converter import get_element_text, SectionBuilder, MarkdownConverter


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


class TestMarkdownConverter:
    """Tests for MarkdownConverter class."""

    @pytest.fixture
    def converter(self):
        """Create a MarkdownConverter instance."""
        return MarkdownConverter()

    @pytest.fixture
    def temp_output(self):
        """Create a temporary output file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir) / "output.md"

    def test_convert_simple_xml(self, converter, temp_output):
        """Convert simple XML to markdown."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <DIV8 TYPE="SECTION" N="1.1">
                <HEAD>Test Section</HEAD>
                <P>Test paragraph.</P>
            </DIV8>
        </ROOT>
        '''
        size, sections, word_counts = converter.convert(xml, temp_output, title_num=1)

        assert size > 0
        assert len(sections) == 1
        assert sections[0]["section"] == "1.1"
        assert sections[0]["heading"] == "Test Section"
        assert "Test paragraph" in sections[0]["text"]

    def test_convert_multiple_sections(self, converter, temp_output):
        """Convert XML with multiple sections."""
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
        size, sections, word_counts = converter.convert(xml, temp_output)

        assert len(sections) == 2

    def test_convert_with_hierarchy(self, converter, temp_output):
        """Convert XML with chapter/part hierarchy."""
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
        size, sections, word_counts = converter.convert(xml, temp_output, title_num=1)

        assert len(sections) == 1
        assert sections[0]["chapter"] == "I"
        assert sections[0]["part"] == "1"

    def test_convert_word_counts_by_chapter(self, converter, temp_output):
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
        size, sections, word_counts = converter.convert(xml, temp_output)

        assert "I" in word_counts
        assert "II" in word_counts
        assert word_counts["I"] == 3
        assert word_counts["II"] == 2

    def test_convert_strips_section_prefix(self, converter, temp_output):
        """Strip § prefix from section numbers."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <DIV8 TYPE="SECTION" N="\xc2\xa7 1.1">
                <HEAD>Test</HEAD>
                <P>Content.</P>
            </DIV8>
        </ROOT>
        '''
        size, sections, word_counts = converter.convert(xml, temp_output)

        assert sections[0]["section"] == "1.1"

    def test_convert_auth_tag(self, converter, temp_output):
        """Handle AUTH tag."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <AUTH>
                <P>Authority text here.</P>
            </AUTH>
        </ROOT>
        '''
        size, sections, word_counts = converter.convert(xml, temp_output)

        content = temp_output.read_text()
        assert "**Authority:**" in content

    def test_convert_source_tag(self, converter, temp_output):
        """Handle SOURCE tag."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <SOURCE>
                <P>Source citation.</P>
            </SOURCE>
        </ROOT>
        '''
        size, sections, word_counts = converter.convert(xml, temp_output)

        content = temp_output.read_text()
        assert "**Source:**" in content

    def test_convert_cita_tag(self, converter, temp_output):
        """Handle CITA tag with italics."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <CITA>Citation text</CITA>
        </ROOT>
        '''
        size, sections, word_counts = converter.convert(xml, temp_output)

        content = temp_output.read_text()
        assert "*Citation text*" in content

    def test_convert_with_agency_lookup(self, temp_output):
        """Include agency metadata when lookup provided."""
        agency_lookup = {
            (1, "I"): [{"agency_slug": "test-agency", "parent_slug": None}]
        }
        converter = MarkdownConverter(agency_lookup)

        # The agency metadata is added when processing HEAD elements
        # for CHAPTER/SUBTITLE/SUBCHAP types
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <DIV1 TYPE="TITLE" N="1">
                <HEAD>Title 1</HEAD>
                <DIV3 TYPE="CHAPTER" N="I">
                    <HEAD>Chapter I</HEAD>
                    <DIV8 TYPE="SECTION" N="1.1">
                        <HEAD>Section</HEAD>
                        <P>Content.</P>
                    </DIV8>
                </DIV3>
            </DIV1>
        </ROOT>
        '''
        size, sections, word_counts = converter.convert(xml, temp_output, title_num=1)

        content = temp_output.read_text()
        assert "Agency Metadata" in content
        assert "test-agency" in content

    def test_convert_collapses_blank_lines(self, converter, temp_output):
        """Collapse multiple blank lines to two."""
        xml = b'''<?xml version="1.0"?>
        <ROOT>
            <P>Para 1</P>
            <P>Para 2</P>
        </ROOT>
        '''
        converter.convert(xml, temp_output)

        content = temp_output.read_text()
        assert "\n\n\n" not in content


class TestMarkdownConverterGovinfo:
    """Tests for MarkdownConverter govinfo methods."""

    @pytest.fixture
    def converter(self):
        return MarkdownConverter()

    @pytest.fixture
    def temp_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir) / "output.md"

    def test_convert_govinfo_simple(self, converter, temp_output):
        """Convert simple govinfo XML."""
        xml = b'''<?xml version="1.0"?>
        <CFRDOC>
            <SECTION>
                <SECTNO>1.1</SECTNO>
                <SUBJECT>Test Subject</SUBJECT>
                <P>Test content.</P>
            </SECTION>
        </CFRDOC>
        '''
        size, sections, word_counts = converter.convert_govinfo(xml, temp_output, title_num=1)

        assert len(sections) == 1
        assert sections[0]["section"] == "1.1"
        assert sections[0]["heading"] == "Test Subject"

    def test_convert_govinfo_chapter_extraction(self, converter, temp_output):
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
        size, sections, word_counts = converter.convert_govinfo(xml, temp_output)

        assert sections[0]["chapter"] == "III"

    def test_convert_govinfo_part_extraction(self, converter, temp_output):
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
        size, sections, word_counts = converter.convert_govinfo(xml, temp_output)

        assert sections[0]["part"] == "100"

    def test_convert_govinfo_missing_sectno(self, converter, temp_output):
        """Skip sections without SECTNO."""
        xml = b'''<?xml version="1.0"?>
        <CFRDOC>
            <SECTION>
                <SUBJECT>No Section Number</SUBJECT>
                <P>Content.</P>
            </SECTION>
        </CFRDOC>
        '''
        size, sections, word_counts = converter.convert_govinfo(xml, temp_output)

        assert len(sections) == 0

    def test_convert_govinfo_volumes(self, converter, temp_output):
        """Convert multiple volume XMLs."""
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
        size, sections, word_counts = converter.convert_govinfo_volumes([vol1, vol2], temp_output)

        assert len(sections) == 2
        assert sections[0]["section"] == "1.1"
        assert sections[1]["section"] == "2.1"

    def test_convert_chunks(self, converter, temp_output):
        """Convert multiple XML chunks."""
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
        size, sections, word_counts = converter.convert_chunks([chunk1, chunk2], temp_output)

        assert len(sections) == 2
