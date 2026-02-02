"""Tests for ecfr/extractor.py."""

import pytest
from lxml import etree
from ecfr.extractor import get_element_text, XMLExtractor


class TestGetElementText:
    def test_simple_text(self):
        assert get_element_text(etree.fromstring("<p>Hello world</p>")) == "Hello world"

    def test_nested_elements(self):
        assert get_element_text(etree.fromstring("<p>Hello <b>bold</b> world</p>")) == "Hello bold world"

    def test_deeply_nested(self):
        assert get_element_text(etree.fromstring("<p>A <span>B <em>C</em> D</span> E</p>")) == "A B C D E"

    def test_empty_element(self):
        assert get_element_text(etree.fromstring("<p></p>")) == ""

    def test_tail_text(self):
        assert get_element_text(etree.fromstring("<p>Before <b>middle</b> after</p>")) == "Before middle after"


class TestXMLExtractor:
    @pytest.fixture
    def extractor(self):
        return XMLExtractor()

    def test_extract_simple_xml(self, extractor):
        xml = b'''<?xml version="1.0"?><ROOT><DIV8 TYPE="SECTION" N="1.1"><HEAD>Test Section</HEAD><P>Test paragraph.</P></DIV8></ROOT>'''
        size, sections, _ = extractor.extract(xml, title_num=1)
        assert size > 0 and len(sections) == 1 and sections[0]["section"] == "1.1" and sections[0]["heading"] == "Test Section"

    def test_extract_multiple_sections(self, extractor):
        xml = b'''<?xml version="1.0"?><ROOT><DIV8 TYPE="SECTION" N="1.1"><HEAD>First</HEAD><P>Content one.</P></DIV8><DIV8 TYPE="SECTION" N="1.2"><HEAD>Second</HEAD><P>Content two.</P></DIV8></ROOT>'''
        _, sections, _ = extractor.extract(xml)
        assert len(sections) == 2

    def test_extract_with_hierarchy(self, extractor):
        xml = b'''<?xml version="1.0"?><ROOT><DIV3 TYPE="CHAPTER" N="I"><HEAD>Chapter I</HEAD><DIV5 TYPE="PART" N="1"><HEAD>Part 1</HEAD><DIV8 TYPE="SECTION" N="1.1"><HEAD>Section</HEAD><P>Content.</P></DIV8></DIV5></DIV3></ROOT>'''
        _, sections, _ = extractor.extract(xml, title_num=1)
        assert len(sections) == 1 and sections[0]["chapter"] == "I" and sections[0]["part"] == "1"

    def test_extract_word_counts_by_chapter(self, extractor):
        xml = b'''<?xml version="1.0"?><ROOT><DIV3 TYPE="CHAPTER" N="I"><DIV8 TYPE="SECTION" N="1.1"><HEAD>Sec</HEAD><P>One two three.</P></DIV8></DIV3><DIV3 TYPE="CHAPTER" N="II"><DIV8 TYPE="SECTION" N="2.1"><HEAD>Sec</HEAD><P>Four five.</P></DIV8></DIV3></ROOT>'''
        _, _, word_counts = extractor.extract(xml)
        assert word_counts.get("I") == 3 and word_counts.get("II") == 2

    def test_extract_strips_section_prefix(self, extractor):
        xml = b'''<?xml version="1.0"?><ROOT><DIV8 TYPE="SECTION" N="\xc2\xa7 1.1"><HEAD>Test</HEAD><P>Content.</P></DIV8></ROOT>'''
        _, sections, _ = extractor.extract(xml)
        assert sections[0]["section"] == "1.1"


class TestXMLExtractorGovinfo:
    @pytest.fixture
    def extractor(self):
        return XMLExtractor()

    def test_extract_govinfo_simple(self, extractor):
        xml = b'''<?xml version="1.0"?><CFRDOC><SECTION><SECTNO>1.1</SECTNO><SUBJECT>Test Subject</SUBJECT><P>Test content.</P></SECTION></CFRDOC>'''
        _, sections, _ = extractor.extract_govinfo(xml, title_num=1)
        assert len(sections) == 1 and sections[0]["section"] == "1.1" and sections[0]["heading"] == "Test Subject"

    def test_extract_govinfo_chapter_extraction(self, extractor):
        xml = b'''<?xml version="1.0"?><CFRDOC><CHAPTER><HD>CHAPTER III-TEST AGENCY</HD></CHAPTER><SECTION><SECTNO>3.1</SECTNO><SUBJECT>Test</SUBJECT><P>Content.</P></SECTION></CFRDOC>'''
        _, sections, _ = extractor.extract_govinfo(xml)
        assert sections[0]["chapter"] == "III"

    def test_extract_govinfo_part_extraction(self, extractor):
        xml = b'''<?xml version="1.0"?><CFRDOC><PART><HD>PART 100-GENERAL</HD></PART><SECTION><SECTNO>100.1</SECTNO><SUBJECT>Test</SUBJECT><P>Content.</P></SECTION></CFRDOC>'''
        _, sections, _ = extractor.extract_govinfo(xml)
        assert sections[0]["part"] == "100"

    def test_extract_govinfo_missing_sectno(self, extractor):
        xml = b'''<?xml version="1.0"?><CFRDOC><SECTION><SUBJECT>No Section Number</SUBJECT><P>Content.</P></SECTION></CFRDOC>'''
        _, sections, _ = extractor.extract_govinfo(xml)
        assert len(sections) == 0

    def test_extract_govinfo_volumes(self, extractor):
        vol1 = b'''<?xml version="1.0"?><CFRDOC><SECTION><SECTNO>1.1</SECTNO><SUBJECT>Vol1</SUBJECT><P>Vol 1.</P></SECTION></CFRDOC>'''
        vol2 = b'''<?xml version="1.0"?><CFRDOC><SECTION><SECTNO>2.1</SECTNO><SUBJECT>Vol2</SUBJECT><P>Vol 2.</P></SECTION></CFRDOC>'''
        _, sections, _ = extractor.extract_govinfo_volumes([vol1, vol2])
        assert len(sections) == 2

    def test_extract_chunks(self, extractor):
        chunk1 = b'''<?xml version="1.0"?><ROOT><DIV8 TYPE="SECTION" N="1.1"><HEAD>Chunk 1</HEAD><P>Content 1.</P></DIV8></ROOT>'''
        chunk2 = b'''<?xml version="1.0"?><ROOT><DIV8 TYPE="SECTION" N="1.2"><HEAD>Chunk 2</HEAD><P>Content 2.</P></DIV8></ROOT>'''
        _, sections, _ = extractor.extract_chunks([chunk1, chunk2])
        assert len(sections) == 2
