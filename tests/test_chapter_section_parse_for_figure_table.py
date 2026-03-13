from modules.extract_specific_figure_xml import _parse_chapter_section_expression as parse_figure_section
from modules.extract_specific_table_xml import _parse_chapter_section_expression as parse_table_section


def test_figure_parser_splits_dot_title() -> None:
    assert parse_figure_section("4. 測試1.4") == ("4", "", "測試1.4")


def test_table_parser_splits_dot_title() -> None:
    assert parse_table_section("2. 測試1.2") == ("2", "", "測試1.2")
