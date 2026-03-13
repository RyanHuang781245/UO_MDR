from modules.Extract_AllFile_to_FinalWord import _parse_chapter_section_expression


def test_parse_section_with_dot_and_title() -> None:
    assert _parse_chapter_section_expression("1. 測試1.1") == ("1", "", "測試1.1")


def test_parse_section_range_with_title() -> None:
    assert _parse_chapter_section_expression("1.1.1 - 1.1.3 測試標題") == ("1.1.1", "1.1.3", "測試標題")


def test_parse_section_without_title() -> None:
    assert _parse_chapter_section_expression("1.1.1-1.1.3") == ("1.1.1", "1.1.3", "")
