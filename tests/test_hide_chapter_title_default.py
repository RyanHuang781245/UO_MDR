import inspect

from modules.Extract_AllFile_to_FinalWord import extract_word_chapter


def test_extract_word_chapter_hide_title_default_disabled() -> None:
    param = inspect.signature(extract_word_chapter).parameters["hide_chapter_title"]
    assert param.default is False

