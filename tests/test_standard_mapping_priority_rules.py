from app.services.standard_mapping_service import (
    build_title_with_amendment,
    extract_standard_match_key,
    extract_latest_year_from_astm_style,
    find_latest_year_from_excel,
    normalize_iso_priority,
)


def test_normalize_iso_priority_expands_full_priority_order():
    assert normalize_iso_priority(("ISO", "BS EN ISO")) == (
        "ISO",
        "BS EN ISO",
        "BS EN",
        "EN",
        "EN ISO",
        "BS ISO",
        "BS",
    )


def test_extract_standard_match_key_removes_prefix_year_and_amendment():
    assert (
        extract_standard_match_key("EN ISO 14971:2019/A11:2021", "BS-EN-DIN(歐洲國家標準)")
        == "14971"
    )
    assert extract_standard_match_key("IEC 60601-1-2:2014", "ISO") == "60601-1-2"


def test_extract_latest_year_from_astm_style_prefers_bracket_year():
    assert extract_latest_year_from_astm_style("ASTM F1140/F1140M-13(2025)") == 2025


def test_build_title_with_amendment_supports_plus_a_and_amd_formats():
    assert (
        build_title_with_amendment(
            "Application of risk management to medical devices",
            "BS EN ISO 14971:2019+A11:2021",
        )
        == "Application of risk management to medical devices - Amendment 11"
    )
    assert (
        build_title_with_amendment(
            "Hexalobular internal driving feature for bolts and screws",
            "ISO 16047：2005/Amd 1：2012",
        )
        == "Hexalobular internal driving feature for bolts and screws - Amendment 1"
    )


def test_find_latest_year_prefers_newer_year_before_priority():
    excel_index = {
        "BS-EN-DIN(歐洲國家標準)": [
            {
                "sheet_name": "BS-EN-DIN(歐洲國家標準)",
                "excel_row_index": 2,
                "excel_col_letter": "F",
                "standard_no": "BS EN ISO 14971:2019",
                "standard_match_key": "14971",
                "search_family": "ISO_FAMILY",
                "standard_display_no": "BS EN ISO 14971",
                "standard_level": "BS EN ISO",
                "standard_level_rank": 7,
            },
            {
                "sheet_name": "BS-EN-DIN(歐洲國家標準)",
                "excel_row_index": 3,
                "excel_col_letter": "F",
                "standard_no": "EN ISO 14971:2021",
                "standard_match_key": "14971",
                "search_family": "ISO_FAMILY",
                "standard_display_no": "EN ISO 14971",
                "standard_level": "EN ISO",
                "standard_level_rank": 4,
            },
        ],
        "ISO": [],
        "ASTM": [],
    }

    result = find_latest_year_from_excel(
        "ISO 14971:2019",
        excel_index,
        ("BS EN ISO", "BS EN", "EN", "EN ISO", "BS ISO", "ISO", "BS"),
    )

    assert result is not None
    assert result["matched_standard_no"] == "EN ISO 14971:2021"
    assert result["latest_year"] == 2021


def test_find_latest_year_prefers_en_group_over_newer_non_en_candidate():
    excel_index = {
        "BS-EN-DIN(歐洲國家標準)": [
            {
                "sheet_name": "BS-EN-DIN(歐洲國家標準)",
                "excel_row_index": 2,
                "excel_col_letter": "F",
                "standard_no": "EN ISO 14971:2021",
                "standard_match_key": "14971",
                "search_family": "ISO_FAMILY",
                "standard_display_no": "EN ISO 14971",
                "standard_level": "EN ISO",
                "standard_level_rank": 4,
            }
        ],
        "ISO": [
            {
                "sheet_name": "ISO",
                "excel_row_index": 4,
                "excel_col_letter": "F",
                "standard_no": "ISO 14971:2023",
                "standard_match_key": "14971",
                "search_family": "ISO_FAMILY",
                "standard_display_no": "ISO 14971",
                "standard_level": "ISO",
                "standard_level_rank": 2,
            }
        ],
        "ASTM": [],
    }

    result = find_latest_year_from_excel(
        "ISO 14971:2019",
        excel_index,
        ("BS EN ISO", "BS EN", "EN", "EN ISO", "BS ISO", "ISO", "BS"),
    )

    assert result is not None
    assert result["matched_standard_no"] == "EN ISO 14971:2021"
    assert any(
        candidate["matched_standard_no"] == "ISO 14971:2023" and candidate["decision"] == "excluded"
        for candidate in result["all_candidates"]
    )


def test_find_latest_year_uses_priority_as_tiebreaker():
    excel_index = {
        "BS-EN-DIN(歐洲國家標準)": [
            {
                "sheet_name": "BS-EN-DIN(歐洲國家標準)",
                "excel_row_index": 2,
                "excel_col_letter": "F",
                "standard_no": "EN ISO 14971:2021",
                "standard_match_key": "14971",
                "search_family": "ISO_FAMILY",
                "standard_display_no": "EN ISO 14971",
                "standard_level": "EN ISO",
                "standard_level_rank": 4,
            },
            {
                "sheet_name": "BS-EN-DIN(歐洲國家標準)",
                "excel_row_index": 3,
                "excel_col_letter": "F",
                "standard_no": "BS EN ISO 14971:2021",
                "standard_match_key": "14971",
                "search_family": "ISO_FAMILY",
                "standard_display_no": "BS EN ISO 14971",
                "standard_level": "BS EN ISO",
                "standard_level_rank": 7,
            },
        ],
        "ISO": [],
        "ASTM": [],
    }

    result = find_latest_year_from_excel(
        "ISO 14971:2019",
        excel_index,
        ("BS EN ISO", "BS EN", "EN", "EN ISO", "BS ISO", "ISO", "BS"),
    )

    assert result is not None
    assert result["matched_standard_no"] == "BS EN ISO 14971:2021"


def test_find_latest_year_candidate_title_includes_amendment_suffix():
    excel_index = {
        "BS-EN-DIN(歐洲國家標準)": [
            {
                "sheet_name": "BS-EN-DIN(歐洲國家標準)",
                "excel_row_index": 3,
                "excel_col_letter": "F",
                "standard_no": "BS EN ISO 14971:2019+A11:2021",
                "standard_title": "Application of risk management to medical devices",
                "standard_match_key": "14971",
                "search_family": "ISO_FAMILY",
                "standard_display_no": "BS EN ISO 14971",
                "standard_level": "BS EN ISO",
                "standard_level_rank": 7,
            },
        ],
        "ISO": [],
        "ASTM": [],
    }

    result = find_latest_year_from_excel(
        "ISO 14971:2019",
        excel_index,
        ("BS EN ISO", "BS EN", "EN", "EN ISO", "BS ISO", "ISO", "BS"),
    )

    assert result is not None
    assert result["matched_title"] == "Application of risk management to medical devices - Amendment 11"
    assert result["all_candidates"][0]["matched_title"] == "Application of risk management to medical devices - Amendment 11"


def test_find_latest_year_keeps_iec_candidate_as_fallback():
    excel_index = {
        "BS-EN-DIN(歐洲國家標準)": [],
        "ISO": [
            {
                "sheet_name": "ISO",
                "excel_row_index": 2,
                "excel_col_letter": "F",
                "standard_no": "IEC 60601-1:2005",
                "standard_match_key": "60601-1",
                "search_family": "IEC_FAMILY",
                "standard_display_no": "IEC 60601-1",
                "standard_level": "IEC",
                "standard_level_rank": 1,
            }
        ],
        "ASTM": [],
    }

    result = find_latest_year_from_excel(
        "IEC 60601-1:2005",
        excel_index,
        ("BS EN ISO", "BS EN", "EN", "EN ISO", "BS ISO", "ISO", "BS"),
    )

    assert result is not None
    assert result["matched_standard_no"] == "IEC 60601-1:2005"
