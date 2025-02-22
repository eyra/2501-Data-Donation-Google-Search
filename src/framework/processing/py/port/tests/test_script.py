import pytest
import json
import pandas as pd
from port.script import (
    extract_search_data,
    is_google_search_url,
    GoogleTakeoutNotFoundError,
    NoGoogleSearchDataError,
    find_google_search_export,
)
import zipfile
from io import BytesIO


@pytest.fixture
def sample_search_data():
    return [
        {
            "header": "Google Suche",
            "title": "Some search",
            "titleUrl": "https://www.google.com/search?q=test+query",
            "time": "2025-02-09T14:39:34.364Z",  # Changed to February
            "products": ["Google Suche"],
        },
        {
            "header": "Google Suche",
            "title": "Test Result Page",
            "titleUrl": "https://example.com/test",
            "time": "2025-02-09T14:39:42.587Z",  # Changed to February
            "products": ["Google Suche"],
        },
        {
            "header": "Google Suche",
            "title": "Another search",
            "titleUrl": "https://www.google.com/search?q=another+search&hl=en",
            "time": "2025-02-09T14:40:00.000Z",  # Changed to February
            "products": ["Google Suche"],
        },
    ]


@pytest.fixture
def expected_columns():
    return {
        "searches": ["Datum", "Nummer", "Suchbegriff"],
        "clicks": ["Datum", "Nummer", "Suchergebnis", "Link"],
    }


def assert_dataframe_structure(df, expected_cols, df_type="searches"):
    """Helper function to verify dataframe structure and format"""
    assert list(df.columns) == expected_cols
    if len(df) > 0:
        # Check date format (DD-MM-YYYY)
        assert all(df["Datum"].str.match(r"^[0-9]{2}-[0-9]{2}-[0-9]{4}$"))


def test_extract_search_data(sample_search_data, expected_columns):
    searches_df, clicks_df = extract_search_data(sample_search_data)

    # Test searches dataframe
    assert len(searches_df) == 2
    assert_dataframe_structure(searches_df, expected_columns["searches"])
    assert searches_df["Suchbegriff"].tolist() == ["test query", "another search"]
    assert all(searches_df["Datum"].str.startswith("09-02-2025"))  # Changed to February

    # Test clicks dataframe
    assert len(clicks_df) == 1
    assert_dataframe_structure(clicks_df, expected_columns["clicks"])
    assert clicks_df["Suchergebnis"].iloc[0] == "Test Result Page"
    assert clicks_df["Link"].iloc[0] == "https://example.com/test"
    assert clicks_df["Datum"].iloc[0] == "09-02-2025"


def test_extract_search_data_empty(expected_columns):
    searches_df, clicks_df = extract_search_data([])

    assert len(searches_df) == 0
    assert len(clicks_df) == 0
    assert_dataframe_structure(searches_df, expected_columns["searches"])
    assert_dataframe_structure(clicks_df, expected_columns["clicks"])


def test_extract_search_data_malformed(expected_columns):
    malformed_data = [{"header": "Google Suche"}]  # Missing required fields
    searches_df, clicks_df = extract_search_data(malformed_data)

    assert len(searches_df) == 0
    assert len(clicks_df) == 0
    assert_dataframe_structure(searches_df, expected_columns["searches"])
    assert_dataframe_structure(clicks_df, expected_columns["clicks"])


def test_extract_search_data_invalid_input(expected_columns):
    invalid_inputs = [None, "string", 123, {"key": "value"}]

    for data in invalid_inputs:
        searches_df, clicks_df = extract_search_data(data)
        assert len(searches_df) == 0
        assert len(clicks_df) == 0
        assert_dataframe_structure(searches_df, expected_columns["searches"])
        assert_dataframe_structure(clicks_df, expected_columns["clicks"])


def test_extract_search_data_viewed_items_skipped():
    data = [
        {
            "header": "Search",
            "title": "Viewed forbidden knowledge",
            "titleUrl": "https://www.google.com/url?q=forbidden",
            "time": "2025-02-08T09:35:03.562Z",  # Changed to February
            "products": ["Search"],
        },
        {
            "header": "Search",
            "title": "Searched for cookies",
            "titleUrl": "https://www.google.com/search?q=cookies",
            "time": "2025-02-08T09:35:03.562Z",  # Changed to February
            "products": ["Search"],
        },
    ]
    from port.script import extract_search_data

    searches_df, clicks_df = extract_search_data(data)
    assert len(searches_df) == 1
    assert searches_df["Suchbegriff"].iloc[0] == "cookies"
    assert len(clicks_df) == 0


def test_extract_search_data_visited_items_processed():
    data = [
        {
            "header": "Search",
            "title": "Visited example website",
            "titleUrl": "https://www.google.com/url?q=https://example.com",
            "time": "2025-02-08T09:35:03.562Z",
            "products": ["Search"],
        },
        {
            "header": "Search",
            "title": "Viewed another website",
            "titleUrl": "https://www.google.com/url?q=https://another.com",
            "time": "2025-02-08T09:35:03.562Z",
            "products": ["Search"],
        },
    ]
    from port.script import extract_search_data

    searches_df, clicks_df = extract_search_data(data)
    assert len(searches_df) == 0
    assert len(clicks_df) == 1
    assert clicks_df["Suchergebnis"].iloc[0] == "example website"
    assert clicks_df["Link"].iloc[0] == "https://example.com"


@pytest.mark.parametrize(
    "url, expected_query",
    [
        ("https://www.google.com/search?q=test", "test"),
        ("https://www.google.de/search?q=schnee", "schnee"),
        ("https://www.google.nl/search?q=weer&hl=nl", "weer"),
        ("https://www.google.co.uk/search?q=weather", "weather"),
        (
            "https://www.google.com/search?q=test+query&hl=en&source=hp&ei=123",
            "test query",
        ),
        ("https://www.google.de/search?q=m%C3%BCnchen", "münchen"),
    ],
)
def test_is_google_search_url_valid_cases(url, expected_query):
    is_search, query = is_google_search_url(url)
    assert is_search == True
    assert query == expected_query


@pytest.mark.parametrize(
    "url",
    [
        "https://www.google.com/maps",
        "https://www.google.com",
        "https://www.example.com/search?q=test",
        "https://maps.google.com/search?q=test",
        "https://not-google.com/search?q=test",
        "invalid_url",
        "",
        None,
    ],
)
def test_is_google_search_url_invalid_cases(url):
    is_search, query = is_google_search_url(url)
    assert is_search == False
    assert query == None


@pytest.fixture
def takeout_json_data():
    """Base fixture for Google Takeout JSON data"""
    return {
        "header": "Google Suche",
        "title": "Some search",
        "titleUrl": "https://www.google.com/search?q=test",
        "time": "2025-01-09T14:39:34.364Z",
        "products": ["Google Suche"],
    }


@pytest.fixture
def valid_takeout_zip(takeout_json_data):
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        zf.writestr("Takeout/MyActivity.json", json.dumps([takeout_json_data]))
    zip_buffer.seek(0)
    return zipfile.ZipFile(zip_buffer)


@pytest.fixture
def invalid_zip():
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        # Add JSON with wrong structure
        invalid_data = [{"wrong": "structure"}]
        zf.writestr("some_file.json", json.dumps(invalid_data))
    zip_buffer.seek(0)
    return zipfile.ZipFile(zip_buffer)


@pytest.fixture
def multiple_json_zip(takeout_json_data):
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        # Add invalid JSON first
        invalid_data = [{"wrong": "structure"}]
        zf.writestr("invalid.json", json.dumps(invalid_data))

        # Add valid Google Takeout JSON
        zf.writestr("Takeout/MyActivity.json", json.dumps([takeout_json_data]))
    zip_buffer.seek(0)
    return zipfile.ZipFile(zip_buffer)


@pytest.fixture
def maps_data_zip():
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        # Sample of the Maps data
        maps_data = [
            {
                "header": "Maps",
                "title": "Heidelberg Castle",
                "titleUrl": "https://www.google.com/maps/place/Heidelberg+Castle/@49.4106196,8.7153092,16z/data=!3m1!4b1!4m2!3m1!1s0x4797c100ca43db93:0x6d672e3649e97eea",
                "time": "2025-01-08T09:34:22.644Z",
                "products": ["Maps"],
                "activityControls": ["Web & App Activity"],
            },
            {
                "header": "Maps",
                "title": "Searched for tea",
                "titleUrl": "https://www.google.com/maps/search/tea/@49.4091535,8.6775828,15z/data=!3m1!4b1",
                "time": "2025-01-08T09:31:59.365Z",
                "products": ["Maps"],
                "activityControls": ["Web & App Activity"],
                "locationInfos": [
                    {
                        "name": "At this general area",
                        "url": "https://www.google.com/maps/@?api=1&map_action=map&center=49.328947,8.752333&zoom=10",
                        "source": "Based on your past activity",
                    }
                ],
            },
        ]
        zf.writestr("Takeout/Maps/MyActivity.json", json.dumps(maps_data))
    zip_buffer.seek(0)
    return zipfile.ZipFile(zip_buffer)


@pytest.fixture
def search_product_zip():
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        # Add valid Google Takeout JSON with "Search" product
        valid_data = [
            {
                "header": "Search",
                "title": "Searched for heidelberg castle",
                "titleUrl": "https://www.google.com/search?q=heidelberg+castle",
                "time": "2025-01-08T09:34:46.043Z",
                "products": ["Search"],
                "activityControls": ["Web & App Activity"],
            }
        ]
        zf.writestr("Takeout/MyActivity.json", json.dumps(valid_data))
    zip_buffer.seek(0)
    return zipfile.ZipFile(zip_buffer)


def assert_google_search_export(data):
    """Helper function to verify Google Search export data structure"""
    assert isinstance(data, list)
    assert len(data) == 1
    assert all(
        key in data[0] for key in ["header", "title", "time", "products", "titleUrl"]
    )


def test_find_google_search_export_valid(valid_takeout_zip):
    from port.script import find_google_search_export

    data = find_google_search_export(valid_takeout_zip)
    assert_google_search_export(data)


def test_find_google_search_export_invalid(invalid_zip):
    from port.script import find_google_search_export, GoogleTakeoutNotFoundError

    with pytest.raises(GoogleTakeoutNotFoundError):
        find_google_search_export(invalid_zip)


def test_find_google_search_export_multiple(multiple_json_zip):
    from port.script import find_google_search_export

    data = find_google_search_export(multiple_json_zip)
    assert_google_search_export(data)


def test_find_google_search_export_empty_zip():
    from port.script import find_google_search_export, GoogleTakeoutNotFoundError

    empty_buffer = BytesIO()
    with zipfile.ZipFile(empty_buffer, "w") as zf:
        pass  # Create empty zip
    empty_buffer.seek(0)
    empty_zip = zipfile.ZipFile(empty_buffer)

    with pytest.raises(GoogleTakeoutNotFoundError):
        find_google_search_export(empty_zip)


def test_find_google_search_export_maps_data(maps_data_zip):
    from port.script import find_google_search_export, GoogleTakeoutNotFoundError

    with pytest.raises(GoogleTakeoutNotFoundError):
        find_google_search_export(maps_data_zip)


def test_find_google_search_export_search_product(search_product_zip):
    from port.script import find_google_search_export

    data = find_google_search_export(search_product_zip)
    assert_google_search_export(data)
    assert data[0]["products"] == ["Search"]


def test_extract_search_data_with_google_redirect_url():
    data = [
        {
            "header": "Google Suche",
            "title": "Habeck will kein TV-Duell mit Weidel ...",
            "titleUrl": "https://www.google.com/url?q=https://www.tagesschau.de/inland/bundestagswahl/tv-duell-habeck-absage-100.html&usg=AOvVaw14OeCEYWWJzc6cqvroGnXB",
            "time": "2025-02-18T07:49:17.826Z",  # Changed to February
            "products": ["Google Suche"],
        }
    ]
    from port.script import extract_search_data

    searches_df, clicks_df = extract_search_data(data)
    assert len(searches_df) == 0
    assert len(clicks_df) == 1
    assert (
        clicks_df["Link"].iloc[0]
        == "https://www.tagesschau.de/inland/bundestagswahl/tv-duell-habeck-absage-100.html"
    )


def test_extract_search_data_index_enumeration():
    data = [
        {
            "header": "Google Suche",
            "title": "First search",
            "titleUrl": "https://www.google.com/search?q=first",
            "time": "2025-02-09T14:39:34.364Z",  # Changed to February
            "products": ["Google Suche"],
        },
        {
            "header": "Google Suche",
            "title": "Second search",
            "titleUrl": "https://www.google.com/search?q=second",
            "time": "2025-02-09T14:39:35.364Z",  # Changed to February
            "products": ["Google Suche"],
        },
    ]
    searches_df, clicks_df = extract_search_data(data)
    assert list(searches_df["Nummer"]) == ["1", "2"]


def test_extract_search_data_date_range():
    data = [
        {
            "header": "Google Suche",
            "title": "Too early",
            "titleUrl": "https://www.google.com/search?q=early",
            "time": "2025-01-11T20:59:59.000Z",
            "products": ["Google Suche"],
        },
        {
            "header": "Google Suche",
            "title": "In range",
            "titleUrl": "https://www.google.com/search?q=range",
            "time": "2025-01-12T01:00:00.000Z",
            "products": ["Google Suche"],
        },
        {
            "header": "Google Suche",
            "title": "In range 2",
            "titleUrl": "https://www.google.com/search?q=range2",
            "time": "2025-03-02T11:59:59.000Z",
            "products": ["Google Suche"],
        },
        {
            "header": "Google Suche",
            "title": "Too late",
            "titleUrl": "https://www.google.com/search?q=late",
            "time": "2025-03-02T12:00:01.000Z",
            "products": ["Google Suche"],
        },
    ]
    searches_df, clicks_df = extract_search_data(data)
    assert len(searches_df) == 3
    assert list(searches_df["Nummer"]) == ["1", "2", "3"]


def test_extract_search_data_url_titles():
    data = [
        {
            "header": "Google Suche",
            "title": "https://example.com/some/long/path",
            "titleUrl": "https://www.google.com/url?q=https://example.com/some/long/path",
            "time": "2025-02-09T14:39:34.364Z",
            "products": ["Google Suche"],
        },
        {
            "header": "Google Suche",
            "title": "https://www.test-site.org/page",
            "titleUrl": "https://www.google.com/url?q=https://www.test-site.org/page",
            "time": "2025-02-09T14:39:42.587Z",
            "products": ["Google Suche"],
        },
        {
            "header": "Google Suche",
            "title": "Regular Title",
            "titleUrl": "https://example.net/page",
            "time": "2025-02-09T14:40:00.000Z",
            "products": ["Google Suche"],
        },
    ]
    searches_df, clicks_df = extract_search_data(data)

    assert len(clicks_df) == 3
    assert clicks_df["Suchergebnis"].tolist() == [
        "example.com",
        "test-site.org",
        "Regular Title",
    ]


def test_find_google_search_export_empty_google_takeout():
    """Test detection of Google Takeout archives that don't contain search data"""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "w") as zf:
        # Add the Google Takeout marker HTML file
        html_content = '<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Google Data Export Archive Contents</title>'
        zf.writestr("index.html", html_content)
        # Add some other non-search data
        other_data = [{"header": "Maps", "products": ["Maps"]}]
        zf.writestr("Takeout/Maps/MyActivity.json", json.dumps(other_data))
    zip_buffer.seek(0)
    test_zip = zipfile.ZipFile(zip_buffer)

    with pytest.raises(NoGoogleSearchDataError):
        find_google_search_export(test_zip)


def test_extract_search_data_german_viewed_items():
    data = [
        {
            "header": "Search",
            "title": "Normal result angesehen",
            "titleUrl": "https://www.google.com/url?q=https://example.com",
            "time": "2025-02-08T09:35:03.562Z",
            "products": ["Search"],
        },
        {
            "header": "Search",
            "title": "Searched for cookies",
            "titleUrl": "https://www.google.com/search?q=cookies",
            "time": "2025-02-08T09:35:03.562Z",
            "products": ["Search"],
        },
    ]
    from port.script import extract_search_data

    searches_df, clicks_df = extract_search_data(data)
    assert len(searches_df) == 1
    assert searches_df["Suchbegriff"].iloc[0] == "cookies"
    assert len(clicks_df) == 0
