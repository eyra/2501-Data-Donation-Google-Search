import port.api.props as props
from port.api.assets import *
from port.api.commands import CommandSystemDonate, CommandSystemExit, CommandUIRender

import pandas as pd
import zipfile
import json
import time
from urllib.parse import urlparse, parse_qs

# Use timezone-aware timestamps for comparison
start_dt = pd.to_datetime("2025-01-12").tz_localize("CET")
end_dt = pd.to_datetime("2025-03-02").tz_localize("CET") + pd.Timedelta(
    days=1
)  # Include all of March 2nd


def process(sessionId):
    key = "google-search-history"
    meta_data = []
    meta_data.append(("debug", f"{key}: start"))

    # STEP 1: select the file
    data = None
    while True:
        meta_data.append(("debug", f"{key}: prompt file"))
        promptFile = prompt_file("application/zip")
        fileResult = yield render_donation_page(promptFile)

        if fileResult.__type__ == "PayloadString":
            try:
                # Try to find and extract Google Takeout data
                meta_data.append(("debug", f"{key}: searching for Google Takeout data"))
                zipfile_ref = get_zipfile(fileResult.value)

                if zipfile_ref == "invalid":
                    raise Exception("Invalid zip file")

                # Find and parse the Google Search export
                json_data = find_google_search_export(zipfile_ref)
                meta_data.append(("debug", f"{key}: found valid Google Takeout data"))

                # Extract search history and clicks into dataframes
                meta_data.append(("debug", f"{key}: extracting search history"))
                data = extract_search_data(json_data)
                break

            except NoGoogleSearchDataError:
                value = json.dumps('{"status" : "no-search-data"}')
                yield donate(f"{sessionId}-{key}", value)
                yield render_no_search_data_page()
                return

            except GoogleTakeoutNotFoundError:
                meta_data.append(
                    ("debug", f"{key}: no valid Google Takeout data found")
                )
                retry_result = yield render_donation_page(retry_confirmation())
                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    value = json.dumps('{"status" : "no-google-takeout-found"}')
                    yield donate(f"{sessionId}-{key}", value)
                    return
            except Exception as e:
                meta_data.append(("debug", f"{key}: error processing file - {str(e)}"))
                retry_result = yield render_donation_page(retry_confirmation())
                if retry_result.__type__ == "PayloadTrue":
                    continue
                else:
                    meta_data.append(("debug", f"{key}: user cancelled"))
                    break

    # STEP 2: ask for consent
    if data is not None:
        meta_data.append(("debug", f"{key}: prompt consent"))
        prompt = prompt_consent(data, meta_data)
        consent_result = yield render_donation_page(prompt)
        if consent_result.__type__ == "PayloadJSON":
            meta_data.append(("debug", f"{key}: donate consent data"))
            yield donate(f"{sessionId}-{key}", consent_result.value)
        if consent_result.__type__ == "PayloadFalse":
            value = json.dumps('{"status" : "donation declined"}')
            yield donate(f"{sessionId}-{key}", value)


def render_donation_page(body):
    header = props.PropsUIHeader(
        props.Translatable(
            {
                "en": "Google Search History",
                "de": "Google Suchverlauf",
                "nl": "Google Zoekgeschiedenis",
            }
        )
    )

    page = props.PropsUIPageDonation("Zip", header, body)
    return CommandUIRender(page)


def render_no_search_data_page():
    header = props.PropsUIHeader(
        props.Translatable(
            {
                "en": "No Google Search Data",
                "de": "Keine Google Suchdaten",
                "nl": "Geen Google Zoekgegevens",
            }
        )
    )

    body = props.PropsUIPromptConfirm(
        text=props.Translatable(
            {
                "en": "Your data package does not contain any Google search data, either because you did not request it during data export, or your privacy settings at Google are set this way. By clicking End, you can complete your study participation.",
                "de": "Ihr Datenpaket enthält keine Google Suchdaten, da Sie diese entweder beim Datenexport nicht angefordert haben, oder Ihre Privatsphäre-Einstellungen bei Google dies so festlegen. Mit Klicken auf Beenden können Sie Ihre Studienteilnahme abschließen.",
                "nl": "Uw datapakket bevat geen Google-zoekgegevens, omdat u deze niet hebt aangevraagd tijdens de gegevensexport, of omdat uw privacy-instellingen bij Google zo zijn ingesteld. Door op Beëindigen te klikken, kunt u uw deelname aan de studie voltooien.",
            }
        ),
        ok=props.Translatable(
            {
                "en": "End",
                "de": "Beenden",
                "nl": "Beëindigen",
            }
        ),
    )
    page = props.PropsUIPageDonation("NoGoogleSearchData", header, body)
    return CommandUIRender(page)


def retry_confirmation():
    text = props.Translatable(
        {
            "en": "Unfortunately, we cannot process your file. Continue, if you are sure that you selected the right file. Try again to select a different file.",
            "de": "Leider können wir Ihre Datei nicht bearbeiten. Fahren Sie fort, wenn Sie sicher sind, dass Sie die richtige Datei ausgewählt haben. Versuchen Sie, eine andere Datei auszuwählen.",
            "nl": "Helaas, kunnen we uw bestand niet verwerken. Weet u zeker dat u het juiste bestand heeft gekozen? Ga dan verder. Probeer opnieuw als u een ander bestand wilt kiezen.",
        }
    )
    ok = props.Translatable(
        {
            "en": "Try again",
            "de": "Versuchen Sie es noch einmal",
            "nl": "Probeer opnieuw",
        }
    )
    cancel = props.Translatable({"en": "Continue", "de": "Weiter", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def prompt_file(extensions):
    description = props.Translatable(
        {
            "en": "Please select the zip file that you downloaded with your Google search history.",
            "de": "Wählen Sie bitte die heruntergeladene ZIP Datei aus mit Ihren Google Suchverlauf.",
            "nl": "Selecteer een willekeurige zip file die u heeft opgeslagen op uw apparaat.",
        }
    )

    return props.PropsUIPromptFileInput(description, extensions)


def prompt_extraction_message(message, percentage):
    description = props.Translatable(
        {
            "en": "One moment please. Information is now being extracted from the selected file.",
            "de": "Einen Moment bitte. Es werden nun Informationen aus der ausgewählten Datei extrahiert.",
            "nl": "Een moment geduld. Informatie wordt op dit moment uit het geselecteerde bestaand gehaald.",
        }
    )

    return props.PropsUIPromptProgress(description, message, percentage)


def get_zipfile(filename):
    try:
        return zipfile.ZipFile(filename)
    except zipfile.error:
        return "invalid"


def get_files(zipfile_ref):
    try:
        return zipfile_ref.namelist()
    except zipfile.error:
        return []


def extract_file(zipfile_ref, filename):
    try:
        # make it slow for demo reasons only
        info = zipfile_ref.getinfo(filename)
        return (filename, info.compress_size, info.file_size)
    except zipfile.error:
        return "invalid"


def prompt_consent(data, meta_data):
    search_title = props.Translatable(
        {
            "en": "Search terms",
            "de": "Suchbegriffe",
            "nl": "Zoektermen",
        }
    )

    clicks_title = props.Translatable(
        {
            "en": "Clicked search results",
            "de": "Angeklickte Suchergebnisse",
            "nl": "Aangeklikte zoekresultaten",
        }
    )

    log_title = props.Translatable(
        {"en": "Log messages", "de": "Log Nachrichten", "nl": "Log berichten"}
    )

    tables = []
    if isinstance(data, tuple) and len(data) == 2:
        searches_df, clicks_df = data
        tables = [
            props.PropsUIPromptConsentFormTable("searches", search_title, searches_df),
            props.PropsUIPromptConsentFormTable("clicks", clicks_title, clicks_df),
        ]

    meta_frame = pd.DataFrame(meta_data, columns=["type", "message"])
    meta_table = props.PropsUIPromptConsentFormTable(
        "log_messages", log_title, meta_frame
    )
    return props.PropsUIPromptConsentForm(tables, [meta_table])


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)


def exit(code, info):
    return CommandSystemExit(code, info)


def is_google_search_url(url):
    """
    Determine if a URL is a Google search URL.
    Accepts any Google TLD (e.g., google.com, google.de, google.nl)

    Args:
        url: URL string to check

    Returns:
        tuple: (is_search, query) where is_search is boolean and query is the search term or None
    """
    try:
        parsed_url = urlparse(url)
        # Check if domain is a Google domain (e.g., www.google.com, www.google.de)
        if parsed_url.netloc.startswith("www.google.") and parsed_url.path == "/search":
            query_params = parse_qs(parsed_url.query)
            if "q" in query_params:
                return True, query_params["q"][0]
    except Exception:
        pass
    return False, None


def extract_search_data(data):
    if not isinstance(data, list):
        return pd.DataFrame(columns=["Datum", "Nummer", "Suchbegriff"]), pd.DataFrame(
            columns=["Datum", "Nummer", "Suchergebnis", "Link"]
        )

    searches = []
    clicks = []

    def resolve_google_redirect(url):
        parsed = urlparse(url)
        if parsed.netloc.startswith("www.google.") and parsed.path == "/url":
            query = parse_qs(parsed.query)
            if "q" in query:
                return query["q"][0]
        return url

    def format_title(title):
        try:
            parsed = urlparse(title)
            # Check if it looks like a URL - must have netloc (domain) and valid scheme
            if parsed.netloc and parsed.scheme in ("http", "https"):
                # Remove 'www.' prefix if it exists
                return parsed.netloc.replace("www.", "", 1)
            return title
        except Exception:
            return title

    records = []
    for item in data:
        if not all(
            key in item for key in ["header", "title", "titleUrl", "time", "products"]
        ):
            continue

        timestamp = pd.to_datetime(item["time"])
        if not (start_dt <= timestamp < end_dt):  # Changed <= to < for end_dt
            continue

        title = item["title"]
        if title.startswith("Visited "):
            item["title"] = title[len("Visited ") :]
        elif title.startswith("Viewed ") or title.endswith(" angesehen"):
            continue  # Skip Viewed items entirely

        records.append(item)

    for i, item in enumerate(records, start=1):
        timestamp = pd.to_datetime(item["time"])
        date = timestamp.strftime("%d-%m-%Y")
        index = str(i)

        final_url = resolve_google_redirect(item["titleUrl"])
        is_search, query = is_google_search_url(final_url)
        if is_search:
            searches.append({"Datum": date, "Nummer": index, "Suchbegriff": query})
        else:
            clicks.append(
                {
                    "Datum": date,
                    "Nummer": index,
                    "Suchergebnis": format_title(item["title"]),
                    "Link": final_url,
                }
            )

    searches_df = pd.DataFrame(searches)
    clicks_df = pd.DataFrame(clicks)

    # Ensure columns exist even if dataframes are empty
    searches_df = searches_df.reindex(columns=["Datum", "Nummer", "Suchbegriff"])
    clicks_df = clicks_df.reindex(columns=["Datum", "Nummer", "Suchergebnis", "Link"])

    return searches_df, clicks_df


class GoogleTakeoutNotFoundError(Exception):
    """Raised when no valid Google Takeout data is found in the zip file"""

    pass


class NoGoogleSearchDataError(Exception):
    """Raised when a Google Takeout archive is found but contains no search data"""

    pass


def find_google_search_export(zipfile_ref):
    """
    Find and validate Google Search export JSON files in a zip archive.
    First checks if it's a Google Takeout archive by looking for the marker HTML file.
    Then validates the JSON structure.

    Args:
        zipfile_ref: ZipFile object to search through

    Returns:
        list: The parsed JSON data if found

    Raises:
        GoogleTakeoutNotFoundError: If no valid Google Takeout data is found
        NoGoogleSearchDataError: If valid Google Takeout is found but contains no search data
        Exception: For other errors during processing
    """

    # Get all JSON files from the zip
    json_files = [f for f in zipfile_ref.namelist() if f.lower().endswith(".json")]

    for file in json_files:
        try:
            with zipfile_ref.open(file) as f:
                # Check if it's valid JSON array with required structure
                data = json.load(f)
                if isinstance(data, list) and len(data) > 0:
                    first_item = data[0]
                    # Check if the JSON structure matches Google Takeout format
                    required_fields = {
                        "header",
                        "title",
                        "time",
                        "products",
                        "titleUrl",
                    }
                    if (
                        required_fields.issubset(first_item.keys())
                        and isinstance(first_item["products"], list)
                        and any(
                            product in ["Search", "Google Suche"]
                            for product in first_item["products"]
                        )
                    ):
                        return data

        except (json.JSONDecodeError, KeyError, AttributeError):
            continue

    # First check if this is a Google Takeout archive by looking for marker HTML
    html_files = [f for f in zipfile_ref.namelist() if f.lower().endswith(".html")]

    for f in zipfile_ref.namelist():
        print(f)

    for html_file in html_files:
        with zipfile_ref.open(html_file) as f:
            try:
                content = f.read().decode("utf-8")
            except Exception:
                continue
            if "Google" in content:
                print("Found Google Takeout archive")
                raise NoGoogleSearchDataError()

    raise GoogleTakeoutNotFoundError("No valid Google Takeout data found in zip file")
