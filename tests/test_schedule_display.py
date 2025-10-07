import ast
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[1] / "ASP FF Dashboard.py"


with MODULE_PATH.open("r", encoding="utf-8") as fp:
    MODULE_SOURCE = fp.read()

MODULE_AST = ast.parse(MODULE_SOURCE, filename=str(MODULE_PATH))

_TARGET_NAME = "display_airport"
_TARGET_SOURCE = None

for node in MODULE_AST.body:
    if isinstance(node, ast.FunctionDef) and node.name == _TARGET_NAME:
        _TARGET_SOURCE = ast.get_source_segment(MODULE_SOURCE, node)
        break

if _TARGET_SOURCE is None:  # pragma: no cover - safety guard for refactors
    raise RuntimeError(f"{_TARGET_NAME} not found in dashboard module")


namespace: dict[str, object] = {}
exec(_TARGET_SOURCE, namespace)

display_airport = namespace[_TARGET_NAME]


def test_display_airport_prefers_icao_token_when_available():
    assert display_airport("CYEG", "YEG") == "CYEG"


def test_display_airport_preserves_non_icao_identifiers():
    # Some airports only supply a three-character identifier (e.g. IATA).
    assert display_airport("YJP", "") == "YJP"


def test_display_airport_falls_back_to_iata_when_icao_missing():
    assert display_airport("", "YJP") == "YJP"


def test_display_airport_returns_placeholder_when_no_tokens():
    assert display_airport("", "") == "—"
