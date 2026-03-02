import ast
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "ASP FF Dashboard.py"

with MODULE_PATH.open("r", encoding="utf-8") as fp:
    source = fp.read()

module_ast = ast.parse(source, filename=str(MODULE_PATH))

needed_assignments = {
    "SUBJ_TAIL_RE",
    "SUBJ_CALLSIGN_RE",
    "SUBJ_PATTERNS",
    "SUBJ_DIVERSION_FROM_PAREN_RE",
    "SUBJ_DIVERSION_FROM_TOKEN_RE",
}

snippets: list[str] = []
for node in module_ast.body:
    if isinstance(node, ast.Assign):
        target_names = {
            tgt.id for tgt in node.targets if isinstance(tgt, ast.Name)
        }
        if target_names & needed_assignments:
            seg = ast.get_source_segment(source, node)
            if seg:
                snippets.append(seg)
    if isinstance(node, ast.FunctionDef) and node.name == "parse_subject_line":
        seg = ast.get_source_segment(source, node)
        if seg:
            snippets.append(seg)

namespace = {
    "re": re,
    "datetime": datetime,
    "timedelta": timedelta,
}
exec("\n\n".join(snippets), namespace)

parse_subject_line = namespace["parse_subject_line"]


def test_parse_subject_line_detects_edct_route_tokens():
    now_utc = datetime(2026, 2, 24, 15, 0, tzinfo=timezone.utc)
    info = parse_subject_line("KTEB to KHPN (ASP473) EDCT Update", now_utc)

    assert info["event_type"] == "EDCT"
    assert info["from_airport"] == "KTEB"
    assert info["to_airport"] == "KHPN"
