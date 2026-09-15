"""실험들이 함께 쓰는 것 — 고정 입력과 정답표, 그리고 프롬프트 조립.

왜 고정 입력인가: 후보가 매번 바뀌면 변형이 아니라 노이즈를 비교하게 된다.
한 번 떠 둔 21건을 모든 변형이 똑같이 본다.
"""
from __future__ import annotations

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

try:                                    # 로컬 실행용 — 키가 없으면 그냥 넘어간다
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

HERE = pathlib.Path(__file__).resolve().parent
RESULTS = HERE / "results"

# 고정 입력 — 2026-09-15 실행에서 예선을 통과한 21건을 그대로 떠 둔 것
SHORTLIST: list[dict] = json.loads((HERE / "fixed_shortlist.json").read_text(encoding="utf-8"))

# ── 손으로 붙인 정답표 ────────────────────────────────────
# 같은 사건을 다룬 쌍. 매체가 달라도 하나만 실려야 한다.
DUP_PAIRS = [(5, 6), (10, 11)]          # 성난사람들2 에미상 · 문학동네 매각
# audience.yaml 의 버릴_것 을 정면으로 어기는 것. 보수적으로 명백한 것만 표시했다.
# 7번은 '시상식 수상자 명단', 5·6 은 '시상식 수상' — 모두 버릴_것 4항.
BAD = {5, 6, 7}


def numbered(items: list[dict]) -> str:
    return "\n".join(f"{i}. ({c['source']}) {c['title']}" for i, c in enumerate(items))


def save(name: str, rows: list[dict]) -> pathlib.Path:
    RESULTS.mkdir(exist_ok=True)
    p = RESULTS / f"{name}.json"
    p.write_text(json.dumps(rows, ensure_ascii=False, indent=1), encoding="utf-8")
    return p
