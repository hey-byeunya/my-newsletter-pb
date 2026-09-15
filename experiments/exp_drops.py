"""3-6절 — 탈락 사유를 받아내는 법.

모델이 스스로 떨어뜨린 목록(Shortlist.drops)이 사유 없이 제목만 돌아왔다.
설명을 조이는 것과 스키마로 강제하는 것 중 무엇이 듣는가.

  A 현행      list[str], 설명만 "기사와 그 이유"
  B 설명 강화  list[str], 형식을 못박음  「제목」 — 이유
  C 스키마 강제 list[Drop], reason 을 필수 칸으로

  ./.venv/bin/python experiments/exp_drops.py --reps 6
"""
from __future__ import annotations

import argparse
import json
import statistics as st
import time

from _common import SHORTLIST as SL, numbered, save
from pydantic import BaseModel, Field

from graph import CRITERIA, OVERSELECT, QUOTA, TOPIC_GROUP, _parse

K = OVERSELECT * 2 if QUOTA else OVERSELECT
TITLES = [c["title"] for c in SL]


class Pick(BaseModel):
    index: int = Field(description="후보 목록에서 고른 기사의 번호")
    event: str = Field(description="작품·전시·책 하나를 식별하는 고유한 이름(제목·전시명). "
                                   "장르나 분야가 아니다. 같은 대상을 다룬 기사끼리만 같은 라벨을 쓸 것")
    topic: str = Field(description=f"반드시 다음 중 하나: {' | '.join(TOPIC_GROUP)}",
                       json_schema_extra={"enum": list(TOPIC_GROUP)})
    why:   str = Field(description="고른 이유 한 줄")


class Drop(BaseModel):
    index:  int = Field(description="떨어뜨린 기사의 번호")
    reason: str = Field(description="떨어뜨린 이유. 어느 기준에 걸렸는지 한 줄로")


class ShortA(BaseModel):
    picks: list[Pick]
    drops: list[str] = Field(default_factory=list, description="눈에 띄게 떨어뜨린 기사와 그 이유")


class ShortB(BaseModel):
    picks: list[Pick]
    drops: list[str] = Field(default_factory=list,
        description="눈에 띄게 떨어뜨린 기사. 반드시 「제목」 — 이유 형식으로 쓸 것. "
                    "제목만 적지 말 것. 이유에는 어느 기준에 걸렸는지를 쓸 것")


class ShortC(BaseModel):
    picks: list[Pick]
    drops: list[Drop] = Field(default_factory=list, description="눈에 띄게 떨어뜨린 기사와 그 이유")


VARIANTS = {"A 현행": ShortA, "B 설명 강화": ShortB, "C 스키마 강제": ShortC}


def has_reason(d, variant: str) -> bool:
    """사유가 실제로 담겼는가.

    ⚠️ A·B 는 문자열이라 제목을 걷어낸 나머지로 판정할 수밖에 없는데, 이 방식은
    '(경향 문화)' 같은 출처 접두어를 사유로 오인한다 — 실제로 첫 집계에서 A 를
    과대평가했다. 그래서 숫자만 믿지 말고 원문도 함께 봐야 한다(--show).
    """
    if variant == "C 스키마 강제":
        r = (d.reason or "").strip()
        return len(r) >= 8 and not any(r in t or t in r for t in TITLES if len(t) > 10)
    s = (d or "").strip()
    body = s
    for t in TITLES:
        if t[:24] in s:
            body = s.replace(t, "")
            break
    return len(body.strip(" -—:·()0123456789.")) >= 8


def run(variant: str, schema, rep: int) -> dict:
    sysm = (f"{CRITERIA}\n\n당신은 위 기준으로 기사를 고르는 데스크입니다.\n"
            f"아래 후보 중 최대 {K}건을 중요한 순서대로 고르세요.\n"
            "같은 사건을 다룬 기사는 하나만 고르고, 같은 사건에는 같은 event 라벨을 쓰세요.")
    t0 = time.time()
    out = _parse([{"role": "system", "content": sysm},
                  {"role": "user", "content": f"[본선] 후보 {len(SL)}건\n\n{numbered(SL)}"}], schema)
    sec = round(time.time() - t0, 1)
    picks = [p for p in out.picks if 0 <= p.index < len(SL)][:K]
    drops = out.drops or []
    return {"variant": variant, "rep": rep, "sec": sec, "picks": len(picks),
            # 고치려던 것
            "drops": len(drops),
            "drops_with_reason": sum(1 for d in drops if has_reason(d, variant)),
            # 함께 망가질 수 있는 것 — 이걸 안 재면 검수 프롬프트 때와 같은 실수를 한다
            "bad_index": len(out.picks) - len(picks),
            "labels": len({(p.event or "").strip().lower() for p in picks}),
            "topics_ok": sum(1 for p in picks if p.topic in TOPIC_GROUP),
            "why_ok": sum(1 for p in picks if len((p.why or "").strip()) >= 8)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=6)
    ap.add_argument("--show", action="store_true", help="drops 원문을 함께 찍는다")
    a = ap.parse_args()

    rows = []
    for name, schema in VARIANTS.items():
        for rep in range(1, a.reps + 1):
            r = run(name, schema, rep)
            rows.append(r)
            print("  ", json.dumps(r, ensure_ascii=False), flush=True)
    print(f"\n기록: {save('exp_drops', rows)}")

    print("\n── 요약 ──")
    print(f"  {'변형':<14} {'n':>3} {'drops':>6} {'사유 포함':>9} {'비율':>6} "
          f"{'범위밖':>6} {'토픽위반':>8} {'초':>5}")
    for name in VARIANTS:
        g = [r for r in rows if r["variant"] == name]
        d = sum(r["drops"] for r in g)
        ok = sum(r["drops_with_reason"] for r in g)
        print(f"  {name:<14} {len(g):>3} {d:>6} {ok:>9} {(ok / d if d else 0):>5.0%} "
              f"{sum(r['bad_index'] for r in g):>6} "
              f"{sum(r['picks'] - r['topics_ok'] for r in g):>8} "
              f"{st.mean(r['sec'] for r in g):>5.1f}")


if __name__ == "__main__":
    main()
