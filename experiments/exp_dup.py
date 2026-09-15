"""3-7절 후반 — 같은 사건이 서로 다른 매체로 들어올 때 묶이는가.

event 라벨만 믿으면 (5,6) 성난사람들2 에미상과 (10,11) 문학동네 매각이
매체별로 다른 라벨을 받아 둘 다 통과한다. 반대로 한 회차에서는 12건 전부에
같은 라벨이 붙어 11건이 버려질 뻔했다.

  A 현행       라벨만 믿는다
  B 라벨 제약   라벨 설명을 조인다
  C 코드 보강   라벨은 그대로 두고 제목의 희귀 낱말로 한 번 더 본다 (graph.py 채택)

  ./.venv/bin/python experiments/exp_dup.py --reps 5
  ./.venv/bin/python experiments/exp_dup.py --detector-only   # LLM 없이 검출기만 전수 평가
"""
from __future__ import annotations

import argparse
import itertools
import json
import time

from _common import DUP_PAIRS, SHORTLIST as SL, numbered, save
from pydantic import BaseModel, Field

from graph import (CRITERIA, OVERSELECT, QUOTA, TOPIC_GROUP, Drop, _doc_freq,
                   _near_dup, _parse, _title_tokens)

K = OVERSELECT * 2 if QUOTA else OVERSELECT
TRUE = set(DUP_PAIRS)

A_DESC = ("작품·전시·책 하나를 식별하는 고유한 이름(제목·전시명). "
          "장르나 분야가 아니다. 같은 대상을 다룬 기사끼리만 같은 라벨을 쓸 것")
B_DESC = ("작품·전시·책 하나를 식별하는 짧은 고유명. 2~5 낱말로 쓰고 기사 제목을 그대로 옮기지 말 것. "
          "장르나 분야가 아니다. 다른 매체가 같은 대상을 다뤘다면 반드시 같은 라벨이 나와야 한다 "
          "— 매체마다 다르게 쓴 수식어는 빼고 대상의 이름만 남길 것")


def mk(desc):
    class P(BaseModel):
        index: int = Field(description="후보 목록에서 고른 기사의 번호")
        event: str = Field(description=desc)
        topic: str = Field(description=f"반드시 다음 중 하나: {' | '.join(TOPIC_GROUP)}",
                           json_schema_extra={"enum": list(TOPIC_GROUP)})
        why:   str = Field(description="고른 이유 한 줄")

    class S(BaseModel):
        picks: list[P]
        drops: list[Drop] = Field(default_factory=list, description="눈에 띄게 떨어뜨린 기사와 그 이유")
    return S


def detector_only() -> None:
    """LLM 없이 검출기만 전수 평가한다 — 210개 쌍 전부. 임계값은 이렇게 정했다."""
    df = _doc_freq(SL)
    tk = [_title_tokens(c["title"]) for c in SL]
    hit = [(i, j) for i, j in itertools.combinations(range(len(SL)), 2)
           if _near_dup(tk[i], tk[j], df)]
    tp = [p for p in hit if p in TRUE]
    print(f"  쌍 {len(list(itertools.combinations(range(len(SL)), 2)))}개 전수 평가")
    print(f"  검출 {len(hit)}쌍 · 정답 {len(tp)}/{len(TRUE)} · 오검출 {len(hit) - len(tp)}")
    for i, j in hit:
        print(f"    {'★' if (i, j) in TRUE else ' '} ({i},{j}) {SL[i]['title'][:30]} ∥ {SL[j]['title'][:30]}")


def run(variant: str, S, use_code: bool, rep: int) -> dict:
    sysm = (f"{CRITERIA}\n\n당신은 위 기준으로 기사를 고르는 데스크입니다.\n"
            f"아래 후보 중 최대 {K}건을 중요한 순서대로 고르세요.\n"
            "같은 사건을 다룬 기사는 하나만 고르고, 같은 사건에는 같은 event 라벨을 쓰세요.")
    t0 = time.time()
    out = _parse([{"role": "system", "content": sysm},
                  {"role": "user", "content": f"[본선] 후보 {len(SL)}건\n\n{numbered(SL)}"}], S)
    sec = round(time.time() - t0, 1)
    picks = [p for p in out.picks if 0 <= p.index < len(SL)][:K]
    lab = {p.index: (p.event or "").strip().lower() for p in picks}
    df = _doc_freq(SL)
    tk = {i: _title_tokens(SL[i]["title"]) for i in lab}
    merged = {(i, j) for i, j in itertools.combinations(sorted(lab), 2)
              if lab[i] == lab[j] or (use_code and _near_dup(tk[i], tk[j], df))}
    both = {p for p in TRUE if p[0] in lab and p[1] in lab}
    return {"variant": variant, "rep": rep, "sec": sec, "picks": len(picks),
            "정답쌍_기회": len(both), "정답쌍_병합": len(both & merged),
            "오병합": len(merged - TRUE),
            "제목그대로": sum(1 for i in lab if lab[i] == SL[i]["title"].strip().lower())}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--detector-only", action="store_true")
    a = ap.parse_args()
    if a.detector_only:
        detector_only()
        return

    V = [("A 현행", mk(A_DESC), False), ("B 라벨 제약", mk(B_DESC), False),
         ("C 코드 보강", mk(A_DESC), True)]
    rows = []
    for name, S, code in V:
        for rep in range(1, a.reps + 1):
            r = run(name, S, code, rep)
            rows.append(r)
            print("  ", json.dumps(r, ensure_ascii=False), flush=True)
    print(f"\n기록: {save('exp_dup', rows)}")

    print("\n── 요약 ──")
    print(f"  {'변형':<12} {'정답쌍 기회':>11} {'병합':>5} {'병합률':>7} {'오병합':>7} {'제목그대로':>10}")
    for name, _, _ in V:
        g = [r for r in rows if r["variant"] == name]
        opp = sum(r["정답쌍_기회"] for r in g)
        mg = sum(r["정답쌍_병합"] for r in g)
        print(f"  {name:<12} {opp:>11} {mg:>5} {(mg / opp if opp else 0):>6.0%} "
              f"{sum(r['오병합'] for r in g):>7} {sum(r['제목그대로'] for r in g):>10}")


if __name__ == "__main__":
    main()
