"""3-7절 전반 — 본선에 몇 건을 요청할 것인가.

어느 회차에서 모델이 에미상 기사 3건을 채택했다(버릴_것 1순위). 본선에
OVERSELECT × 2 = 16건을 요청하니 자리를 채우려 바닥을 긁는다고 의심했다.

결론: 가설이 틀렸다. K 를 줄여도 위반은 안 줄고 쿼터 충족만 나빠진다.
      그래서 graph.py 는 바꾸지 않았다.

  ./.venv/bin/python experiments/exp_k.py --reps 5 --ks 16 12 10 8
"""
from __future__ import annotations

import argparse
import json
import statistics as st
import time

from _common import BAD, DUP_PAIRS, SHORTLIST as SL, numbered, save

from graph import CRITERIA, OVERSELECT, QUOTA, Shortlist, TOPIC_GROUP, _parse


def assign(picks) -> list[dict]:
    """graph.py select() 의 중복 제거 + 쿼터 배분을 그대로 옮긴 것.

    ⚠️ 원본과 갈라질 수 있다. select() 를 고치면 여기도 함께 고쳐야 한다.
    (여기서는 근접 중복 검사 이전 상태를 재고 있다 — 실험 당시의 코드다.)
    """
    picked, seen, overflow = [], set(), []
    filled = {g: 0 for g in QUOTA}
    for p in picks:
        ev = (p.event or SL[p.index]["title"]).strip().lower()
        if ev in seen:
            continue
        seen.add(ev)
        g = TOPIC_GROUP.get(p.topic)
        rec = {"i": p.index, "g": g or "미분류"}
        if g and filled[g] < QUOTA[g] and len(picked) < OVERSELECT:
            filled[g] += 1
            picked.append(rec)
        else:
            overflow.append(rec)
    for rec in overflow:
        if len(picked) >= OVERSELECT:
            continue
        picked.append(rec)
    return picked


def run(K: int, rep: int) -> dict:
    sysm = (f"{CRITERIA}\n\n당신은 위 기준으로 기사를 고르는 데스크입니다.\n"
            f"아래 후보 중 최대 {K}건을 중요한 순서대로 고르세요.\n"
            "같은 사건을 다룬 기사는 하나만 고르고, 같은 사건에는 같은 event 라벨을 쓰세요.")
    t0 = time.time()
    out = _parse([{"role": "system", "content": sysm},
                  {"role": "user", "content": f"[본선] 후보 {len(SL)}건\n\n{numbered(SL)}"}], Shortlist)
    sec = round(time.time() - t0, 1)
    picks = [p for p in out.picks if 0 <= p.index < len(SL)][:K]
    final = assign(picks)
    idx = [r["i"] for r in final]
    groups: dict[str, int] = {}
    for r in final:
        groups[r["g"]] = groups.get(r["g"], 0) + 1
    lab = {p.index: (p.event or "").strip().lower() for p in picks}
    both = [(a, b) for a, b in DUP_PAIRS if a in lab and b in lab]
    return {"K": K, "rep": rep, "sec": sec,
            "asked": len(out.picks), "after_quota": len(final),
            # 고치려던 것 — 버릴_것 위반이 발행 후보까지 올라오는가
            "위반_요청": len([p for p in picks if p.index in BAD]),
            "위반_발행후보": len([i for i in idx if i in BAD]),
            # 함께 망가질 수 있는 것 — 쿼터를 못 채우게 되는가
            "문화": groups.get("문화", 0), "공간": groups.get("공간", 0),
            "쿼터충족": groups.get("문화", 0) >= QUOTA["문화"] and groups.get("공간", 0) >= QUOTA["공간"],
            "같은사건쌍_동일라벨": sum(1 for a, b in both if lab[a] == lab[b]),
            "같은사건쌍_기회": len(both)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--ks", type=int, nargs="+", default=[16, 12, 10, 8])
    a = ap.parse_args()

    rows = []
    for K in a.ks:
        for rep in range(1, a.reps + 1):
            r = run(K, rep)
            rows.append(r)
            print("  ", json.dumps(r, ensure_ascii=False), flush=True)
    print(f"\n기록: {save('exp_k', rows)}")

    print("\n── 요약 ──")
    print(f"  {'K':>3} {'n':>3} {'쿼터충족':>10} {'문화 평균':>9} {'위반(발행후보)':>14} {'초':>5}")
    for K in a.ks:
        g = [r for r in rows if r["K"] == K]
        q = sum(r["쿼터충족"] for r in g)
        print(f"  {K:>3} {len(g):>3} {q}/{len(g)} ({q / len(g):>4.0%}) "
              f"{st.mean(r['문화'] for r in g):>9.2f} "
              f"{st.mean(r['위반_발행후보'] for r in g):>14.2f} "
              f"{st.mean(r['sec'] for r in g):>5.1f}")


if __name__ == "__main__":          # ← 이게 없어서 import 할 때마다 다시 돌았다.
    main()                          #   중간 요약의 n 이 어긋나 반대 결론을 낼 뻔했다.
