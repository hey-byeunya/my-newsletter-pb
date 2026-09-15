# source_audit.py — 소스 채택표의 '수치적 근거' 를 만드는 계측 스크립트.
#
# 언어 모델을 부르지 않는다. 같은 날 돌리면 같은 답이 나오는 관문 검사다.
# graph.py 의 수집기를 그대로 불러 쓴다 — 여기서 잰 값과 실제 파이프라인이
# 보는 값이 달라지면 표가 거짓말이 되기 때문이다.
#
#   ./.venv/bin/python source_audit.py                 # 기본 7일, 소스당 본문 3건 표본
#   ./.venv/bin/python source_audit.py --hours 168 --sample 5
#   ./.venv/bin/python source_audit.py --md            # REPORT.md 에 붙일 표로 출력
import argparse
import json
import pathlib
import sys
import time
from datetime import datetime, timedelta, timezone

# run.py 와 같은 규칙으로 .env 를 읽는다 — 키가 필요한 소스(KCISA)를
# '접근 불가' 로 잘못 기록하지 않기 위해서다.
try:
    from dotenv import load_dotenv
    load_dotenv(pathlib.Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

from graph import (SET, SOURCES, ROOT, extract_body, fetch_hn, fetch_kcisa,
                   fetch_rss, published_at, _shift)

AUDIT = ROOT / "store" / "source_audit.json"


def measure(src, hours, sample):
    """소스 하나를 관문 세 개로 잰다 — G3 접근 · G2 생존 · G1 본문."""
    row = {"name": src.name, "url": src.url, "kind": src.kind,
           "local_only": bool(getattr(src, "local_only", False)),
           "match": src.match, "error": None,
           "entries": 0, "in_window": 0,
           "body_tried": 0, "body_ok": 0, "body_avg": 0, "body_min": 0, "body_max": 0,
           "elapsed": 0.0}

    started = time.time()
    try:
        raw = ({"hn": fetch_hn, "kcisa": fetch_kcisa}.get(src.kind, fetch_rss))(src)
    except Exception as exc:                       # G3 — 접근 관문 탈락
        row["error"] = (str(exc).strip() or type(exc).__name__)[:160]
        row["elapsed"] = round(time.time() - started, 1)
        return row
    row["elapsed"] = round(time.time() - started, 1)
    row["entries"] = len(raw)

    # G2 — 생존 관문. 시간 창 안에 몇 건이 살아 있는가.
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    fresh = []
    for it in raw:
        at = it.get("at")
        if at is None or at >= cutoff:             # 날짜를 못 읽은 것은 살려 둔다(수집과 동일)
            fresh.append(it)
    row["in_window"] = len(fresh)

    # G1 — 본문 관문. 실제로 읽어서 min_body 를 넘는가. 앞에서 몇 건만 표본으로.
    lens = []
    for it in fresh[:sample]:
        row["body_tried"] += 1
        try:
            n = len(extract_body(it["url"]) or it.get("body_hint", ""))
        except Exception:
            n = 0
        lens.append(n)
        if n >= SET.min_body:
            row["body_ok"] += 1
    if lens:
        row["body_avg"] = round(sum(lens) / len(lens))
        row["body_min"], row["body_max"] = min(lens), max(lens)
    return row


def verdict(r):
    """관문 통과 여부를 한 단어로. 표에서 '왜 남겼는가' 가 보여야 한다."""
    if r["error"]:
        return "G3 탈락 — 접근 불가"
    if r["in_window"] == 0:
        return "G2 탈락 — 창 안에 0건"
    if r["body_tried"] and r["body_ok"] == 0:
        return f"G1 탈락 — 본문 {r['body_avg']}자"
    return "채택"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=SET.hours)
    ap.add_argument("--sample", type=int, default=3)
    ap.add_argument("--md", action="store_true", help="마크다운 표로 출력")
    a = ap.parse_args()

    print(f"# 소스 실측 — 창 {a.hours}h · 본문 표본 소스당 {a.sample}건 · "
          f"본문 관문 {SET.min_body}자\n", file=sys.stderr)

    rows = []
    for src in SOURCES:
        print(f"  … {src.name}", file=sys.stderr, flush=True)
        r = measure(src, a.hours, a.sample)
        r["verdict"] = verdict(r)
        rows.append(r)

    stamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    AUDIT.parent.mkdir(exist_ok=True)
    AUDIT.write_text(json.dumps({"at": stamp, "hours": a.hours, "sample": a.sample,
                                 "min_body": SET.min_body, "rows": rows},
                                ensure_ascii=False, indent=2), encoding="utf-8")

    if a.md:
        print(f"<!-- source_audit.py · {stamp} · 창 {a.hours}h · 본문 관문 {SET.min_body}자 -->\n")
        print("| 소스 | 종류 | 전체 | 창 안 | 본문 평균 | 본문 통과 | 소요 | 판정 |")
        print("|---|---|--:|--:|--:|--:|--:|---|")
        for r in rows:
            body = f"{r['body_avg']}자" if r["body_tried"] else "—"
            ok = f"{r['body_ok']}/{r['body_tried']}" if r["body_tried"] else "—"
            note = f" <br>`{r['error'][:70]}`" if r["error"] else ""
            print(f"| {r['name']} | {r['kind']} | {r['entries']} | {r['in_window']} "
                  f"| {body} | {ok} | {r['elapsed']}초 | {r['verdict']}{note} |")
    else:
        print(f"\n── 소스 실측 ({stamp}) ──")
        for r in rows:
            body = (f"본문 {r['body_avg']}자({r['body_min']}~{r['body_max']}) "
                    f"{r['body_ok']}/{r['body_tried']}" if r["body_tried"] else "본문 —")
            print(f"  {r['name']:<16} 전체 {r['entries']:>4}  창안 {r['in_window']:>3}  "
                  f"{body:<28} {r['elapsed']:>5}초  {r['verdict']}")
            if r["error"]:
                print(f"    └ {r['error'][:110]}")
    print(f"\n기록: {AUDIT}", file=sys.stderr)


if __name__ == "__main__":
    main()
