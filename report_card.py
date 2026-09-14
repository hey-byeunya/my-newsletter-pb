"""
report_card.py — 섹션 12: 쌓인 기록으로 소스 성적표 만들기

새 도구가 필요하지 않다. store/metrics.jsonl 의 by_source 를 더하기만 하면 된다.
    python report_card.py
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
METRICS = ROOT / "store" / "metrics.jsonl"


def rows() -> list[dict]:
    if not METRICS.exists():
        return []
    out = []
    for line in METRICS.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            out.append(json.loads(line))
    return out


def main() -> None:
    data = rows()
    if not data:
        print(f"기록이 없습니다: {METRICS}\n먼저 run.py 를 한 번 돌리세요.")
        return

    # ── 소스별 발행 기여 ─────────────────────────────────────
    total: dict[str, int] = {}
    for r in data:
        for src, n in (r.get("by_source") or {}).items():
            total[src] = total.get(src, 0) + n

    # 수집은 되는데 한 번도 발행까지 못 올라간 소스를 보이게 한다
    try:
        import yaml
        cfg = yaml.safe_load((ROOT / "settings.yaml").read_text(encoding="utf-8"))
        for s in cfg.get("sources", []):
            total.setdefault(s["name"], 0)
    except Exception:
        pass

    print(f"── 소스별 발행 기여 ({len(data)}회 실행 누적) ──")
    width = max((len(k) for k in total), default=10)
    for src, n in sorted(total.items(), key=lambda kv: -kv[1]):
        mark = "  ← 몇 주째 0이면 목록에서 뺀다" if n == 0 else ""
        print(f"  {src:<{width}}  {n:>3}건  {'█' * n}{mark}")

    # ── 깔때기 ──────────────────────────────────────────────
    print("\n── 깔때기 (최근 7회) ──")
    print(f"  {'run_id':<20} {'수집':>5} {'선별':>5} {'취재':>5} {'발행':>5}   통과율")
    for r in data[-7:]:
        c, p = r.get("collected", 0), r.get("picked", 0)
        d, pub = r.get("drafted", 0), r.get("published", 0)
        # 절대 개수는 출렁이지만 비율은 안정적이다 — 비율이 튀는 날을 본다
        rate = f"{p / c:.0%}" if c else "—"
        print(f"  {r.get('run_id', ''):<20} {c:>5} {p:>5} {d:>5} {pub:>5}   예선·본선 {rate}")

    # ── 발행 건수의 분포 ────────────────────────────────────
    dist: dict[int, int] = {}
    for r in data:
        dist[r.get("published", 0)] = dist.get(r.get("published", 0), 0) + 1
    print("\n── 발행 건수 분포 ──")
    for n in sorted(dist):
        note = "  ← 0건인 날도 함께 센다" if n == 0 else ""
        print(f"  {n}건 발행: {dist[n]}회{note}")


if __name__ == "__main__":
    main()
