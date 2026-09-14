"""
graph.py — 뉴스레터 에이전트 (수집 → 선별 → 취재 → 검수 → 발행)

강의 섹션과의 대응
  섹션 2  : Brief(State) · build() · INIT · run()
  섹션 5  : SOURCES · strip_tags · published_at · collect
  섹션 7  : Pick · Shortlist · CRITERIA · ask_picks · select
  섹션 8  : Draft · extract_body · draft · fan_report · report
  섹션 9  : Verdict · check · verify
  섹션 10 : build_embeds · make_lead · send · publish
  섹션 12 : run() — store/metrics.jsonl 에 한 줄 남기기
  섹션 13 : audience.yaml · settings.yaml 을 읽어 프롬프트를 조립

이 파일은 불러오기(import)만으로는 아무 일도 하지 않는다. (섹션 11)
"""
from __future__ import annotations

import calendar
import html
import json
import operator
import os
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, TypedDict

import feedparser
import requests
import trafilatura
import yaml
from langgraph.graph import END, START, StateGraph
from pydantic import BaseModel, Field

try:                                    # langgraph 버전에 따라 위치가 다르다
    from langgraph.types import Send
except ImportError:                     # pragma: no cover
    from langgraph.constants import Send


# ─────────────────────────────────────────────────────────────
# 섹션 13 — 설정과 코드의 경계
#   분야가 바뀌면 바뀌는 값(독자·기준·토픽·소스)은 전부 YAML 에 있다.
#   이 파일에는 '절차'만 남긴다.
# ─────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent


def _load(name: str) -> dict:
    path = ROOT / name
    if not path.exists():
        # 설정을 못 읽은 채 기본값으로 도는 것이 가장 나쁜 실패다 — 여기서 멈춘다.
        raise FileNotFoundError(f"설정 파일이 없습니다: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


class Topic(BaseModel):
    이름: str
    데스크지침: str = ""
    색: int = 0x0B6E77


class Audience(BaseModel):
    """audience.yaml — 편집 방향을 정하는 사람이 고치는 파일."""
    독자: dict
    중요도_기준: list[str]
    버릴_것: list[str] = Field(default_factory=list)
    토픽: list[Topic]


class Source(BaseModel):
    name: str
    url: str
    kind: str = "rss"       # rss | hn   — 사다리의 몇 칸으로 가져오는가 (섹션 3)
    tier: int = 2           # 1 = 당사자 발표(경쟁 면제), 2 = 매체 (섹션 4)


class Settings(BaseModel):
    """settings.yaml — 개발자가 고치는 임계값."""
    hours: int = 24
    batch: int = 40             # 예선 묶음 크기 (섹션 7)
    prelim_keep: int = 8        # 묶음당 예선 통과 수 — '묶음 운'을 줄이려고 넉넉히
    target: int = 5             # 최종 발행 목표
    tier1_max: int = 2          # 면제에도 상한이 필요하다 (섹션 4)
    per_source_max: int = 8     # 한 매체가 후보를 독차지하지 못하게
    min_body: int = 600         # G1 본문 관문 기준선 (섹션 4)
    model: str = "gpt-4o-mini"


# 오타는 프로그램이 시작하자마자 잡힌다 — 새벽 실행 중간에 터지는 것보다 낫다.
CFG = Audience(**_load("audience.yaml"))
_raw = _load("settings.yaml")
SOURCES = [Source(**s) for s in _raw.pop("sources", [])]
SET = Settings(**_raw)


def build_criteria(cfg: Audience) -> str:
    """선별 프롬프트에 그대로 들어가는 문단."""
    d = cfg.독자
    parts = [
        f"[독자] {d.get('누구', '')}",
        f"[독자가 이미 아는 것] {d.get('이미_아는_것', '')}",
        "[중요도 기준] 위에 있을수록 우선한다.",
        *(f"  {i}. {c}" for i, c in enumerate(cfg.중요도_기준, 1)),
    ]
    if cfg.버릴_것:
        parts += ["[버릴 것]", *(f"  - {x}" for x in cfg.버릴_것)]
    return "\n".join(parts)


def build_sys(cfg: Audience) -> str:
    """취재 프롬프트의 시스템 메시지."""
    return (
        f"당신은 {cfg.독자.get('누구', '')}을 위한 뉴스레터 기자입니다.\n"
        "모든 출력은 한국어 '~합니다'체로 씁니다.\n"
        "headline 과 summary 는 원문에 있는 내용만 옮깁니다. 원문에 없는 사실을 쓰지 마세요.\n"
        "why 는 요약에 있는 내용만 근거로, 이 독자에게 왜 중요한지 한 문장으로 씁니다."
    )


CRITERIA = build_criteria(CFG)
SYS = build_sys(CFG)


# ─────────────────────────────────────────────────────────────
# 언어 모델 호출 — 구조화 출력 (섹션 7)
# ─────────────────────────────────────────────────────────────
_client = None


def client():
    """키가 없어도 import 는 되게, 실제 호출 시점에만 만든다."""
    global _client
    if _client is None:
        from openai import OpenAI
        _client = OpenAI()
    return _client


def _parse(messages: list[dict], schema, model: str | None = None):
    """자유 문장을 파싱하는 코드는 언젠가 반드시 깨진다 — 스키마로 형식을 강제한다."""
    kwargs = dict(model=model or SET.model, messages=messages, response_format=schema)
    try:
        r = client().chat.completions.parse(**kwargs)
    except AttributeError:                       # 구버전 SDK
        r = client().beta.chat.completions.parse(**kwargs)
    return r.choices[0].message.parsed


# ─────────────────────────────────────────────────────────────
# 섹션 2 — State
#   단계 사이를 실제로 건너가는 것만 담는다.
#   drafted·log 는 합치는 키(리듀서), collected·picked·verified 는 덮어쓰는 키.
# ─────────────────────────────────────────────────────────────
class Brief(TypedDict):
    hours:     int
    collected: list
    picked:    list
    drafted:   Annotated[list, operator.add]    # 취재 워커들이 나눠 채운다
    verified:  list                             # 검수는 '줄이는' 일이라 키를 나눴다 (섹션 9)
    log:       Annotated[list, operator.add]


class WorkerState(TypedDict):
    """팬아웃된 취재 워커가 받는 것 — 자기가 맡은 기사 하나뿐. (섹션 8)"""
    item: dict


# ─────────────────────────────────────────────────────────────
# 섹션 5 — ① 자료 수집
# ─────────────────────────────────────────────────────────────
TAG_RE = re.compile(r"<[^>]+>")


def strip_tags(s: str | None) -> str:
    return html.unescape(TAG_RE.sub(" ", s or "")).strip()


def published_at(e) -> datetime | None:
    """published_parsed 는 비어 있을 때가 있다 — 그런 항목은 시간 창에서 함께 버려진다."""
    for key in ("published_parsed", "updated_parsed"):
        t = getattr(e, key, None)
        if t:
            return datetime.fromtimestamp(calendar.timegm(t), tz=timezone.utc)
    return None


UA = {"User-Agent": "Mozilla/5.0 (compatible; newsletter-agent/1.0)"}


def fetch_rss(src: Source) -> list[dict]:
    """사다리 2칸 — 발행자가 스스로 공개한 피드.

    feedparser 에 URL 을 그대로 넘기지 않고 requests 로 받아 바이트를 넘긴다.
    feedparser 의 자체 URL 열기는 certifi 를 쓰지 않아 SSL 검증에서 막히고,
    타임아웃도 걸 수 없다 — 타임아웃이 없으면 한 소스가 전체를 매달아 둔다.
    """
    r = requests.get(src.url, timeout=15, headers=UA)
    r.raise_for_status()
    feed = feedparser.parse(r.content)
    if not feed.entries:
        raise RuntimeError(f"항목 0건 (bozo={getattr(feed, 'bozo', 0)})")
    out = []
    for e in feed.entries:
        link = getattr(e, "link", "")
        if not link:
            continue
        out.append({
            "title":   strip_tags(getattr(e, "title", "")),
            "url":     link,
            "source":  src.name,
            "tier":    src.tier,
            "at":      published_at(e),
            "summary": strip_tags(getattr(e, "summary", ""))[:600],
        })
    return out


def fetch_hn(src: Source) -> list[dict]:
    """사다리 1칸 — 공개 API. RSS 에 없는 비LLM 신호(추천 수)를 함께 준다. (섹션 3)"""
    r = requests.get(src.url, timeout=15)
    r.raise_for_status()
    out = []
    for h in r.json().get("hits", []):
        at = h.get("created_at")
        out.append({
            "title":        h.get("title") or "",
            "url":          h.get("url") or f"https://news.ycombinator.com/item?id={h.get('objectID')}",
            "source":       src.name,
            "tier":         src.tier,
            "at":           datetime.fromisoformat(at.replace("Z", "+00:00")) if at else None,
            "summary":      "",
            "points":       h.get("points"),
            "num_comments": h.get("num_comments"),
        })
    return out


def collect(s: dict) -> dict:
    hours = s.get("hours") or SET.hours
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    total, dup = 0, 0
    fresh: list[dict] = []
    dead: list[str] = []
    seen: set[str] = set()

    for src in SOURCES:
        try:
            raw = fetch_hn(src) if src.kind == "hn" else fetch_rss(src)
        except Exception as exc:
            dead.append(f"{src.name}({type(exc).__name__})")   # 한 곳이 죽어도 나머지는 모인다
            continue
        total += len(raw)
        for it in raw:
            at = it["at"]
            if not at or at < cutoff:                 # 시간 창 + 날짜 없는 항목
                continue
            key = it["url"].split("?")[0]             # utm_* 꼬리표를 떼고 비교한다
            if key in seen:
                dup += 1
                continue
            seen.add(key)
            fresh.append(it)

    # 소스별 상한 — 실제로 중복 제거보다 더 많이 걸러낸다 (섹션 7)
    fresh.sort(key=lambda x: x["at"], reverse=True)
    per: dict[str, int] = {}
    kept, capped = [], 0
    for it in fresh:
        if per.get(it["source"], 0) >= SET.per_source_max:
            capped += 1
            continue
        per[it["source"]] = per.get(it["source"], 0) + 1
        kept.append(it)

    log = [f"① 수집   전체 {total} → 창({hours}h) {len(fresh) + dup} "
           f"→ 중복 -{dup} → 상한 -{capped} → 후보 {len(kept)}건"]
    if dead:
        # 이 한 줄이 없으면 소스가 조용히 빠진 채 매일 '성공'한다. (섹션 5)
        log.append(f"   · 응답 없음: {', '.join(dead)}")
    return {"collected": kept, "log": log}


# ─────────────────────────────────────────────────────────────
# 섹션 7 — ② 중요도 선별 (예선 → 본선)
# ─────────────────────────────────────────────────────────────
class Pick(BaseModel):
    index: int = Field(description="후보 목록에서 고른 기사의 번호")
    event: str = Field(description="사건 라벨. 같은 사건을 다룬 기사에는 반드시 같은 라벨을 쓸 것")
    why:   str = Field(description="고른 이유 한 줄")


class Shortlist(BaseModel):
    picks: list[Pick]
    drops: list[str] = Field(default_factory=list, description="눈에 띄게 떨어뜨린 기사와 그 이유")


def _numbered(items: list[dict]) -> str:
    lines = []
    for i, it in enumerate(items):
        sig = ""
        if it.get("points") is not None:                 # 1칸이 준 신호는 근거로 쓸 수 있다
            sig = f" [추천 {it['points']}·댓글 {it.get('num_comments', 0)}]"
        lines.append(f"{i}. ({it['source']}) {it['title']}{sig}")
    return "\n".join(lines)


def ask_picks(items: list[dict], k: int, stage: str) -> Shortlist:
    """개수가 고정된 문제는 채점이 아니라 정렬 문제다 — 한 화면에 놓고 견준다."""
    sys = (
        f"{CRITERIA}\n\n"
        f"당신은 위 기준으로 기사를 고르는 데스크입니다.\n"
        f"아래 후보 중 최대 {k}건을 중요한 순서대로 고르세요.\n"
        "같은 사건을 다룬 기사는 하나만 고르고, 같은 사건에는 같은 event 라벨을 쓰세요."
    )
    user = f"[{stage}] 후보 {len(items)}건\n\n{_numbered(items)}"
    out = _parse([{"role": "system", "content": sys},
                  {"role": "user", "content": user}], Shortlist)
    # 모델이 범위를 벗어난 번호를 줄 수 있다 — 코드로 거른다
    out.picks = [p for p in out.picks if 0 <= p.index < len(items)][:k]
    return out


def select(s: dict) -> dict:
    cands = s.get("collected") or []
    if not cands:
        return {"picked": [], "log": ["② 선별   후보 0건 — 건너뜀"]}

    # 1차 소스는 경쟁에서 빼되, 면제에도 상한을 둔다 (섹션 4)
    tier1 = [c for c in cands if c["tier"] == 1][: SET.tier1_max]
    tier1_ids = {id(c) for c in tier1}
    rest = [c for c in cands if id(c) not in tier1_ids]

    log: list[str] = []
    shortlist: list[dict] = list(tier1)

    # 예선 — 묶음끼리 서로 모르는 채로 처리된다 (Anthropic 의 sectioning 패턴)
    for i in range(0, len(rest), SET.batch):
        chunk = rest[i: i + SET.batch]
        res = ask_picks(chunk, SET.prelim_keep, "예선")
        got = [dict(chunk[p.index], event=p.event, pick_why=p.why) for p in res.picks]
        shortlist += got
        log.append(f"   예선 묶음 {len(chunk)}건 → {len(got)}건")
    log.append(f"   예선 통과 {len(shortlist)}건 (tier1 자동통과 {len(tier1)}건 포함)")

    # 본선 — 예선 통과분이면 한 화면에 놓을 수 있다
    final = ask_picks(shortlist, SET.target, "본선")

    picked, seen_events, drops = [], set(), []
    for p in final.picks:
        it = shortlist[p.index]
        ev = (p.event or it["title"]).strip().lower()
        if ev in seen_events:
            # 프롬프트에 적은 부탁은 코드로 세는 수밖에 없다 (섹션 7)
            drops.append(f"{it['title'][:40]} — 같은 사건 중복({ev})")
            continue
        seen_events.add(ev)
        picked.append(dict(it, event=p.event, pick_why=p.why))

    log.insert(0, f"② 선별   {len(cands)} → {len(picked)}건")
    # 탈락 사유가 없으면 선별이 잘못됐을 때 무엇을 고칠지 알 수 없다
    for d in drops + final.drops:
        log.append(f"   − {d}")
    return {"picked": picked, "log": log}


# ─────────────────────────────────────────────────────────────
# 섹션 8 — ③ 요약 (취재 · 팬아웃)
# ─────────────────────────────────────────────────────────────
class Draft(BaseModel):
    """칸을 나누는 기준은 '이 칸은 무엇과 대조할 수 있는가'.
       headline·summary → 원문과 대조 가능 / why → 대조 대상 없음(해석)."""
    headline: str = Field(description="한국어 헤드라인. 40자 이내")
    summary:  str = Field(description="한국어 세 문장 요약. '~합니다'체. 원문에 있는 내용만")
    why:      str = Field(description="이 독자에게 왜 중요한지 한 문장. 요약에 있는 내용만 근거로")
    topic:    str = Field(description="아래 토픽 이름 중 하나를 그대로")


HANGUL = re.compile(r"[가-힣]")


def extract_body(url: str) -> str:
    """RSS 요약만으로는 G1 을 통과하지 못한다 — 원문 주소로 한 번 더 간다. (섹션 4)"""
    try:
        r = requests.get(url, timeout=20, headers=UA)
        r.raise_for_status()
        return (trafilatura.extract(r.text, include_comments=False,
                                    include_tables=False) or "").strip()
    except Exception:
        return ""


def draft(item: dict, body: str, note: str = "") -> Draft:
    topics = "\n".join(f"- {t.이름}: {t.데스크지침}" for t in CFG.토픽)
    sys = f"{SYS}\n\n[토픽과 데스크지침]\n{topics}"
    if note:
        sys += f"\n\n{note}"
    user = f"제목: {item['title']}\n출처: {item['source']}\n\n원문:\n{body[:8000]}"
    return _parse([{"role": "system", "content": sys},
                   {"role": "user", "content": user}], Draft)


def report(s: WorkerState) -> dict:
    """워커는 전체 상황을 모른다 — 자기가 맡은 기사만 안다."""
    it = s["item"]
    body = extract_body(it["url"])
    if len(body) < SET.min_body:
        # 빠진 사실이 로그에 남는다. 모자란다고 다시 긁어 오지 않는다.
        return {"drafted": [], "log": [f"   − {it['title'][:40]} — 본문 부족({len(body)}자)"]}

    d = draft(it, body)
    tries = 1
    # 지시가 지켜졌는지 출력만 보고 확인할 수 있으면, 코드로 강제한다 (섹션 8)
    while not HANGUL.search(d.summary) and tries < 2:
        d = draft(it, body, "반드시 한국어로 쓰세요. 원문이 영어여도 출력은 한국어여야 합니다.")
        tries += 1

    rec = {**it, "headline": d.headline, "summary": d.summary,
           "why": d.why, "topic": d.topic, "body": body, "retries": tries - 1}
    log = [f"   · {d.headline[:40]} (재요청 {tries - 1}회)"] if tries > 1 else []
    return {"drafted": [rec], "log": log}


def fan_report(s: dict):
    """팬아웃 경계 — 이 단계는 다른 항목을 안 봐도 답할 수 있으므로 펼친다."""
    picked = s.get("picked") or []
    if not picked:
        return "verify"
    return [Send("report", {"item": it}) for it in picked]


# ─────────────────────────────────────────────────────────────
# 섹션 9 — ④ 검수
# ─────────────────────────────────────────────────────────────
class Verdict(BaseModel):
    ok:       bool
    problems: list[str] = Field(default_factory=list, description="원문에서 뒷받침되지 않는 부분")


def check(d: dict) -> Verdict:
    """판정은 생성과 분리된 호출이어야 한다 — 쓴 사람에게 물으면 맞다고 답한다."""
    sys = (
        "당신은 교열 담당입니다. 요약의 각 주장이 원문에서 뒷받침되는지 판정하세요.\n"
        "번역과 단위 환산은 환각이 아닙니다 (three months→3개월, $60 million→6000만 달러).\n"
        "원문에 없는 사실·숫자·인용·과장이 있으면 불합격입니다."
    )
    user = (f"[헤드라인]\n{d['headline']}\n\n[요약]\n{d['summary']}\n\n"
            f"[원문]\n{d['body'][:8000]}")
    return _parse([{"role": "system", "content": sys},
                   {"role": "user", "content": user}], Verdict)


def verify(s: dict) -> dict:
    drafted = s.get("drafted") or []
    picked  = s.get("picked") or []
    missing = len(picked) - len(drafted)

    log = [f"③ 취재   {len(drafted)}건" + (f" (본문 부족 -{missing})" if missing > 0 else "")]

    passed, failed = [], []
    for d in drafted:
        v = check(d)                      # why 는 검수하지 않는다 — 원문에 근거가 있을 수 없다
        (passed if v.ok else failed).append((d, v.problems))
    passed = [d for d, _ in passed]

    log.append(f"④ 검수   합격 {len(passed)} · 불합격 {len(failed)}")
    for d, probs in failed:
        log.append(f"   − {d['headline'][:40]} — {'; '.join(probs)[:120]}")

    # 합치는 키(drafted)에 통과분을 돌려주면 줄지 않고 늘어난다 → 키를 나눈다
    return {"verified": passed, "log": log}


# ─────────────────────────────────────────────────────────────
# 섹션 10 — ⑤ 발행
# ─────────────────────────────────────────────────────────────
EMBED_MAX, TITLE_MAX, DESC_MAX, TOTAL_MAX = 10, 256, 4096, 6000


def _cut(s: str, n: int) -> str:
    return s if len(s) <= n else s[: n - 1] + "…"


def _weight(e: dict) -> int:
    return (len(e.get("title", "")) + len(e.get("description", ""))
            + len(e.get("footer", {}).get("text", "")))


def _color(topic: str) -> int:
    for t in CFG.토픽:
        if t.이름 == topic:
            return t.색
    return 0x0B6E77


def make_lead(n: int) -> dict:
    today = datetime.now().strftime("%Y년 %m월 %d일")
    if n == 0:
        # 아무것도 안 보내면 파이프라인이 죽은 것과 구분이 안 된다 (섹션 10·11)
        return {"title": f"{today} 브리핑",
                "description": "오늘은 조용합니다.",
                "color": 0x6B7280}
    return {"title": f"{today} 브리핑", "description": f"오늘 고른 {n}건입니다.", "color": 0x0B6E77}


def build_embeds(items: list[dict]) -> list[dict]:
    """한 덩어리로 받았다면 이렇게 배치할 수 없었다 — 세 칸이 각자 다른 자리로 간다."""
    embeds = [make_lead(len(items))]
    for i, d in enumerate(items[: EMBED_MAX - 1], 1):
        embeds.append({
            "title":       _cut(f"{i}. {d['headline']}", TITLE_MAX),
            "description": _cut(f"{d['summary']}\n\n💡 **{d['why']}**", DESC_MAX),
            "url":         d["url"],
            "color":       _color(d.get("topic", "")),
            "footer":      {"text": f"{d['source']} · {d.get('topic', '')}".strip(" ·")},
        })
    # 한도를 넘기면 400 이 돌아오고 아무것도 발행되지 않는다 — 보내기 전에 자른다
    while len(embeds) > 1 and sum(_weight(e) for e in embeds) > TOTAL_MAX:
        embeds.pop()
    return embeds


def send(embeds: list[dict], dry_run: bool) -> str:
    if dry_run:
        print(f"[dry-run] embed {len(embeds)}개 · {sum(_weight(e) for e in embeds)}자 — 보내지 않음")
        print(json.dumps(embeds, ensure_ascii=False, indent=2))
        return "dry-run"
    url = os.environ.get("DISCORD_WEBHOOK_URL", "")
    if not url:
        return "웹훅 주소 없음 — 보내지 않음"
    r = requests.post(url, json={"username": "편집실", "embeds": embeds}, timeout=15)
    return str(r.status_code)          # 성공은 204 (본문 없음)


def publish(s: dict) -> dict:
    items = s.get("verified") or []
    # State 에는 body 처럼 발행에 쓰지 않는 칸이 있다 — 필요한 칸만 골라 넘긴다
    slim = [{k: d.get(k) for k in ("headline", "summary", "why", "url", "source", "topic")}
            for d in items]
    dry = os.environ.get("DRY_RUN", "1") != "0"      # 기본값은 '보내지 않음'
    code = send(build_embeds(slim), dry)
    return {"log": [f"⑤ 발행   {len(slim)}건 → {code}"]}


# ─────────────────────────────────────────────────────────────
# 섹션 2·8 — 그래프
# ─────────────────────────────────────────────────────────────
def build():
    """노드를 이름으로 찾으므로, 같은 이름으로 다시 정의하면 그것이 들어간다."""
    g = StateGraph(Brief)
    for name in ("collect", "select", "report", "verify", "publish"):
        g.add_node(name, globals()[name])
    g.add_edge(START, "collect")
    g.add_edge("collect", "select")
    g.add_conditional_edges("select", fan_report, ["report", "verify"])
    g.add_edge("report", "verify")
    g.add_edge("verify", "publish")
    g.add_edge("publish", END)
    return g


INIT = {"hours": SET.hours, "collected": [], "picked": [],
        "drafted": [], "verified": [], "log": []}


# ─────────────────────────────────────────────────────────────
# 섹션 12 — 실행마다 한 줄 남기기
# ─────────────────────────────────────────────────────────────
METRICS = ROOT / "store" / "metrics.jsonl"


def run(hours: int | None = None) -> dict:
    init = dict(INIT)
    if hours:
        init["hours"] = hours

    started = time.time()
    out = build().compile().invoke(init)

    by_source: dict[str, int] = {}
    for d in out.get("verified", []):
        by_source[d["source"]] = by_source.get(d["source"], 0) + 1

    row = {
        "run_id":      datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "hours":       init["hours"],
        "collected":   len(out.get("collected", [])),      # ─┐
        "picked":      len(out.get("picked", [])),         #  │ 깔때기
        "drafted":     len(out.get("drafted", [])),        #  │
        "published":   len(out.get("verified", [])),       # ─┘
        "by_source":   by_source,
        "elapsed_sec": round(time.time() - started, 1),
        "log":         out.get("log", []),
    }
    METRICS.parent.mkdir(exist_ok=True)
    with METRICS.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return out


if __name__ == "__main__":
    for line in run()["log"]:
        print(line)
