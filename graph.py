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
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import xml.etree.ElementTree as ET

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
    묶음: str = "기타"      # 쿼터의 단위. settings.yaml 의 quota 와 이름이 맞아야 한다
    데스크지침: str = ""
    색: int = 0x0B6E77


class Audience(BaseModel):
    """audience.yaml — 편집 방향을 정하는 사람이 고치는 파일."""
    독자: dict
    # 묶음 이름만으로는 모자란다. '전시도 문화 아닌가' 로 읽혀 분류가 틀어졌다.
    묶음설명: dict[str, str] = Field(default_factory=dict)
    중요도_기준: list[str]
    버릴_것: list[str] = Field(default_factory=list)
    토픽: list[Topic]


class Source(BaseModel):
    name: str
    url: str
    kind: str = "rss"       # rss | hn   — 사다리의 몇 칸으로 가져오는가 (섹션 3)
    tier: int = 2           # 1 = 당사자 발표(경쟁 면제), 2 = 매체 (섹션 4)
    match: str | None = None  # filters 의 이름. 종합 피드를 주제로 좁힐 때 쓴다
    tz_offset: float | None = None  # pubDate 에 타임존이 없고 현지 시각으로 적는 발행자용
    local_only: bool = False        # 해외 리전에서 닿지 않는 소스. CI 에서는 건너뛴다


class Settings(BaseModel):
    """settings.yaml — 개발자가 고치는 임계값."""
    hours: int = 24
    batch: int = 40             # 예선 묶음 크기 (섹션 7)
    prelim_keep: int = 8        # 묶음당 예선 통과 수 — '묶음 운'을 줄이려고 넉넉히
    target: int = 5             # 최종 발행 목표
    # 쿼터를 거는 자리와 결과가 정해지는 자리가 달라서 생긴 필드 두 개 (docs/발행_단계_개선점과_해결방안.md)
    overselect: int = 0         # 선별에서 뽑을 여유분. 0 이면 target 과 같다 = 후처리 꺼짐
    max_per_source: int = 0     # '발행분' 기준 매체 상한. 0 이면 제한 없음
    tier1_max: int = 2          # 면제에도 상한이 필요하다 (섹션 4)
    per_source_max: int = 8     # 한 매체가 후보를 독차지하지 못하게
    min_body: int = 600         # G1 본문 관문 기준선 (섹션 4)
    model: str = "gpt-4o-mini"
    # 이름 → 키워드 목록. 종합 피드(속보·IT 전체)에서 주제에 맞는 것만 남긴다.
    filters: dict[str, list[str]] = Field(default_factory=dict)
    # 묶음 → 최소 보장 건수. 비어 있으면 중요도 순으로만 뽑는다.
    quota: dict[str, int] = Field(default_factory=dict)


# 오타는 프로그램이 시작하자마자 잡힌다 — 새벽 실행 중간에 터지는 것보다 낫다.
CFG = Audience(**_load("audience.yaml"))
_raw = _load("settings.yaml")
SOURCES = [Source(**s) for s in _raw.pop("sources", [])]
SET = Settings(**_raw)


def topic_groups(cfg: Audience) -> dict[str, list[str]]:
    """묶음 → 그 묶음에 속한 토픽 이름들."""
    out: dict[str, list[str]] = {}
    for t in cfg.토픽:
        out.setdefault(t.묶음, []).append(t.이름)
    return out


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
    groups = topic_groups(cfg)
    if len(groups) > 1:
        parts += ["[주제 묶음] 고른 것은 반드시 아래 묶음 중 하나에 속해야 한다.",
                  "  묶음은 '무엇을 다루는가' 가 아니라 '독자가 어떻게 접하는가' 로 가른다."]
        for g, names in groups.items():
            desc = cfg.묶음설명.get(g, "")
            parts.append(f"  {g}" + (f" — {desc}" if desc else "") + f" : {', '.join(names)}")
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
GROUPS = topic_groups(CFG)          # 묶음 → 토픽 이름들
QUOTA = SET.quota                   # 묶음 → 최소 보장 건수

# 선별은 여유분까지 뽑고, 발행 직전에 target 으로 줄인다. 검수가 사이에서 건수를 깎기 때문이다.
OVERSELECT = max(SET.overselect, SET.target)
MAX_PER_SOURCE = SET.max_per_source or 10**9      # 0 = 제한 없음

# 선필터는 언어 모델을 부르기 전에 도는 문자열 검사다 — 비용 0, 결정론적.
# 예선에 무관한 기사를 잔뜩 넣으면 판단이 흔들리고 토큰만 쓴다.
FILTERS = {name: re.compile("|".join(re.escape(w) for w in words), re.I)
           for name, words in SET.filters.items() if words}


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
    published_items: list                       # 후처리로 확정한 실제 발행분 (검수 합격분의 부분집합)
    log:       Annotated[list, operator.add]


class WorkerState(TypedDict):
    """팬아웃된 취재 워커가 받는 것 — 자기가 맡은 기사 하나뿐. (섹션 8)"""
    item: dict


# ─────────────────────────────────────────────────────────────
# 섹션 5 — ① 자료 수집
# ─────────────────────────────────────────────────────────────
TAG_RE = re.compile(r"<[^>]+>")

# 추적용 꼬리표. 이것만 떼고 나머지 쿼리는 남긴다.
TRACKING = {"fbclid", "gclid", "igshid", "mc_cid", "mc_eid", "ref", "ref_src",
            "spm", "cmpid", "smid", "at_medium", "at_campaign"}


def canonical_url(url: str) -> str:
    """중복 판정용 키.

    '?' 앞을 자르는 방식은 쓸 수 없다. 한국 언론사 CMS 는 기사 번호를
    쿼리에 넣기 때문이다(aitimes .../articleView.html?idxno=215216,
    zdnet .../view/?no=2026...). 그렇게 자르면 한 매체의 기사 전부가
    같은 키가 되어 한 건만 남고 조용히 사라진다.
    """
    p = urlsplit(url)
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
         if not (k.lower().startswith("utm_") or k.lower() in TRACKING)]
    q.sort()
    return urlunsplit((p.scheme, p.netloc.lower(), p.path.rstrip("/"), urlencode(q), ""))


def strip_tags(s: str | None) -> str:
    return html.unescape(TAG_RE.sub(" ", s or "")).strip()


def published_at(e) -> datetime | None:
    """published_parsed 는 비어 있을 때가 있다 — 그런 항목은 시간 창에서 함께 버려진다."""
    for key in ("published_parsed", "updated_parsed"):
        t = getattr(e, key, None)
        if t:
            return datetime.fromtimestamp(calendar.timegm(t), tz=timezone.utc)
    return None


def _shift(at: datetime | None, offset: float | None) -> datetime | None:
    """타임존 없이 현지 시각으로 pubDate 를 적는 발행자를 보정한다.

    feedparser 는 타임존이 없는 시각을 UTC 로 읽는다. AI타임스는 KST 를
    '2026-09-14 14:32:15' 처럼 적어서, 보정하지 않으면 모든 기사가 9시간
    미래로 들어온다 — 시간 창을 늘 통과하고 정렬에서 늘 맨 위에 선다.
    """
    if at is None or not offset:
        return at
    return at - timedelta(hours=offset)


UA = {"User-Agent": "Mozilla/5.0 (compatible; newsletter-agent/1.0)"}

# GitHub Actions 러너는 해외 리전이라 국내 공공 API 에 대부분 닿지 않는다.
# 실측: 로컬 0.9초 성공 / 러너 ConnectTimeout 30초 × 3회.
# 코드로 고칠 수 있는 문제가 아니므로, 못 닿는 곳에서는 시도조차 하지 않는다.
IS_CI = os.environ.get("GITHUB_ACTIONS") == "true"

KCISA_BASE = "https://apis.data.go.kr/B553457/cultureinfo"
KCISA_DAYS = 14          # 앞으로 며칠 안에 열려 있는 것까지 볼 것인가
KCISA_REALMS = {"전시"}  # period2 는 공연·축제도 준다
KCISA_DETAIL_MAX = 15    # detail2 는 건당 한 번씩 부르므로 상한을 둔다


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
            "at":      _shift(published_at(e), src.tz_offset),
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


PUBLISHED = ROOT / "store" / "published.jsonl"
PUBLISHED_KEEP_DAYS = 90


def published_urls() -> set[str]:
    """이미 발행한 URL. 매일 같은 7일 창을 훑으므로 이게 없으면 같은 걸 또 보낸다."""
    if not PUBLISHED.exists():
        return set()
    cutoff = datetime.now(timezone.utc) - timedelta(days=PUBLISHED_KEEP_DAYS)
    out = set()
    for line in PUBLISHED.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
            at = datetime.fromisoformat(row["at"])
        except Exception:
            continue
        if at >= cutoff and row.get("url"):
            out.add(row["url"])
    return out


def record_published(items: list[dict]) -> None:
    """실제로 보낸 것만 기록한다. dry-run 을 기록하면 그 항목은 영영 발행되지 않는다."""
    PUBLISHED.parent.mkdir(exist_ok=True)
    now = datetime.now(timezone.utc).isoformat()
    with PUBLISHED.open("a", encoding="utf-8") as f:
        for d in items:
            f.write(json.dumps({"url": canonical_url(d["url"]), "at": now,
                                "headline": d.get("headline", "")},
                               ensure_ascii=False) + "\n")


def _kv(d: dict, *keys) -> str:
    """응답 필드명이 판본마다 달라 후보를 순서대로 훑는다."""
    for k in keys:
        v = d.get(k)
        if v not in (None, "", [], {}):
            return str(v).strip()
    return ""


def _ymd(v: str) -> datetime | None:
    digits = re.sub(r"[^0-9]", "", str(v))[:8]
    if len(digits) != 8:
        return None
    try:
        return datetime.strptime(digits, "%Y%m%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _kcisa_get(path: str, key: str, **params) -> ET.Element:
    """공공데이터포털은 실패 사유를 본문에 담아 준다 — 상태 코드만으로는 못 고친다.

    그래서 사유를 꺼내 예외 메시지에 싣는다. 키 값은 절대 싣지 않고,
    대신 '길이와 모양'만 남긴다 — Encoding 키(%2B·%2F 포함)를 넣으면
    requests 가 한 번 더 인코딩해 깨지는 것이 흔한 원인이라서다.
    """
    shape0 = f"키 {len(key)}자/{'인코딩형' if '%' in key else '일반형'}"
    last = None
    for attempt in (1, 2, 3):
        t0 = time.time()
        try:
            # (연결, 응답) 타임아웃을 나눈다. 국내 공공 API 는 해외 리전에서
            # 느리거나 아예 닿지 않는 일이 있어, 어느 쪽에서 막혔는지 구분해야 한다.
            # 연결 30초. 국내 공공 API 는 해외 리전(GitHub Actions)에서 첫 연결이
            # 느려 10초로는 자주 놓친다 — 같은 주소가 어떤 실행에서는 403 을
            # 돌려줬으니 막힌 게 아니라 느린 것이다.
            r = requests.get(f"{KCISA_BASE}/{path}", timeout=(30, 60), headers=UA,
                             params={"serviceKey": key, **params})
            break
        except requests.RequestException as exc:
            last = f"{type(exc).__name__} {time.time() - t0:.1f}s (시도 {attempt}/3)"
            time.sleep(2 * attempt)
    else:
        raise RuntimeError(f"{path} 연결 실패 · {last} · {shape0}")
    body = r.text or ""
    m = re.search(r"<(?:returnAuthMsg|errMsg|resultMsg)>(.*?)</(?:returnAuthMsg|errMsg|resultMsg)>", body)
    reason = (m.group(1) if m else "").strip()
    shape = f"키 {len(key)}자/{'인코딩형' if '%' in key else '일반형'}"

    if r.status_code != 200:
        raise RuntimeError(f"{path} HTTP {r.status_code} · {reason or body[:60]} · {shape}")
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        raise RuntimeError(f"{path} XML 아님 · {body[:60]} · {shape}") from None
    code = root.findtext(".//resultCode")
    if code not in (None, "00"):
        raise RuntimeError(f"{path} resultCode={code} · {reason} · {shape}")
    return root


def fetch_kcisa(src: Source) -> list[dict]:
    """사다리 1칸 — 한국문화정보원 '한눈에보는문화정보' (공공데이터포털).

    국내 전시는 RSS 가 거의 없어(네오룩·MMCA·아트맵 전부 404) 1칸으로 올라간다.

    두 번 부른다.
      period2 : 기간 안의 문화정보 목록. 제목·장소·지역·기간을 준다.
      detail2 : 그중 고른 것의 상세. 여기에만 실제 전시 페이지 url 이 있다.
    설명(contents1)은 열 중 셋만 채워져 있고 길어야 380자라 그것만으로는
    G1 을 넘지 못한다. 대신 url 이 100% 있어서 그 페이지에서 본문을 뽑는다.

    이 소스만 시간 창의 예외다. 전시에는 '발행 시각' 이 없고 기간이 있다.
    그래서 at 을 '지금' 으로 채워 창을 통과시키고, 기간은 period2 가 걸러 준다.
    """
    key = os.environ.get("KCISA_SERVICE_KEY", "")
    if not key:
        raise RuntimeError("KCISA_SERVICE_KEY 없음")

    today = datetime.now()
    root = _kcisa_get("period2", key,
                      **{"from": today.strftime("%Y%m%d"),
                         "to": (today + timedelta(days=KCISA_DAYS)).strftime("%Y%m%d"),
                         "cPage": 1, "rows": 100, "sortStdr": 1})
    # period2 는 공연·축제까지 함께 준다. 우리가 쓰는 것은 전시뿐이다.
    listed = [it for it in root.findall(".//item")
              if (it.findtext("realmName") or "").strip() in KCISA_REALMS]

    now = datetime.now(timezone.utc)
    out: list[dict] = []
    for it in listed[:KCISA_DETAIL_MAX]:
        seq = (it.findtext("seq") or "").strip()
        if not seq:
            continue
        try:
            d = _kcisa_get("detail2", key, seq=seq).find(".//item")
        except Exception:
            continue
        if d is None:
            continue
        g = lambda t: html.unescape((d.findtext(t) or "").strip())   # noqa: E731

        url = g("url")
        if not url:
            continue          # 어디서 볼 수 있는지 댈 수 없으면 싣지 않는다

        title = g("title") or (it.findtext("title") or "").strip()
        period = f"{g('startDate')}~{g('endDate')}"
        where = " ".join(x for x in (g("area"), g("sigungu"), g("place")) if x)
        info = " · ".join(x for x in (where, period, g("price")) if x)
        out.append({
            "title": title,
            "url": url,
            "source": src.name,
            "tier": src.tier,
            "at": now,                      # 발행 시각 개념이 없다 — docstring 참고
            "summary": (info + " " + g("contents1"))[:600],
            # 상세페이지 추출이 실패할 때 쓸 대체 본문
            "body_hint": f"{title}\n{info}\n주소: {g('placeAddr')}\n\n{g('contents1')}",
        })
    return out


def collect(s: dict) -> dict:
    hours = s.get("hours") or SET.hours
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    total, dup, off, seen_before = 0, 0, 0, 0
    off_by: dict[str, int] = {}
    skipped: list[str] = []
    already = published_urls()
    fresh: list[dict] = []
    dead: list[str] = []
    seen: set[str] = set()

    for src in SOURCES:
        if src.local_only and IS_CI:
            # 건너뛴 것도 반드시 남긴다. 조용히 빠지면 며칠 뒤 아무도 모른다.
            skipped.append(src.name)
            continue
        try:
            raw = ({"hn": fetch_hn, "kcisa": fetch_kcisa}
                   .get(src.kind, fetch_rss))(src)
        except Exception as exc:
            # 타입 이름만 남기면 'RuntimeError' 한 단어뿐이라 고칠 수가 없다.
            # 사유까지 실어야 로그가 진단이 된다.
            reason = str(exc).strip() or type(exc).__name__
            dead.append(f"{src.name}: {reason[:130]}")        # 한 곳이 죽어도 나머지는 모인다
            continue
        total += len(raw)
        pat = FILTERS.get(src.match) if src.match else None
        for it in raw:
            at = it["at"]
            if not at or at < cutoff:                 # 시간 창 + 날짜 없는 항목
                continue
            if pat and not pat.search(f"{it['title']} {it['summary']}"):
                off += 1                              # 주제 밖 — 걸러낸 건수를 남긴다
                off_by[src.name] = off_by.get(src.name, 0) + 1
                continue
            key = canonical_url(it["url"])            # 추적 꼬리표만 떼고 비교한다
            if key in already:                        # 지난 실행에서 이미 보낸 것
                seen_before += 1
                continue
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

    log = [f"① 수집   전체 {total} → 창({hours}h) {len(fresh) + dup + off + seen_before} "
           f"→ 주제밖 -{off} → 기발행 -{seen_before} → 중복 -{dup} "
           f"→ 상한 -{capped} → 후보 {len(kept)}건"]
    if off_by:
        log.append("   · 선필터: " + ", ".join(f"{k} -{v}" for k, v in off_by.items()))
    if skipped:
        log.append(f"   · 건너뜀(로컬 전용): {', '.join(skipped)}")
    if dead:
        # 이 한 줄이 없으면 소스가 조용히 빠진 채 매일 '성공'한다. (섹션 5)
        log.append(f"   · 응답 없음: {', '.join(dead)}")
    return {"collected": kept, "log": log}


# ─────────────────────────────────────────────────────────────
# 섹션 7 — ② 중요도 선별 (예선 → 본선)
# ─────────────────────────────────────────────────────────────
class Pick(BaseModel):
    index: int = Field(description="후보 목록에서 고른 기사의 번호")
    # 라벨이 넓으면 서로 다른 작품이 한 덩어리로 묶여 잘린다.
    # 실제로 다른 건축물 두 건이 '건축 디스플레이' 로 묶여 탈락한 적이 있다.
    event: str = Field(description="작품·전시·책 하나를 식별하는 고유한 이름(제목·전시명). "
                                   "장르나 분야가 아니다. 같은 대상을 다룬 기사끼리만 같은 라벨을 쓸 것")
    # default 를 주면 구조화 출력에서 '선택 필드' 가 되어 모델이 통째로 생략한다.
    # 필수로 두고, 값도 enum 으로 못박는다 — 부탁이 아니라 스키마로 강제한다.
    group: str = Field(description=f"주제 묶음. 반드시 다음 중 하나: {' | '.join(GROUPS)}",
                       json_schema_extra={"enum": list(GROUPS)})
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

    # 본선 — 쿼터를 채우려면 넉넉히 받아야 한다. 여기서 뽑는 것은 '발행분' 이 아니라
    # '취재 후보' 다. 검수에서 깎인 뒤 publish 가 최종 target 건을 확정한다.
    final = ask_picks(shortlist, OVERSELECT * 2 if QUOTA else OVERSELECT, "본선")

    picked, seen_events, drops = [], set(), []
    filled = {g: 0 for g in QUOTA}
    overflow: list[dict] = []
    for p in final.picks:
        it = shortlist[p.index]
        ev = (p.event or it["title"]).strip().lower()
        if ev in seen_events:
            # 프롬프트에 적은 부탁은 코드로 세는 수밖에 없다 (섹션 7)
            drops.append(f"{it['title'][:40]} — 같은 사건 중복({ev})")
            continue
        seen_events.add(ev)
        rec = dict(it, event=p.event, pick_why=p.why, group=p.group)

        # 묶음별 최소 보장을 먼저 채운다. 넘치는 것은 남은 자리를 놓고 겨룬다.
        g = p.group if p.group in QUOTA else None
        if g and filled[g] < QUOTA[g] and len(picked) < OVERSELECT:
            filled[g] += 1
            picked.append(rec)
        else:
            overflow.append(rec)

    # 한쪽 묶음이 모자라면 남은 자리를 다른 쪽으로 메운다
    for rec in overflow:
        if len(picked) >= OVERSELECT:
            drops.append(f"{rec['title'][:40]} — 자리 없음(묶음 {rec.get('group') or '미분류'})")
            continue
        picked.append(rec)

    log.insert(0, f"② 선별   {len(cands)} → {len(picked)}건"
               + (f" (여유분 {OVERSELECT} · 최종 {SET.target}건은 발행 직전 확정)"
                  if OVERSELECT > SET.target else ""))
    if QUOTA:
        got: dict[str, int] = {}
        for r in picked:
            got[r.get("group") or "미분류"] = got.get(r.get("group") or "미분류", 0) + 1
        want = " · ".join(f"{g} {got.get(g, 0)}/{q}" for g, q in QUOTA.items())
        extra = got.get("미분류", 0)
        log.insert(1, f"   쿼터: {want}" + (f" · 미분류 {extra}" if extra else ""))
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
SENT_END = re.compile(r"[.!?]+\s*")
POLITE = ("니다", "니까", "세요", "십시오")


def is_polite(text: str) -> bool:
    """'~합니다'체인지 문장 끝으로 판정한다.

    문체는 프롬프트에 두 군데나 적어도 지켜지지 않았다. 그런데 출력만 보고
    확인할 수 있는 규칙이므로 — 코드로 강제할 수 있다. (섹션 8)
    """
    sents = [s.strip() for s in SENT_END.split(text) if s.strip()]
    return bool(sents) and all(s.endswith(POLITE) for s in sents)


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
    # 공공 API 항목처럼 상세페이지 추출이 안 되는 자료는 대체 본문을 쓴다
    body = extract_body(it["url"]) or it.get("body_hint", "")
    if len(body) < SET.min_body:
        # 빠진 사실이 로그에 남는다. 모자란다고 다시 긁어 오지 않는다.
        return {"drafted": [], "log": [f"   − {it['title'][:40]} — 본문 부족({len(body)}자)"]}

    # 지시가 지켜졌는지 출력만 보고 확인할 수 있으면, 부탁으로 두지 않고 코드로 센다 (섹션 8)
    d = draft(it, body)
    tries = 1
    while tries < 3:
        notes = []
        if not HANGUL.search(d.summary):
            notes.append("반드시 한국어로 쓰세요. 원문이 영어여도 출력은 한국어여야 합니다.")
        if not (is_polite(d.summary) and is_polite(d.why)):
            notes.append("summary 와 why 의 모든 문장을 '~합니다'체로 끝내세요. "
                         "'~했다 / ~이다 / ~한다' 같은 평서체는 안 됩니다.")
        if not notes:
            break
        d = draft(it, body, "\n".join(notes))
        tries += 1

    # 재요청하고도 못 고친 건 로그에 남긴다 — 안 남기면 그대로 발행되고 아무도 모른다
    still = []
    if not HANGUL.search(d.summary):
        still.append("한국어 아님")
    if not (is_polite(d.summary) and is_polite(d.why)):
        still.append("문체 불일치")

    rec = {**it, "headline": d.headline, "summary": d.summary,
           "why": d.why, "topic": d.topic, "body": body,
           "retries": tries - 1, "style_ok": not still}

    log = []
    if tries > 1:
        log.append(f"   · {d.headline[:36]} — 재요청 {tries - 1}회"
                   + (f", 미해결({'·'.join(still)})" if still else ", 해결"))
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
    # 이 세 줄을 '개선'하려다 오탐이 0/3 → 3/3 으로 늘었다. 같은 초안 위에서
    # 세 가지 프롬프트를 비교해 확인한 결과이고, 고치려면 그때도 비교해야 한다.
    #   · 불합격 사유를 길게 나열했더니 모델이 '누락'과 '표현 차이'로 떨어뜨렸다
    #   · 근거 구절을 인용하게 했더니 요약에 없는 문장을 지어내 인용했다 (섹션 9의 ④)
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
    return {"title": f"{today} 브리핑", "description": f"오늘 고른 소식 {n}건", "color": 0x0B6E77}


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
    r = requests.post(url, json={"username": "모두의 교양", "embeds": embeds}, timeout=15)
    return str(r.status_code)          # 성공은 204 (본문 없음)


def pick_for_publish(verified: list[dict]) -> tuple[list[dict], list[str]]:
    """검수를 통과한 것 중에서 실제로 내보낼 것을 고른다.

    선별(섹션 7)에서 쿼터를 맞춰 놓아도 검수(섹션 9)가 건수를 줄이므로
    발행 시점에는 배분이 깨져 있다. 실측으로 두 회차 연속
    '한 매체 과반 · 한 묶음 0건' 이 나왔다. 보장은 결과가 정해지는
    자리에서 걸어야 한다 — 그래서 여기서 마지막으로 한 번 더 건다.
    """
    out, per_src, per_grp, dropped = [], {}, {}, []
    taken = set()                                    # dict 는 해시가 안 되므로 위치로 센다

    # 1차: 묶음 쿼터를 먼저 채운다   2차: 남은 자리를 순서대로 메운다
    for quota_phase in (True, False):
        for i, d in enumerate(verified):
            if i in taken or len(out) >= SET.target:
                continue
            g = d.get("group") or "미분류"
            if quota_phase and per_grp.get(g, 0) >= QUOTA.get(g, 0):
                continue                             # 쿼터 단계에서는 넘치는 묶음을 미룬다
            if per_src.get(d["source"], 0) >= MAX_PER_SOURCE:
                dropped.append(f"{d['headline'][:36]} — 매체 상한({d['source']} {MAX_PER_SOURCE}건)")
                taken.add(i)                         # 한 번 떨어뜨렸으면 2차에서도 떨어진다
                continue
            per_src[d["source"]] = per_src.get(d["source"], 0) + 1
            per_grp[g] = per_grp.get(g, 0) + 1
            taken.add(i)
            out.append(d)

    for i, d in enumerate(verified):                 # 자리가 모자라 못 넣은 것도 남긴다
        if i not in taken:
            dropped.append(f"{d['headline'][:36]} — 자리 없음({SET.target}건 초과)")
    return out, dropped


def publish(s: dict) -> dict:
    verified = s.get("verified") or []
    items, dropped = pick_for_publish(verified)

    # State 에는 body 처럼 발행에 쓰지 않는 칸이 있다 — 필요한 칸만 골라 넘긴다
    slim = [{k: d.get(k) for k in ("headline", "summary", "why", "url", "source", "topic")}
            for d in items]
    dry = os.environ.get("DRY_RUN", "1") != "0"      # 기본값은 '보내지 않음'
    code = send(build_embeds(slim), dry)
    if not dry and code[:1] == "2" and items:        # 2xx 로 실제로 나간 것만
        record_published(items)

    log = [f"⑤ 발행   합격 {len(verified)} → 발행 {len(slim)}건 → {code}"]
    if QUOTA or dropped:                             # 후처리가 실제로 무엇을 했는지 남긴다
        got: dict[str, int] = {}
        src: dict[str, int] = {}
        for d in items:
            g = d.get("group") or "미분류"
            got[g] = got.get(g, 0) + 1
            src[d["source"]] = src.get(d["source"], 0) + 1
        if QUOTA:
            log.append("   쿼터: " + " · ".join(f"{g} {got.get(g, 0)}/{q}" for g, q in QUOTA.items()))
        if src:
            log.append("   매체: " + " · ".join(f"{k} {v}" for k, v in src.items()))
    for d in dropped:
        log.append(f"   − {d}")
    # publish 만 쓰는 키다 — 리듀서 없이 덮어쓴다
    return {"published_items": items, "log": log}


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
        "drafted": [], "verified": [], "published_items": [], "log": []}


# ─────────────────────────────────────────────────────────────
# 섹션 12 — 실행마다 한 줄 남기기
# ─────────────────────────────────────────────────────────────
METRICS = ROOT / "store" / "metrics.jsonl"

# 자동 실행에는 화면을 보는 사람이 없다. 비밀값은 가려지는 것이 설계라(섹션 11),
# 잘못 넣었는지 확인할 방법도 함께 사라진다. 값 대신 '모양' 만 남긴다.
SECRET_KEYS = ("OPENAI_API_KEY", "DISCORD_WEBHOOK_URL", "KCISA_SERVICE_KEY")


def key_shape(name: str) -> str:
    """길이와 형태만 돌려준다. 값은 어떤 경우에도 찍지 않는다."""
    v = os.environ.get(name, "")
    if not v:
        return f"{name}=없음"
    kind = ("URL형" if v.startswith("http")
            else "sk-형" if v.startswith("sk-")
            else "일반형")
    return f"{name}={len(v)}자/{kind}"


def run(hours: int | None = None) -> dict:
    init = dict(INIT)
    if hours:
        init["hours"] = hours

    # 첫 줄에 찍는다 — 키를 잘못 넣었으면 돈을 쓰기 전에 보인다
    keys = "   · 키: " + " · ".join(key_shape(k) for k in SECRET_KEYS)
    print(keys)

    started = time.time()
    out = build().compile().invoke(init)
    out["log"] = [keys] + list(out.get("log", []))

    # 집계는 '검수 합격분' 이 아니라 '실제 나간 것' 을 센다.
    # 이 둘이 다르다는 사실 자체가 후처리를 넣은 이유다.
    published_items = out.get("published_items", [])
    by_source: dict[str, int] = {}
    by_group: dict[str, int] = {}
    for d in published_items:
        by_source[d["source"]] = by_source.get(d["source"], 0) + 1
        g = d.get("group") or "미분류"
        by_group[g] = by_group.get(g, 0) + 1

    row = {
        "run_id":      datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        # 그 실행에서 쓴 설정 — 나중에 '언제 무엇을 바꿨는지' 되짚을 수 있게 (섹션 12)
        "hours":       init["hours"],
        "per_source_max": SET.per_source_max,
        "batch":       SET.batch,
        "collected":   len(out.get("collected", [])),      # ─┐
        "picked":      len(out.get("picked", [])),         #  │ 깔때기
        "drafted":     len(out.get("drafted", [])),        #  │
        "verified":    len(out.get("verified", [])),       #  │ 검수 합격
        "published":   len(published_items),               # ─┘ 후처리 후 실제 발행
        "by_source":   by_source,
        "by_group":    by_group,        # 쿼터가 실제로 채워졌는지 (계획 §7)
        # 부탁이 몇 번 중 몇 번 지켜지는지 — '프롬프트만으로 될까'에 대한 답 (섹션 8)
        "style_retry": sum(1 for d in out.get("drafted", []) if d.get("retries", 0)),
        "style_unfixed": sum(1 for d in out.get("drafted", []) if not d.get("style_ok", True)),
        "verify_fail": len(out.get("drafted", [])) - len(out.get("verified", [])),
        # 쿼터·매체 상한 후처리가 몇 건을 걸렀는가 — overselect 값을 조정하는 근거
        "quota_dropped": len(out.get("verified", [])) - len(published_items),
        "overselect":  OVERSELECT,
        "max_per_source": SET.max_per_source,
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
