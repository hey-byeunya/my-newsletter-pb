# run.py — 진입점. graph.py 는 불러오기만으로는 아무 일도 하지 않는다. (섹션 11)
import os
import sys

if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        os.environ["DRY_RUN"] = "1"        # 섹션 10의 스위치
    if "--send" in sys.argv:
        os.environ["DRY_RUN"] = "0"        # 실제로 보내려면 명시적으로

    # 로컬에서만 .env 를 읽는다. Actions 에서는 secrets 가 env 로 들어온다.
    # 이미 들어와 있는 값은 덮어쓰지 않는다 — 위의 --send 가 .env 보다 우선한다.
    try:
        from pathlib import Path
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parent / ".env")
    except ImportError:
        pass

    from graph import run                  # 키를 설정한 뒤에 불러온다

    # 인자는 사람이 손으로 치는 것이라 반드시 틀린다. 조용히 넘기면
    # 기본값으로 돌면서 "왜 내가 준 값이 안 먹지" 를 한참 헤매게 된다.
    hours = None
    for i, a in enumerate(sys.argv):
        if a != "--hours":
            continue
        if i + 1 >= len(sys.argv):
            sys.exit("--hours 뒤에 숫자가 필요합니다. 예: --hours 48")
        raw = sys.argv[i + 1]
        try:
            hours = int(raw)
        except ValueError:
            sys.exit(f"--hours 는 숫자여야 합니다 (받은 값: {raw!r}). 예: --hours 48")
        if not 1 <= hours <= 8760:
            # 0 은 특히 조용히 먹히기 쉽다 — run() 의 `if hours` 에서 falsy 로 걸러졌다
            sys.exit(f"--hours 는 1~8760(1년) 사이여야 합니다 (받은 값: {hours}).")

    out = run(hours)
    for line in out["log"]:
        print(line)                        # 이 출력이 Actions 로그에 그대로 남는다
