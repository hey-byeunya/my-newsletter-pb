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

    hours = None
    for i, a in enumerate(sys.argv):
        if a == "--hours" and i + 1 < len(sys.argv):
            hours = int(sys.argv[i + 1])

    out = run(hours)
    for line in out["log"]:
        print(line)                        # 이 출력이 Actions 로그에 그대로 남는다
