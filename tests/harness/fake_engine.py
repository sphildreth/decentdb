import argparse
import json


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--sql")
    parser.add_argument("--open-close", action="store_true")
    args = parser.parse_args()

    payload = {"ok": True, "error": None, "rows": []}
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
