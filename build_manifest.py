import json
from pathlib import Path

DATA_ROOT = Path("data")   # change if your data folder is elsewhere

def main():
    out = {}
    for state_dir in DATA_ROOT.iterdir():
        if not state_dir.is_dir():
            continue
        state = state_dir.name
        acs = []
        for ac_dir in sorted(state_dir.glob("ac=*")):
            meta_path = ac_dir / "meta.json"
            if not meta_path.exists():
                continue
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
            except Exception:
                continue

            # Defensive: normalize AC number
            ac_no = meta.get("ac_no")
            if ac_no is None:
                # try from folder name ac=XX
                ac_no = int(ac_dir.name.split("=")[-1])
            acs.append({
                "ac_no": int(ac_no),
                "row_count": int(meta.get("row_count", 0)),
                "parts_count": meta.get("parts_count", None),
                "prefix_len": int(meta.get("prefix_len", 3)),
                "path": f"/data/{state}/ac={int(ac_no):02d}/"
            })

        acs.sort(key=lambda x: x["ac_no"])
        out[state] = {
            "state_code": state,
            "acs": acs
        }

    # Write per-state manifests + a top-level manifest
    for state, payload in out.items():
        (DATA_ROOT / state / "ac_manifest.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

    (DATA_ROOT / "manifest.json").write_text(
        json.dumps({"states": list(out.keys())}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

    print("Wrote:")
    for state in out.keys():
        print(f" - data/{state}/ac_manifest.json")
    print(" - data/manifest.json")

if __name__ == "__main__":
    main()
