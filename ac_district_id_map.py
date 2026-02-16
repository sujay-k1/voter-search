import json

def load_district_manifest(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_ac_to_district_id(manifest: dict) -> dict[int, str]:
    ac_to_dist = {}
    for d in manifest["districts"]:
        did = d["id"]  # this is the slug
        for ac in d["acs"]:
            if ac in ac_to_dist and ac_to_dist[ac] != did:
                raise ValueError(f"AC {ac} mapped twice: {ac_to_dist[ac]} vs {did}")
            ac_to_dist[ac] = did
    return ac_to_dist

# Example:
manifest = load_district_manifest("data/s27/district_manifest.json")
ac_to_dist = build_ac_to_district_id(manifest)
print(ac_to_dist[40])  # -> "dhanbad" (expected)
