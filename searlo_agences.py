#!/usr/bin/env python3
"""
Enrichit leboncoin_extraction_final.csv (siren;nom;adresse) via l'API Searlo (Google Places)
et écrit agences.csv.

- 3 clés Searlo utilisées dans l'ordre : SEARLO_KEY_1 (3000), SEARLO_KEY_2 (3000), SEARLO_KEY_3 (2900)
- reprise automatique : searlo_state.json mémorise les siren déjà traités et les crédits consommés
- doublons gérés, rapport final : rapport.txt (+ résumé GitHub Actions)

Variables d'environnement optionnelles :
  MAX_REQUESTS  nombre max de requêtes pour ce run (0 = illimité, utile pour tester)
  SEARLO_URL    endpoint (défaut : https://api.searlo.tech/api/v1/search/places)
"""

import csv
import json
import os
import re
import sys
import time
from collections import Counter

import requests

# --------------------------------------------------------------------------- config
INPUT_CSV = "leboncoin_extraction_final.csv"
OUTPUT_CSV = "agences.csv"
STATE_FILE = "searlo_state.json"
REPORT_FILE = "rapport.txt"

SEARLO_URL = os.getenv("SEARLO_URL", "https://api.searlo.tech/api/v1/search/places")
MAX_REQUESTS = int(os.getenv("MAX_REQUESTS", "0") or 0)

NUM = 2          # limite de résultats
PAGE = 1
GL = "fr"        # pays : France
HL = "fr"        # langue : français

# (variable d'environnement, quota de crédits) — ordre d'insertion = ordre d'utilisation
KEYS = [
    ("SEARLO_KEY_1", 3000),
    ("SEARLO_KEY_2", 3000),
    ("SEARLO_KEY_3", 2900),
]

OUT_FIELDS = [
    "siren", "nom", "adresse", "rating", "rating_count",
    "phone_number", "website", "horaires", "featureId",
]

DAY_ORDER = ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"]

SAVE_EVERY = 10
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
PAUSE_BETWEEN_REQUESTS = 0.2


# --------------------------------------------------------------------------- helpers
def fmt_time(t: str) -> str:
    """'10:00' -> '10h' ; '09:30' -> '9h30'"""
    m = re.match(r"^\s*(\d{1,2}):(\d{2})", t)
    if not m:
        return t.strip()
    h, mm = int(m.group(1)), m.group(2)
    return f"{h}h" if mm == "00" else f"{h}h{mm}"


def fmt_slot(slot: str) -> str:
    """'10:00–12:00' -> '10h-12h' ; 'Fermé' -> 'Fermé'"""
    parts = re.split(r"\s*[–—-]\s*", slot.strip())
    if len(parts) == 2 and all(re.match(r"^\s*\d{1,2}:\d{2}", p) for p in parts):
        return f"{fmt_time(parts[0])}-{fmt_time(parts[1])}"
    return slot.strip()


def format_hours(hours) -> str:
    """Tout dans une seule case : 'lundi : 10h-12h 14h-18h | mardi : ...'"""
    if not hours:
        return ""
    by_day = {}
    for entry in hours:
        day = (entry.get("day") or "").strip().lower()
        slots = " ".join(fmt_slot(s) for s in (entry.get("hours") or []))
        by_day[day] = slots
    ordered = [d for d in DAY_ORDER if d in by_day] + [d for d in by_day if d not in DAY_ORDER]
    return " | ".join(f"{d} : {by_day[d]}" for d in ordered)


def clean_address(address: str, title: str) -> str:
    """Google renvoie 'Nom, adresse' : on retire le nom en préfixe."""
    address = (address or "").strip()
    prefix = f"{title}, " if title else ""
    if prefix and address.startswith(prefix):
        address = address[len(prefix):]
    return address


def place_to_row(siren: str, place: dict) -> dict:
    title = place.get("title") or ""
    return {
        "siren": siren,
        "nom": title,
        "adresse": clean_address(place.get("address"), title),
        "rating": place.get("rating") if place.get("rating") is not None else "",
        "rating_count": place.get("ratingCount") if place.get("ratingCount") is not None else "",
        "phone_number": place.get("phoneNumber") or "",
        "website": place.get("website") or "",
        "horaires": format_hours(place.get("hours")),
        "featureId": place.get("featureId") or place.get("cid") or "",
    }


def dedup_key(row: dict) -> str:
    """Identifiant d'un lieu : featureId, sinon nom+adresse."""
    return row["featureId"] or f'{row["nom"]}|{row["adresse"]}'


def read_input():
    with open(INPUT_CSV, newline="", encoding="utf-8-sig") as f:
        sample = f.read(2048)
        f.seek(0)
        delim = ";" if sample.count(";") >= sample.count(",") else ","
        return list(csv.DictReader(f, delimiter=delim))


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, encoding="utf-8") as f:
            st = json.load(f)
    else:
        st = {}
    st.setdefault("done", {})                      # siren -> nombre de résultats gardés
    st.setdefault("credits_used", {k: 0 for k, _ in KEYS})
    st.setdefault("exhausted", [])                 # clés marquées épuisées / invalides
    for k, _ in KEYS:
        st["credits_used"].setdefault(k, 0)
    return st


def save_state(st):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=0)
    os.replace(tmp, STATE_FILE)


# --------------------------------------------------------------------------- clés
class AllKeysExhausted(Exception):
    pass


class KeyManager:
    def __init__(self, state):
        self.state = state
        self.keys = []
        for env, quota in KEYS:
            value = os.getenv(env, "").strip()
            if value:
                self.keys.append({"env": env, "quota": quota, "value": value})
            else:
                print(f"⚠️  {env} absente : ignorée")
        if not self.keys:
            sys.exit("❌ Aucune clé SEARLO_KEY_x définie.")

    def current(self):
        for k in self.keys:  # par ordre d'insertion
            if k["env"] in self.state["exhausted"]:
                continue
            if self.state["credits_used"][k["env"]] >= k["quota"]:
                continue
            return k
        raise AllKeysExhausted()

    def add_usage(self, key, credits):
        self.state["credits_used"][key["env"]] += credits

    def mark_exhausted(self, key):
        if key["env"] not in self.state["exhausted"]:
            self.state["exhausted"].append(key["env"])
        print(f"🔑 {key['env']} marquée épuisée/invalide, passage à la suivante")


# --------------------------------------------------------------------------- appel API
def search_place(km: KeyManager, nom: str, adresse: str):
    """Retourne (places, total_brut, credits). Lève AllKeysExhausted si plus de clé."""
    params = {"q": nom, "location": adresse, "gl": GL, "hl": HL, "num": NUM, "page": PAGE}
    attempt = 0
    while True:
        key = km.current()
        try:
            r = requests.get(
                SEARLO_URL, params=params,
                headers={"x-api-key": key["value"]}, timeout=REQUEST_TIMEOUT,
            )
        except requests.RequestException as e:
            attempt += 1
            if attempt >= MAX_RETRIES:
                raise RuntimeError(f"réseau: {e}")
            time.sleep(2 * attempt)
            continue

        if r.status_code == 200:
            data = r.json()
            places = data.get("places") or []
            credits = data.get("credits", 1) or 1
            km.add_usage(key, credits)
            return places, len(places), credits

        body = r.text[:300].lower()
        quota_msg = any(w in body for w in ("credit", "crédit", "quota", "limit"))
        if r.status_code in (401, 402, 403) or (r.status_code == 429 and quota_msg):
            km.mark_exhausted(key)       # clé invalide ou plus de crédits -> clé suivante
            continue
        if r.status_code == 429 or r.status_code >= 500:
            attempt += 1
            if attempt >= MAX_RETRIES:
                raise RuntimeError(f"HTTP {r.status_code}")
            time.sleep(5 * attempt)
            continue
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:150]}")


# --------------------------------------------------------------------------- main
def main():
    rows_in = read_input()
    stats = Counter()
    stats["lignes_csv"] = len(rows_in)

    # --- dédoublonnage de l'entrée (même siren)
    inputs, seen_siren = [], set()
    for r in rows_in:
        siren = (r.get("siren") or "").strip()
        nom = (r.get("nom") or "").strip()
        adresse = (r.get("adresse") or "").strip()
        if not siren or not nom:
            stats["entree_invalide"] += 1
            continue
        if siren in seen_siren:
            stats["doublons_entree"] += 1
            continue
        seen_siren.add(siren)
        inputs.append({"siren": siren, "nom": nom, "adresse": adresse})
    stats["siren_uniques"] = len(inputs)

    state = load_state()
    km = KeyManager(state)
    credits_start = dict(state["credits_used"])

    # --- relecture de agences.csv (reprise + détection de doublons)
    seen_pairs = set()                 # (siren, lieu)
    place_owners = {}                  # lieu -> {siren}
    new_file = not os.path.exists(OUTPUT_CSV)
    if not new_file:
        with open(OUTPUT_CSV, newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f, delimiter=";"):
                k = dedup_key(r)
                seen_pairs.add((r["siren"], k))
                place_owners.setdefault(k, set()).add(r["siren"])

    todo = [r for r in inputs if r["siren"] not in state["done"]]
    stats["deja_traites"] = len(inputs) - len(todo)
    if MAX_REQUESTS:
        todo = todo[:MAX_REQUESTS]
    print(f"▶ {len(inputs)} siren | déjà traités : {stats['deja_traites']} | à traiter ce run : {len(todo)}")

    errors, not_found, truncated, stop_reason = [], [], [], None

    out = open(OUTPUT_CSV, "a", newline="", encoding="utf-8-sig" if new_file else "utf-8")
    writer = csv.DictWriter(out, fieldnames=OUT_FIELDS, delimiter=";")
    if new_file:
        writer.writeheader()

    try:
        for i, item in enumerate(todo, 1):
            siren = item["siren"]
            try:
                places, total, _ = search_place(km, item["nom"], item["adresse"])
            except AllKeysExhausted:
                stop_reason = "toutes les clés Searlo sont épuisées"
                break
            except Exception as e:  # noqa: BLE001
                errors.append((siren, item["nom"], str(e)))
                stats["erreurs"] += 1
                continue  # pas marqué "done" -> réessayé au prochain run

            stats["requetes"] += 1

            if total > NUM:
                stats["plus_de_2_resultats"] += 1
                truncated.append((siren, item["nom"], total))
            places = places[:NUM]

            kept = 0
            for p in places:
                row = place_to_row(siren, p)
                k = dedup_key(row)
                if (siren, k) in seen_pairs:           # même lieu deja écrit pour ce siren
                    stats["doublons_supprimes"] += 1
                    continue
                seen_pairs.add((siren, k))
                owners = place_owners.setdefault(k, set())
                if owners and siren not in owners:     # même lieu trouvé pour un autre siren
                    stats["lieu_partage_entre_siren"] += 1
                owners.add(siren)
                writer.writerow(row)
                kept += 1
            out.flush()

            state["done"][siren] = kept
            stats[f"siren_avec_{kept}_resultat(s)"] += 1
            if kept == 0:
                not_found.append((siren, item["nom"], item["adresse"]))

            if i % SAVE_EVERY == 0:
                save_state(state)
            if i % 100 == 0:
                print(f"  … {i}/{len(todo)}")
            time.sleep(PAUSE_BETWEEN_REQUESTS)
    finally:
        out.close()
        save_state(state)

    write_report(stats, state, credits_start, errors, not_found, truncated, place_owners, stop_reason, len(inputs))


def write_report(stats, state, credits_start, errors, not_found, truncated, place_owners, stop_reason, total_inputs):
    restant = total_inputs - len(state["done"])
    shared = {k: s for k, s in place_owners.items() if len(s) > 1}

    lines = [
        "=== COMPTE RENDU ===",
        f"Lignes dans le CSV source        : {stats['lignes_csv']}",
        f"  - lignes invalides (siren/nom) : {stats['entree_invalide']}",
        f"  - doublons de siren (ignorés)  : {stats['doublons_entree']}",
        f"Siren uniques                    : {stats['siren_uniques']}",
        f"Déjà traités avant ce run        : {stats['deja_traites']}",
        "",
        f"Requêtes envoyées ce run         : {stats['requetes']}",
        f"  - siren avec 2 résultats       : {stats['siren_avec_2_resultat(s)']}",
        f"  - siren avec 1 résultat        : {stats['siren_avec_1_resultat(s)']}",
        f"  - siren sans résultat          : {stats['siren_avec_0_resultat(s)']}",
        f"  - réponses > 2 résultats (coupées à 2) : {stats['plus_de_2_resultats']}",
        f"Doublons supprimés (siren+lieu)  : {stats['doublons_supprimes']}",
        f"Lieux partagés entre plusieurs siren (gardés, à vérifier) : {len(shared)}",
        f"Erreurs (seront réessayées)      : {stats['erreurs']}",
        "",
        "Crédits Searlo :",
    ]
    for env, quota in KEYS:
        used = state["credits_used"][env]
        run = used - credits_start.get(env, 0)
        flag = " (épuisée/invalide)" if env in state["exhausted"] else ""
        lines.append(f"  - {env} : {used}/{quota} utilisés (+{run} ce run), reste {max(quota - used, 0)}{flag}")
    lines += ["", f"Siren restant à traiter          : {restant}"]
    if stop_reason:
        lines.append(f"⛔ Arrêt anticipé : {stop_reason}")
    elif restant == 0:
        lines.append("✅ Tous les siren sont traités.")
    else:
        lines.append("↻ Relancer le workflow pour continuer (reprise automatique).")

    if truncated:
        lines += ["", "--- Réponses > 2 résultats (siren, nom, nb) ---"]
        lines += [f"{s};{n};{t}" for s, n, t in truncated]
    if shared:
        lines += ["", "--- Lieux (featureId) partagés entre plusieurs siren ---"]
        lines += [f"{k} -> {', '.join(sorted(s))}" for k, s in shared.items()]
    if not_found:
        lines += ["", "--- Siren sans résultat (siren;nom;adresse) ---"]
        lines += [f"{s};{n};{a}" for s, n, a in not_found]
    if errors:
        lines += ["", "--- Erreurs (siren;nom;message) ---"]
        lines += [f"{s};{n};{m}" for s, n, m in errors]

    text = "\n".join(lines)
    print("\n" + text)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(text + "\n")

    summary = os.getenv("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write("```\n" + "\n".join(lines[:30]) + "\n```\n")


if __name__ == "__main__":
    main()
