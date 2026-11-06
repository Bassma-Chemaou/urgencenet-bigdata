#!/usr/bin/env python3
"""Générateur d'événements urgences (mode file = historique, mode kafka = flux)."""
import argparse, heapq, json, math, os, random, time
from collections import Counter
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
FMT = "%Y-%m-%dT%H:%M:%SZ"
CITY_CODE = {"Casablanca": "CAS", "Rabat": "RAB", "Marrakech": "MAR", "Tanger": "TAN"}
ALL = ["Traumatologie", "Cardiologie", "Pédiatrie", "Médecine générale",
       "Chirurgie", "Neurologie", "Pneumologie", "Gynécologie"]
DEPTS = {
    "CHU": ALL,
    "HOPITAL_REGIONAL": [d for d in ALL if d != "Neurologie"],
    "CLINIQUE": ["Traumatologie", "Cardiologie", "Chirurgie", "Médecine générale", "Gynécologie"],
    "CENTRE_SANTE": ["Médecine générale", "Pédiatrie", "Traumatologie"],
}
MEAN_WAIT = {1: 5, 2: 12, 3: 25, 4: 40, 5: 55}                 # minutes, selon la priorité
TYPE_W = {"CHU": 6, "HOPITAL_REGIONAL": 3, "CLINIQUE": 1, "CENTRE_SANTE": 0.2}
HOUR_W = [1,1,1,1,1,2,3,5,7,8,8,8,7,7,7,7,8,8,8,7,5,4,3,2]


def load_facilities(path):
    facs = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                d = json.loads(line)
                facs[d["facility_id"]] = d
    by_city = {}
    for fid, d in facs.items():
        by_city.setdefault(d["city"], []).append(fid)
    for city, ids in by_city.items():                      # 2 postes de triage par établissement
        code = CITY_CODE.get(city, city[:3].upper())
        for j, fid in enumerate(sorted(ids)):
            facs[fid]["desks"] = [f"T-{code}-{2*j+1:02d}", f"T-{code}-{2*j+2:02d}"]
    for idx, fid in enumerate(facs):
        facs[fid]["offset"] = (idx * 7) % 30               # décalage du cycle d'activité
    return facs


class Gen:
    def __init__(self, facs, rng, patients, visit_start, late_rate, dup_rate,
                 anomaly_rate, violation_rate):
        self.facs, self.rng, self.patients = facs, rng, patients
        self.vc = visit_start
        self.late_rate, self.dup_rate = late_rate, dup_rate
        self.anomaly_rate, self.violation_rate = anomaly_rate, violation_rate
        self.ids = list(facs)
        self.w = [facs[i]["beds"] ** 0.6 for i in self.ids]
        self.same, self.sw, self.other_chu, self.bad = {}, {}, {}, {}
        for i in self.ids:
            g = facs[i]["hospital_group"]
            self.same[i] = [j for j in self.ids if j != i and facs[j]["hospital_group"] == g]
            self.sw[i] = [TYPE_W[facs[j]["type"]] for j in self.same[i]]
            self.other_chu[i] = [j for j in self.ids if facs[j]["hospital_group"] != g
                                 and facs[j]["type"] == "CHU"]
            self.bad[i] = [j for j in self.ids if facs[j]["hospital_group"] != g
                           and facs[j]["type"] != "CHU"]

    # --- activité par cycles de 30 min (15 min actif / 15 min calme) : crée des sessions de triage
    def active(self, fid, t):
        return ((t.timestamp() / 60.0 + self.facs[fid]["offset"]) % 30) < 15

    def pick_facility(self, t=None):
        for _ in range(100):
            f = self.rng.choices(self.ids, self.w)[0]
            if t is None or self.active(f, t):
                return f
        return f

    def pick_same(self, fid):
        return self.rng.choices(self.same[fid], self.sw[fid])[0]

    def pick_dest(self, fid, violation=True):
        r = self.rng.random()
        v = self.violation_rate if violation else 0.0
        if r < v and self.bad[fid]:                        # rare : inter-groupe vers non-CHU
            return self.rng.choice(self.bad[fid])
        if r < v + 0.12 and self.other_chu[fid]:           # inter-groupe vers un CHU (autorisé)
            return self.rng.choice(self.other_chu[fid])
        return self.pick_same(fid)

    def emit(self, dt, ev):
        delay = self.rng.randint(60, 900) if self.rng.random() < self.late_rate else 0
        e = dt + timedelta(seconds=delay)
        out = [(e, ev)]
        if self.rng.random() < self.dup_rate:              # doublon exact (même visit_id + status)
            out.append((e + timedelta(seconds=self.rng.randint(0, 5)), dict(ev)))
        return out

    def visit(self, fid, t0, patient, forced):
        rng, fac = self.rng, self.facs[fid]
        t0 = t0.replace(microsecond=0)
        self.vc += 1
        vid = "V-%07d" % self.vc
        prio = rng.choice([1, 2]) if forced is not None else \
            rng.choices([1, 2, 3, 4, 5], [5, 15, 35, 30, 15])[0]
        dept = rng.choice(DEPTS[fac["type"]])
        tri_s = rng.randint(60, 480)
        mean = MEAN_WAIT[prio] * (1.3 if fac["type"] == "CHU" else 1.0)
        wait = int(min(180, 2 + rng.expovariate(1.0 / mean)))
        wait = max(wait, tri_s // 60 + 2)
        t_tri = t0 + timedelta(seconds=tri_s)
        t_care = t0 + timedelta(minutes=wait)
        if forced is not None:
            dur = rng.randint(10, 60)
            dest = forced.pop(0) if forced else None
        else:
            dur = rng.randint(10, 90)
            dest = self.pick_dest(fid) if rng.random() < (0.15 if prio <= 2 else 0.05) else None
        t_end = t_care + timedelta(minutes=dur, seconds=rng.randint(0, 59))

        def ev(status, t, priority=None, wait_m=None, desk=None, to=None):
            return t, {"visit_id": vid, "patient_id": patient, "facility_id": fid,
                       "city": fac["city"], "status": status, "priority": priority,
                       "wait_minutes": wait_m, "department": dept, "desk_id": desk,
                       "transfer_to": to, "event_time": t.strftime(FMT)}

        evs = [ev("ARRIVED", t0),
               ev("TRIAGED", t_tri, priority=prio, desk=rng.choice(fac["desks"])),
               ev("CARE_STARTED", t_care, priority=prio, wait_m=wait),
               ev("TRANSFERRED" if dest else "DISCHARGED", t_end, priority=prio, to=dest)]
        return dest, t_end, evs

    def new_chain(self, t0, fid):
        rng = self.rng
        patient = "P-%05d" % rng.randint(1, self.patients)
        r, forced = rng.random(), None
        if r < self.anomaly_rate:                          # ping-pong A->B->A->B->A
            b = self.pick_same(fid)
            forced = [b, fid, b, fid]
        elif r < 2 * self.anomaly_rate:                    # longue chaîne de 4 transferts
            forced, cur = [], fid
            for _ in range(4):
                cur = self.pick_dest(cur, violation=False)
                forced.append(cur)
        out, cur, t = [], fid, t0
        while True:
            dest, t_end, evs = self.visit(cur, t, patient, forced)
            for dt, e in evs:
                out.extend(self.emit(dt, e))
            if dest is None:
                return out
            cur, t = dest, t_end + timedelta(minutes=rng.randint(10, 40))


def run_file(gen, args, rng):
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    events = []
    while len(events) < args.n:
        fid = gen.pick_facility(None)
        while True:
            t = start + timedelta(days=rng.randrange(args.days),
                                  hours=rng.choices(range(24), HOUR_W)[0],
                                  seconds=rng.randint(0, 3599))
            if gen.active(fid, t):
                break
        events.extend(gen.new_chain(t, fid))
    events.sort(key=lambda x: x[0])                        # ordre d'émission (retards inclus)
    events = events[:args.n]
    with open(args.out, "w", encoding="utf-8") as f:
        for _, e in events:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    st = Counter(e["status"] for _, e in events)
    print("Fichier :", args.out)
    print("Événements :", len(events), "| passages :", len({e["visit_id"] for _, e in events}))
    print("Par statut :", dict(st))


def run_kafka(gen, args, rng):
    from kafka import KafkaProducer
    prod = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all", linger_ms=20,
                         key_serializer=lambda k: k.encode(),
                         value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode())
    visit_rate = args.rate / 4.3                           # ~4,3 événements par passage
    heap, seq, sent = [], 0, 0
    v0, w0 = datetime.now(timezone.utc), time.time()
    next_arr, last = 0.0, time.time()
    print(f"Envoi vers {args.bootstrap}/{args.topic} : ~{args.rate} evt/s, "
          f"temps simulé x{args.speedup}. Ctrl+C pour arrêter.")
    try:
        while True:
            wall = time.time() - w0
            vnow = v0 + timedelta(seconds=wall * args.speedup)
            while next_arr <= wall:
                fid = gen.pick_facility(vnow)
                for emit_dt, e in gen.new_chain(vnow, fid):
                    seq += 1
                    heapq.heappush(heap, (emit_dt.timestamp(), seq, e))
                next_arr += rng.expovariate(visit_rate)
            now_ts = vnow.timestamp()
            while heap and heap[0][0] <= now_ts:
                _, _, e = heapq.heappop(heap)
                prod.send(args.topic, key=e["visit_id"], value=e)
                sent += 1
            if time.time() - last >= 5:
                print(f"[{vnow.strftime(FMT)}] envoyés : {sent}  (en attente : {len(heap)})", flush=True)
                last = time.time()
            time.sleep(0.02)
    except KeyboardInterrupt:
        prod.flush()
        print("Arrêt. Total envoyé :", sent)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["file", "kafka"], required=True)
    ap.add_argument("--n", type=int, default=50000)
    ap.add_argument("--rate", type=float, default=20)
    ap.add_argument("--out", default=os.path.join(HERE, "urgences_history.jsonl"))
    ap.add_argument("--facilities", default=os.path.join(HERE, "facilities.json"))
    ap.add_argument("--bootstrap", default="kafka:9092")
    ap.add_argument("--topic", default="urgences")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--start", default="2026-10-05")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--speedup", type=float, default=20,
                    help="temps simulé / temps réel (1 = temps réel)")
    ap.add_argument("--patients", type=int, default=None)
    ap.add_argument("--late-rate", type=float, default=0.05)
    ap.add_argument("--dup-rate", type=float, default=0.02)
    ap.add_argument("--anomaly-rate", type=float, default=0.003)
    ap.add_argument("--violation-rate", type=float, default=0.002)
    a = ap.parse_args()

    rng = random.Random(a.seed)
    facs = load_facilities(a.facilities)
    if a.mode == "file":
        patients, vstart = a.patients or max(1000, a.n // 6), 0
    else:                                                  # ids uniques d'une exécution à l'autre
        patients, vstart = a.patients or 5000, int(time.time() * 5) % 9_000_000 + 100_000
    g = Gen(facs, rng, patients, vstart, a.late_rate, a.dup_rate,
            a.anomaly_rate, a.violation_rate)
    run_file(g, a, rng) if a.mode == "file" else run_kafka(g, a, rng)