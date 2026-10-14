"""
Seed mode: build the university's full history, from the first academic year up to
`config.seed_until`, in one run.

Order matters, because of foreign keys:
    reference data -> courses and curriculum -> students -> enrollments
    -> course offerings -> faculty -> registrations -> class sessions -> exams
    -> attendance -> exam results -> fee invoices -> payments

Everything is built in memory with explicit IDs, except attendance, which is
streamed straight into the database because it has millions of rows.
"""

import math
import random
import time as clock
from collections import defaultdict
from datetime import date, datetime, timedelta

from faker import Faker
from sqlalchemy import func, select

from src.generator.common import (
    at, attendance_status, grade_for, indian_phone, invalid_phone,
    payment_mode, section_letter, term_calendar,
)
from src.generator.profiles import student_profile
from src.generator.reference import (
    BTECH, BTECH_COMMON, CITIES, DEPARTMENTS, DESIGNATIONS, FEE_COMPONENTS, LAB,
    PROGRAMS, THEORY,
)
from src.generator.writer import BulkWriter, sync_identity
from src.source_db import schema as s

AUDIT = ("created_at", "updated_at", "is_deleted")

# Department that teaches each shared first-year B.Tech course
COMMON_COURSE_DEPT = {
    "Engineering Mathematics I": "SH", "Engineering Mathematics II": "SH",
    "Engineering Physics": "SH", "Engineering Physics Lab": "SH", "Engineering Chemistry": "SH",
    "English Communication": "SH", "Programming in C": "CSE", "Programming Lab": "CSE",
    "Basic Electrical Engineering": "ECE", "Engineering Graphics": "ME",
}

FACULTY_EXIT_CUTOFF = date(2025, 7, 1)   # faculty who have exited taught only before this date


def run_seed(engine, cfg):
    started = clock.time()
    _require_empty(engine)

    rng = random.Random(cfg.seed)
    fake = Faker("en_IN")
    fake.seed_instance(cfg.seed)
    writer = BulkWriter(engine)
    until = cfg.seed_until
    epoch = at(date(cfg.first_year, 6, 1), 10)          # when reference data was "created"

    def step(name):
        print(f"  {name:<28} {clock.time() - started:6.1f}s")

    # ------------------------------------------------------------------ reference data
    dept_id = {}
    rows = []
    for i, (code, name) in enumerate(DEPARTMENTS, start=1):
        dept_id[code] = i
        rows.append((i, code, name, epoch, epoch, False))
    writer.write(s.department, ("department_id", "dept_code", "name") + AUDIT, rows)

    program_id = {}
    rows = []
    for i, (short, p) in enumerate(PROGRAMS.items(), start=1):
        program_id[short] = i
        rows.append((i, dept_id[p["dept"]], p["code"], p["name"], p["level"], p["terms"], epoch, epoch, False))
    writer.write(s.program, ("program_id", "department_id", "program_code", "name", "degree_level",
                             "duration_terms") + AUDIT, rows)

    years, terms = term_calendar(cfg.first_year, until.year)
    year_id = {}
    rows = []
    for i, y in enumerate(years, start=1):
        year_id[y["year"]] = i
        rows.append((i, y["label"], y["start"], y["end"], epoch, epoch, False))
    writer.write(s.academic_year, ("academic_year_id", "label", "start_date", "end_date") + AUDIT, rows)

    rows = []
    for t in terms:
        t["id"] = t["idx"] + 1
        rows.append((t["id"], year_id[t["year"]], t["term_no"], t["name"], t["start"], t["end"], epoch, epoch, False))
    writer.write(s.term, ("term_id", "academic_year_id", "term_no", "name", "start_date", "end_date") + AUDIT, rows)
    started_terms = [t for t in terms if t["start"] <= until]

    fee_id = {}
    rows = []
    for i, (code, name, refundable, _amount, _who) in enumerate(FEE_COMPONENTS, start=1):
        fee_id[code] = i
        rows.append((i, code, name, refundable, epoch, epoch, False))
    writer.write(s.fee_component, ("fee_component_id", "code", "name", "is_refundable") + AUDIT, rows)
    step("reference data")

    # ------------------------------------------------------------------ courses and curriculum
    courses = {}                       # course_id -> dict
    curriculum = defaultdict(list)     # (program_short, semester) -> [course_id]
    pc_rows = []

    def add_course(code, title, ctype, dept):
        cid = len(courses) + 1
        credits = 2 if ctype == "lab" else rng.choice([3, 4, 4])
        courses[cid] = dict(id=cid, code=code, title=title, type=ctype, dept=dept,
                            credits=credits, max_marks=50 if ctype == "lab" else 100)
        return cid

    for sem, subjects in BTECH_COMMON.items():
        for n, (title, ctype) in enumerate(subjects, start=1):
            cid = add_course(f"BTC{sem}{n:02d}", title, ctype, COMMON_COURSE_DEPT[title])
            for short in BTECH:
                curriculum[(short, sem)].append(cid)

    for short, p in PROGRAMS.items():
        first_sem = 3 if short in BTECH else 1
        subjects = iter(THEORY[short])
        per_sem = 5 if short == "MBA" else 4
        for sem in range(first_sem, p["terms"] + 1):
            for n in range(1, per_sem + 1):
                cid = add_course(f"{short}{sem}{n:02d}", next(subjects), "theory", p["dept"])
                curriculum[(short, sem)].append(cid)
            if short in LAB:
                cid = add_course(f"{short}{sem}L1", f"{LAB[short]} {sem}", "lab", p["dept"])
                curriculum[(short, sem)].append(cid)

    rows = [(c["id"], dept_id[c["dept"]], c["code"], c["title"], c["credits"], c["type"], c["max_marks"],
             epoch, epoch, False) for c in courses.values()]
    writer.write(s.course, ("course_id", "department_id", "course_code", "title", "credits", "course_type",
                            "max_marks") + AUDIT, rows)

    for (short, sem), cids in curriculum.items():
        for n, cid in enumerate(cids):
            elective = sem >= 5 and n == 3      # the 4th subject from semester 5 is an elective
            pc_rows.append((len(pc_rows) + 1, program_id[short], cid, sem, elective, epoch, epoch, False))
    writer.write(s.program_course, ("program_course_id", "program_id", "course_id", "semester_no",
                                    "is_elective") + AUDIT, pc_rows)
    step("courses and curriculum")

    # ------------------------------------------------------------------ students
    students = {}                      # student_id -> dict
    cohorts = defaultdict(list)        # (program_short, admission_year) -> [student_id]
    city_choices = [(c, st) for c, st, _ in CITIES]
    city_weights = [w for _, _, w in CITIES]

    for adm_year in range(cfg.first_year, until.year + 1):
        adm_term = 2 * (adm_year - cfg.first_year)
        if terms[adm_term]["start"] > until:
            continue
        for short, p in PROGRAMS.items():
            size = max(5, round(p["intake"] * cfg.scale))
            for seq in range(1, size + 1):
                sid = len(students) + 1
                prof = student_profile(cfg.seed, sid)
                gender = rng.choices(["Male", "Female", "Other"], weights=[54, 45, 1])[0]
                first = fake.first_name_female() if gender == "Female" else fake.first_name_male()
                last = fake.last_name()
                city, state = rng.choices(city_choices, weights=city_weights)[0]
                age = 18 if p["level"] == "UG" else 22
                dob = date(adm_year - age, 1, 1) + timedelta(days=rng.randint(-200, 500))

                # Will this student drop out, and when?
                drop_rng = random.Random(f"drop-{cfg.seed}-{sid}")
                drop_term = drop_date = None
                if drop_rng.random() < prof.drop_probability:
                    drop_idx = adm_term + drop_rng.randint(0, p["terms"] - 1)
                    if drop_idx < len(terms):                   # later terms are still in the future
                        t = terms[drop_idx]
                        candidate = t["start"] + timedelta(days=drop_rng.randint(20, 100))
                        if candidate <= until:
                            drop_term, drop_date = t["idx"], candidate

                last_term = adm_term + p["terms"] - 1
                graduation = (terms[last_term]["end_end"] + timedelta(days=14)
                              if last_term < len(terms) else None)
                created = at(date(adm_year, 7, 1) + timedelta(days=rng.randint(0, 10)), rng.randint(10, 17))

                if drop_date:
                    status, updated = "dropped", at(drop_date, 15)
                elif graduation and graduation <= until:
                    status, updated = "graduated", at(graduation, 12)
                elif rng.random() < 0.01:
                    status, updated = "inactive", at(until - timedelta(days=rng.randint(5, 60)), 11)
                else:
                    status, updated = "active", created

                students[sid] = dict(
                    id=sid, program=short, adm_year=adm_year, adm_term=adm_term,
                    section=(seq - 1) // cfg.section_size, profile=prof,
                    first=first, last=last, gender=gender, dob=dob, city=city, state=state,
                    roll_no=f"NIT{adm_year % 100:02d}{short}{seq:04d}",
                    email=f"{first}.{last}{seq}@student.northfield.edu.in".lower().replace(" ", ""),
                    phone=indian_phone(rng), status=status, created=created, updated=updated,
                    drop_term=drop_term, drop_date=drop_date,
                )
                cohorts[(short, adm_year)].append(sid)

    # Deliberate data-quality problems (the pipeline must catch these later)
    real_ids = list(students)
    for sid in real_ids:
        st = students[sid]
        if rng.random() < cfg.bad_data_rate * 2:
            st["email"] = None                                   # missing email
        if rng.random() < cfg.bad_data_rate:
            st["phone"] = invalid_phone(rng)                     # invalid phone format
        if rng.random() < cfg.bad_data_rate:
            st["city"] = rng.choice([f"  {st['city'].lower()}", f"{st['city'].upper()} "])  # messy text
    duplicates = 0
    for sid in rng.sample(real_ids, max(1, int(len(real_ids) * cfg.bad_data_rate * 0.3))):
        # Same person entered twice with a new roll number: a classic data-entry duplicate
        original = students[sid]
        dup_id = len(students) + 1
        students[dup_id] = dict(original, id=dup_id, roll_no=original["roll_no"] + "D",
                                created=original["created"] + timedelta(days=rng.randint(1, 20)),
                                drop_term=None, drop_date=None, duplicate_of=sid)
        students[dup_id]["updated"] = students[dup_id]["created"]
        duplicates += 1

    writer.write(s.student, ("student_id", "first_name", "last_name", "roll_no", "program_id", "admission_year",
                             "email", "city", "state", "gender", "date_of_birth", "phone", "status") + AUDIT,
                 ((st["id"], st["first"], st["last"], st["roll_no"], program_id[st["program"]], st["adm_year"],
                   st["email"], st["city"], st["state"], st["gender"], st["dob"], st["phone"], st["status"],
                   st["created"], st["updated"], False) for st in students.values()))
    step(f"students (+{duplicates} duplicates)")

    # ------------------------------------------------------------------ enrollments
    enrollments = []                   # one per student per term they study in
    enrollment_of = {}                 # (student_id, term_idx) -> enrollment dict
    current_idx = started_terms[-1]["idx"]
    for (short, adm_year), sids in cohorts.items():
        for sid in sids:
            st = students[sid]
            for t in started_terms:
                sem = t["idx"] - st["adm_term"] + 1
                if not 1 <= sem <= PROGRAMS[short]["terms"]:
                    continue
                if st["drop_term"] is not None and t["idx"] > st["drop_term"]:
                    break
                if t["idx"] == st["drop_term"]:
                    status, updated = "withdrawn", at(st["drop_date"], 15)
                elif t["idx"] == current_idx:
                    status, updated = "registered", None
                else:
                    status, updated = "completed", at(t["end_end"] + timedelta(days=14), 12)
                created = at(t["start"] - timedelta(days=5), 10)
                e = dict(id=len(enrollments) + 1, student=sid, term=t["idx"], sem=sem, status=status,
                         created=created, updated=updated or created)
                enrollments.append(e)
                enrollment_of[(sid, t["idx"])] = e
    writer.write(s.enrollment, ("enrollment_id", "student_id", "term_id", "semester_no", "status") + AUDIT,
                 ((e["id"], e["student"], terms[e["term"]]["id"], e["sem"], e["status"],
                   e["created"], e["updated"], False) for e in enrollments))
    step("enrollments")

    # ------------------------------------------------------------------ course offerings (faculty assigned below)
    offerings = {}                     # (course_id, term_idx, program, adm_year, section) -> offering dict
    letters_used = defaultdict(int)    # (course_id, term_idx) -> sections handed out so far
    for (short, adm_year), sids in cohorts.items():
        n_sections = math.ceil(len(sids) / cfg.section_size)
        for t in started_terms:
            sem = t["idx"] - 2 * (adm_year - cfg.first_year) + 1
            if not 1 <= sem <= PROGRAMS[short]["terms"]:
                continue
            for cid in curriculum[(short, sem)]:
                for sec in range(n_sections):
                    n = letters_used[(cid, t["idx"])]
                    letters_used[(cid, t["idx"])] += 1
                    offerings[(cid, t["idx"], short, adm_year, sec)] = dict(
                        id=len(offerings) + 1, course=cid, term=t["idx"], section=section_letter(n),
                        capacity=cfg.section_size + rng.choice([0, 0, 5]), faculty=None,
                    )

    # ------------------------------------------------------------------ faculty, sized to the teaching load
    load = defaultdict(int)            # (dept, term_idx) -> number of offerings
    for o in offerings.values():
        load[(courses[o["course"]]["dept"], o["term"])] += 1
    faculty = []
    for code, _ in DEPARTMENTS:
        peak = max([load[(code, t["idx"])] for t in started_terms] + [1])
        for _ in range(math.ceil(peak / 4 * 1.25)):
            fid = len(faculty) + 1
            early = rng.random() < 0.8
            joined = (date(rng.randint(2008, 2021), rng.randint(1, 12), 1) if early
                      else date(rng.randint(2022, 2025), rng.randint(1, 12), 1))
            status = "active"
            if early and rng.random() < 0.05:
                status = "exited"
            elif rng.random() < 0.03:
                status = "on_leave"
            first, last = fake.first_name(), fake.last_name()
            updated = at(joined, 10)
            if status == "exited":
                updated = at(FACULTY_EXIT_CUTOFF - timedelta(days=1), 17)
            elif status == "on_leave":
                updated = at(until - timedelta(days=rng.randint(10, 50)), 11)
            email = (f"{first}.{last}{fid}@northfield.edu.in".lower().replace(" ", "")
                     if rng.random() > 0.03 else None)
            faculty.append(dict(id=fid, dept=code, first=first, last=last, code=f"FAC{1000 + fid}",
                                designation=rng.choices([d for d, _ in DESIGNATIONS],
                                                        weights=[w for _, w in DESIGNATIONS])[0],
                                email=email, joined=joined, status=status, created=at(joined, 10),
                                updated=updated))

    def can_teach(f, t):
        if f["joined"] > t["start"]:
            return False
        if f["status"] == "exited" and t["start"] >= FACULTY_EXIT_CUTOFF:
            return False
        if f["status"] == "on_leave" and t["idx"] == current_idx:
            return False
        return True

    faculty_load = defaultdict(int)
    for o in offerings.values():
        t = terms[o["term"]]
        dept = courses[o["course"]]["dept"]
        pool = [f for f in faculty if f["dept"] == dept and can_teach(f, t)] or \
               [f for f in faculty if f["dept"] == dept]
        chosen = min(pool, key=lambda f: (faculty_load[(f["id"], t["idx"])], f["id"]))
        faculty_load[(chosen["id"], t["idx"])] += 1
        o["faculty"] = chosen["id"]

    writer.write(s.faculty, ("faculty_id", "first_name", "last_name", "faculty_code", "department_id",
                             "designation", "email", "joined_on", "status") + AUDIT,
                 ((f["id"], f["first"], f["last"], f["code"], dept_id[f["dept"]], f["designation"], f["email"],
                   f["joined"], f["status"], f["created"], f["updated"], False) for f in faculty))
    writer.write(s.course_offering, ("offering_id", "course_id", "term_id", "faculty_id", "section",
                                     "capacity") + AUDIT,
                 ((o["id"], o["course"], terms[o["term"]]["id"], o["faculty"], o["section"], o["capacity"],
                   at(terms[o["term"]]["start"] - timedelta(days=30), 10),
                   at(terms[o["term"]]["start"] - timedelta(days=30), 10), False) for o in offerings.values()))
    step(f"faculty ({len(faculty)}) and offerings")

    # ------------------------------------------------------------------ course registrations
    registrations = {}                 # registration_id -> dict
    regs_by_offering = defaultdict(list)
    for (cid, tidx, short, adm_year, sec), o in offerings.items():
        t = terms[tidx]
        for sid in cohorts[(short, adm_year)]:
            st = students[sid]
            if st["section"] != sec or (sid, tidx) not in enrollment_of:
                continue
            dropped = st["drop_term"] == tidx
            registered_on = at(t["start"] - timedelta(days=rng.randint(0, 7)), rng.randint(9, 18), rng.randint(0, 59))
            r = dict(id=len(registrations) + 1, student=sid, offering=o["id"], course=cid, term=tidx,
                     status="dropped" if dropped else "registered", registered_on=registered_on,
                     updated=at(st["drop_date"], 15) if dropped else registered_on, present=0, held=0)
            registrations[r["id"]] = r
            regs_by_offering[o["id"]].append(r)
    writer.write(s.course_registration, ("registration_id", "student_id", "offering_id", "registered_on",
                                         "status") + AUDIT,
                 ((r["id"], r["student"], r["offering"], r["registered_on"], r["status"], r["registered_on"],
                   r["updated"], False) for r in registrations.values()))
    step("course registrations")

    # ------------------------------------------------------------------ class sessions (timetable)
    sessions = []                      # (session_id, offering_id, date, slot, status)
    for o in offerings.values():
        t = terms[o["term"]]
        srng = random.Random(f"timetable-{cfg.seed}-{o['id']}")
        if courses[o["course"]]["type"] == "lab":
            weekdays, slot = {srng.randint(0, 4)}, srng.randint(5, 8)
        else:
            weekdays, slot = set(srng.sample(range(5), 2)), srng.randint(1, 6)
        day = t["start"]
        while day <= t["end"]:
            if day.weekday() in weekdays and not (t["mid_start"] <= day <= t["mid_end"]):
                if day <= until:
                    status = "cancelled" if srng.random() < 0.03 else "held"
                else:
                    status = "scheduled"
                sessions.append((len(sessions) + 1, o["id"], day, slot, status))
            day += timedelta(days=1)

    def session_rows():
        for sess_id, off_id, day, slot, status in sessions:
            o_term = terms[offering_term[off_id]]
            created = at(o_term["start"] - timedelta(days=10), 10)
            updated = at(day, 8 + slot, 55) if status != "scheduled" else created
            yield (sess_id, off_id, day, slot, status, created, updated, False)

    offering_term = {o["id"]: o["term"] for o in offerings.values()}
    writer.write(s.class_session, ("session_id", "offering_id", "session_date", "slot_no", "status") + AUDIT,
                 session_rows())
    step(f"class sessions ({len(sessions):,})")

    # ------------------------------------------------------------------ exams
    exams = {}                         # (term_idx, exam_type) -> dict
    for t in started_terms:
        plan = [("midterm", t["mid_start"], t["mid_end"]), ("endterm", t["end_start"], t["end_end"])]
        if t["end_end"] + timedelta(days=14) <= until:
            plan.append(("supplementary", t["supp_start"], t["supp_end"]))
        for exam_type, start, end in plan:
            exams[(t["idx"], exam_type)] = dict(id=len(exams) + 1, term=t["idx"], type=exam_type,
                                                start=start, end=end)
    writer.write(s.exam, ("exam_id", "term_id", "exam_type", "start_date", "end_date") + AUDIT,
                 ((e["id"], terms[e["term"]]["id"], e["type"], e["start"], e["end"],
                   at(terms[e["term"]]["start"], 10), at(terms[e["term"]]["start"], 10), False)
                  for e in exams.values()))

    # ------------------------------------------------------------------ attendance (streamed: millions of rows)
    all_student_ids = [sid for sid in students if "duplicate_of" not in students[sid]]
    orphan_count = 0

    def attendance_rows():
        nonlocal orphan_count
        aid = 0
        arng = random.Random(f"attendance-{cfg.seed}")
        for sess_id, off_id, day, slot, status in sessions:
            if status != "held":
                continue
            t = terms[offering_term[off_id]]
            late_in_term = (t["end"] - day).days < 28
            registered = set()
            for r in regs_by_offering[off_id]:
                st = students[r["student"]]
                registered.add(r["student"])
                if st["drop_date"] and day > st["drop_date"]:
                    continue
                prof = st["profile"]
                rate = prof.attendance_rate - (0.06 if late_in_term and prof.kind != "regular" else 0)
                state = attendance_status(arng, rate)
                marked = at(day, 8 + slot, arng.randint(5, 50))
                updated = marked
                if state == "absent" and arng.random() < 0.01:          # teacher corrects it later
                    fix = marked + timedelta(days=arng.randint(1, 3), hours=arng.randint(1, 5))
                    if fix.date() <= until:
                        state, updated = "present", fix
                r["held"] += 1
                r["present"] += state in ("present", "late")
                aid += 1
                yield (aid, sess_id, r["student"], state, marked, marked, updated, False)
            if arng.random() < cfg.bad_data_rate * 2:
                # Attendance for a student who is not registered in this class (DQ: referential check)
                stranger = arng.choice(all_student_ids)
                if stranger not in registered:
                    aid += 1
                    orphan_count += 1
                    marked = at(day, 8 + slot, 30)
                    yield (aid, sess_id, stranger, "present", marked, marked, marked, False)

    n_att = writer.write(s.attendance, ("attendance_id", "session_id", "student_id", "status",
                                        "marked_at") + AUDIT, attendance_rows())
    step(f"attendance ({n_att:,}, {orphan_count} orphans)")

    # ------------------------------------------------------------------ exam results
    results = []
    erng = random.Random(f"results-{cfg.seed}")

    def score(r, exam_type, bonus=0.0):
        st = students[r["student"]]
        prof = st["profile"]
        att = r["present"] / r["held"] if r["held"] else prof.attendance_rate
        absent_p = {"regular": 0.015, "irregular": 0.03, "at_risk": 0.08}[prof.kind] + (0.05 if att < 0.5 else 0)
        course = courses[r["course"]]
        max_marks = (20 if course["type"] == "lab" else 30) if exam_type == "midterm" else course["max_marks"]
        if erng.random() < absent_p:
            return None, max_marks, "AB", "absent"
        pct = min(1.0, max(0.0, prof.ability + bonus + 0.35 * (att - 0.75) + erng.gauss(0, 0.10)))
        marks = round(pct * max_marks * 2) / 2
        if erng.random() < cfg.bad_data_rate * 0.2:
            return max_marks + erng.randint(1, 15), max_marks, "O", "pass"   # DQ: marks above maximum
        if pct >= cfg.pass_percent:
            return marks, max_marks, grade_for(pct), "pass"
        return marks, max_marks, "F", "fail"

    for (tidx, exam_type), e in exams.items():
        published = e["end"] + timedelta(days=14)
        if published > until or exam_type == "supplementary":
            continue
        stamp = at(published, 16)
        for r in registrations.values():
            if r["term"] != tidx or r["status"] != "registered":
                continue
            marks, max_marks, grade, status = score(r, exam_type)
            results.append((len(results) + 1, e["id"], r["id"], marks, max_marks, grade, status, stamp, stamp, False))
            if exam_type == "endterm" and status in ("fail", "absent"):
                supp = exams.get((tidx, "supplementary"))
                if supp and supp["end"] + timedelta(days=14) <= until and erng.random() < 0.8:
                    s_marks, s_max, s_grade, s_status = score(r, "supplementary", bonus=0.12)
                    s_stamp = at(supp["end"] + timedelta(days=14), 16)
                    results.append((len(results) + 1, supp["id"], r["id"], s_marks, s_max, s_grade, s_status,
                                    s_stamp, s_stamp, False))
    writer.write(s.exam_result, ("result_id", "exam_id", "registration_id", "marks_obtained", "max_marks",
                                 "grade", "result_status") + AUDIT, results)
    step(f"exam results ({len(results):,})")

    # ------------------------------------------------------------------ fee invoices and payments
    invoices, payments = [], []
    frng = random.Random(f"fees-{cfg.seed}")
    for e in enrollments:
        st = students[e["student"]]
        prof = st["profile"]
        t = terms[e["term"]]
        growth = 1.05 ** (t["year"] - cfg.first_year)
        issue, due = t["start"] - timedelta(days=20), t["start"] + timedelta(days=15)
        for code, _name, refundable, amount, who in FEE_COMPONENTS:
            if (who == "hostel" and not prof.hostel) or (who == "transport" and not prof.transport):
                continue
            base = PROGRAMS[st["program"]]["tuition"] if code == "TUITION" else amount
            total = round(base * growth / 100) * 100
            inv_id = len(invoices) + 1
            plan = _plan_payments(frng, prof.fee_behaviour, total, issue, due)
            stop = min(until, st["drop_date"]) if st["drop_date"] else until
            paid = 0
            last_change = at(issue, 10)
            for amount_paid, when in plan:
                if when.date() > stop:
                    break
                if frng.random() < 0.05:            # failed attempt first, then a successful retry
                    payments.append(dict(invoice=inv_id, amount=amount_paid, mode=payment_mode(frng),
                                         status="failed", at=when - timedelta(minutes=frng.randint(2, 30))))
                payments.append(dict(invoice=inv_id, amount=amount_paid, mode=payment_mode(frng),
                                     status="success", at=when))
                paid += amount_paid
                last_change = when
            status = "paid" if paid >= total else "partially_paid" if paid > 0 else "open"
            if e["status"] == "withdrawn":
                refund_day = st["drop_date"] + timedelta(days=10)
                if refundable and status == "paid" and refund_day <= until:
                    for p in payments[::-1]:
                        if p["invoice"] != inv_id:
                            break
                        if p["status"] == "success":
                            p["status"], p["updated"] = "refunded", at(refund_day, 12)
                    status, last_change = "cancelled", at(refund_day, 12)
                elif status != "paid":
                    status, last_change = "cancelled", at(st["drop_date"], 16)
            invoices.append((inv_id, f"INV{t['start'].year}{t['term_no']}{inv_id:07d}", e["id"], fee_id[code],
                             issue, due, total, status, at(issue, 10), last_change, False))

    writer.write(s.fee_invoice, ("invoice_id", "invoice_no", "enrollment_id", "fee_component_id", "issue_date",
                                 "due_date", "amount", "status") + AUDIT, invoices)

    def payment_rows():
        for pid, p in enumerate(payments, start=1):
            amount = p["amount"]
            if p["status"] == "success" and frng.random() < cfg.bad_data_rate * 0.5:
                amount = rng.choice([-amount, 0])               # DQ: negative or zero amount
            yield (pid, f"PAY{p['at']:%Y%m%d}{pid:07d}", p["invoice"], amount, p["mode"], p["status"],
                   p["at"], p["at"], p.get("updated", p["at"]), False)

    writer.write(s.payment, ("payment_id", "payment_ref", "invoice_id", "amount", "mode", "status",
                             "paid_at") + AUDIT, payment_rows())
    step("fee invoices and payments")

    sync_identity(engine, s.SOURCE_TABLES)
    print(f"\nSeed complete in {clock.time() - started:.0f}s (data up to {until})")
    width = max(len(n) for n in writer.counts)
    for name in [t.name for t in s.SOURCE_TABLES]:
        print(f"  {name:<{width}} {writer.counts.get(name, 0):>12,}")


def _plan_payments(rng, behaviour, total, issue, due):
    """Planned (amount, datetime) payments for one invoice, before any cut-off is applied."""
    def moment(day):
        return datetime.combine(day, datetime.min.time()) + timedelta(hours=rng.randint(9, 21),
                                                                      minutes=rng.randint(0, 59))
    if behaviour == "on_time":
        return [(total, moment(issue + timedelta(days=rng.randint(0, (due - issue).days))))]
    if behaviour == "late":
        first_day = due + timedelta(days=rng.randint(5, 75))
        if rng.random() < 0.35:
            half = round(total / 2 / 100) * 100
            return [(half, moment(first_day)), (total - half, moment(first_day + timedelta(days=rng.randint(15, 45))))]
        return [(total, moment(first_day))]
    if rng.random() < 0.5:                                   # defaulter: partial payment or nothing
        part = round(total * rng.uniform(0.4, 0.7) / 100) * 100
        return [(part, moment(due + timedelta(days=rng.randint(20, 90))))]
    return []


def _require_empty(engine):
    with engine.connect() as conn:
        if conn.execute(select(func.count()).select_from(s.student)).scalar():
            raise SystemExit("The source database already has data. Start fresh with:\n"
                             "  python -m src.source_db.create_schema --reset")
