"""
Delta mode: simulate ONE day of university activity after the seed.

This is what feeds the incremental pipeline. Each simulated day:
  - classes scheduled for that day are held (or cancelled) and attendance is marked
  - a few earlier absences are corrected by teachers           (UPDATE)
  - a few wrongly entered attendance rows are soft-deleted      (is_deleted = true)
  - students pay fees; invoice statuses change                  (INSERT + UPDATE)
  - some students move city or change phone                     (UPDATE, feeds SCD2)
  - a few at-risk students drop out                             (UPDATE across several tables)

Every row touched gets updated_at = that day, so the watermark picks it up.
Everything happens in one transaction: a day is either fully simulated or not at all.
"""

import random
from collections import defaultdict
from datetime import date, datetime, timedelta

from sqlalchemy import and_, bindparam, func, select

from src.generator.common import at, attendance_status, indian_phone, invalid_phone, payment_mode
from src.generator.profiles import student_profile
from src.generator.reference import CITIES
from src.generator.writer import next_id
from src.source_db import schema as s


def last_simulated_day(conn) -> date:
    """The latest day any activity happened. A new day must come after it."""
    latest = max(
        conn.execute(select(func.max(t.c.updated_at))).scalar() or datetime.min
        for t in (s.attendance, s.payment, s.student, s.class_session, s.fee_invoice)
    )
    return latest.date()


def run_delta(engine, cfg, day: date):
    rng = random.Random(f"delta-{cfg.seed}-{day.isoformat()}")
    stats = defaultdict(int)

    with engine.begin() as conn:
        last = last_simulated_day(conn)
        if day <= last:
            raise SystemExit(f"Cannot simulate {day}: data already exists up to {last}. "
                             f"Use a later date (default is the next day).")

        _hold_classes(conn, cfg, day, rng, stats)
        _correct_and_soft_delete_attendance(conn, day, rng, stats)
        _collect_payments(conn, cfg, day, rng, stats)
        _student_changes(conn, cfg, day, rng, stats)

    print(f"Simulated {day} ({day:%A})")
    for key in ("sessions_held", "sessions_cancelled", "attendance_inserted", "attendance_orphans",
                "attendance_corrected", "attendance_soft_deleted", "payments_success", "payments_failed",
                "invoices_updated", "students_moved", "phones_changed", "students_dropped"):
        print(f"  {key:<24} {stats[key]:>8,}")


# --------------------------------------------------------------------------- attendance
def _hold_classes(conn, cfg, day, rng, stats):
    todays = conn.execute(
        select(s.class_session.c.session_id, s.class_session.c.offering_id, s.class_session.c.slot_no)
        .where(s.class_session.c.session_date == day,
               s.class_session.c.status == "scheduled",
               s.class_session.c.is_deleted.is_(False))
        .order_by(s.class_session.c.session_id)
    ).all()
    if not todays:
        return

    offering_ids = sorted({row.offering_id for row in todays})
    roster = defaultdict(list)
    for reg in conn.execute(
        select(s.course_registration.c.offering_id, s.course_registration.c.student_id)
        .join(s.student, s.student.c.student_id == s.course_registration.c.student_id)
        .where(s.course_registration.c.offering_id.in_(offering_ids),
               s.course_registration.c.status == "registered",
               s.student.c.status == "active")
        .order_by(s.course_registration.c.offering_id, s.course_registration.c.student_id)
    ):
        roster[reg.offering_id].append(reg.student_id)
    active_ids = conn.execute(select(s.student.c.student_id).where(s.student.c.status == "active")
                              .order_by(s.student.c.student_id)).scalars().all()

    session_updates, rows = [], []
    aid = next_id(conn, s.attendance)
    for sess in todays:
        if rng.random() < 0.03:
            session_updates.append(dict(b_id=sess.session_id, b_status="cancelled", b_at=at(day, 8 + sess.slot_no, 55)))
            stats["sessions_cancelled"] += 1
            continue
        session_updates.append(dict(b_id=sess.session_id, b_status="held", b_at=at(day, 8 + sess.slot_no, 55)))
        stats["sessions_held"] += 1
        for sid in roster[sess.offering_id]:
            state = attendance_status(rng, student_profile(cfg.seed, sid).attendance_rate)
            marked = at(day, 8 + sess.slot_no, rng.randint(5, 50))
            rows.append(dict(attendance_id=aid, session_id=sess.session_id, student_id=sid, status=state,
                             marked_at=marked, created_at=marked, updated_at=marked, is_deleted=False))
            aid += 1
        if rng.random() < cfg.bad_data_rate * 2 and active_ids:
            stranger = rng.choice(active_ids)
            if stranger not in roster[sess.offering_id]:
                marked = at(day, 8 + sess.slot_no, 30)
                rows.append(dict(attendance_id=aid, session_id=sess.session_id, student_id=stranger,
                                 status="present", marked_at=marked, created_at=marked, updated_at=marked,
                                 is_deleted=False))
                aid += 1
                stats["attendance_orphans"] += 1

    t = s.class_session
    conn.execute(t.update().where(t.c.session_id == bindparam("b_id"))
                 .values(status=bindparam("b_status"), updated_at=bindparam("b_at")), session_updates)
    if rows:
        conn.execute(s.attendance.insert(), rows)
    stats["attendance_inserted"] = len(rows)


def _correct_and_soft_delete_attendance(conn, day, rng, stats):
    a = s.attendance
    recent = conn.execute(
        select(a.c.attendance_id, a.c.status)
        .where(a.c.marked_at >= datetime.combine(day - timedelta(days=3), datetime.min.time()),
               a.c.marked_at < datetime.combine(day, datetime.min.time()),
               a.c.is_deleted.is_(False))
        .order_by(a.c.attendance_id)
    ).all()
    corrections = [dict(b_id=r.attendance_id, b_at=at(day, rng.randint(10, 17), rng.randint(0, 59)))
                   for r in recent if r.status == "absent" and rng.random() < 0.01]
    deletions = [dict(b_id=r.attendance_id, b_at=at(day, rng.randint(10, 17), rng.randint(0, 59)))
                 for r in recent if rng.random() < 0.0005]
    if corrections:
        conn.execute(a.update().where(a.c.attendance_id == bindparam("b_id"))
                     .values(status="present", updated_at=bindparam("b_at")), corrections)
    if deletions:
        conn.execute(a.update().where(a.c.attendance_id == bindparam("b_id"))
                     .values(is_deleted=True, updated_at=bindparam("b_at")), deletions)
    stats["attendance_corrected"] = len(corrections)
    stats["attendance_soft_deleted"] = len(deletions)


# --------------------------------------------------------------------------- fees
def _collect_payments(conn, cfg, day, rng, stats):
    i, e, p = s.fee_invoice, s.enrollment, s.payment
    paid_so_far = (
        select(p.c.invoice_id, func.sum(p.c.amount).label("paid"))
        .where(p.c.status == "success", p.c.is_deleted.is_(False))
        .group_by(p.c.invoice_id).subquery()
    )
    open_invoices = conn.execute(
        select(i.c.invoice_id, i.c.amount, i.c.due_date, e.c.student_id,
               func.coalesce(paid_so_far.c.paid, 0).label("paid"))
        .join(e, e.c.enrollment_id == i.c.enrollment_id)
        .outerjoin(paid_so_far, paid_so_far.c.invoice_id == i.c.invoice_id)
        .where(i.c.status.in_(["open", "partially_paid"]), i.c.issue_date <= day, i.c.is_deleted.is_(False))
        .order_by(i.c.invoice_id)
    ).all()

    pid = next_id(conn, p)
    new_payments, invoice_updates = [], []
    for inv in open_invoices:
        behaviour = student_profile(cfg.seed, inv.student_id).fee_behaviour
        overdue = day > inv.due_date
        chance = {"on_time": 0.35 if overdue else 0.12,
                  "late": 0.04 if day > inv.due_date + timedelta(days=4) else 0.0,
                  "defaulter": 0.004}[behaviour]
        if rng.random() >= chance:
            continue
        remaining = float(inv.amount) - float(inv.paid)
        amount = remaining
        if behaviour == "late" and inv.paid == 0 and rng.random() < 0.35:
            amount = round(remaining / 2 / 100) * 100
        if behaviour == "defaulter":
            amount = round(remaining * rng.uniform(0.3, 0.6) / 100) * 100
        if amount <= 0:
            continue
        when = at(day, rng.randint(9, 21), rng.randint(0, 59))
        if rng.random() < 0.05:
            failed_at = when - timedelta(minutes=rng.randint(2, 30))
            new_payments.append(_payment(pid, inv.invoice_id, amount, payment_mode(rng), "failed", failed_at))
            pid += 1
            stats["payments_failed"] += 1
        recorded = amount
        if rng.random() < cfg.bad_data_rate * 0.5:
            recorded = rng.choice([-amount, 0])                     # DQ: negative or zero amount
        new_payments.append(_payment(pid, inv.invoice_id, recorded, payment_mode(rng), "success", when))
        pid += 1
        stats["payments_success"] += 1
        new_status = "paid" if amount >= remaining else "partially_paid"
        invoice_updates.append(dict(b_id=inv.invoice_id, b_status=new_status, b_at=when))

    if new_payments:
        conn.execute(p.insert(), new_payments)
    if invoice_updates:
        conn.execute(i.update().where(i.c.invoice_id == bindparam("b_id"))
                     .values(status=bindparam("b_status"), updated_at=bindparam("b_at")), invoice_updates)
    stats["invoices_updated"] = len(invoice_updates)


def _payment(pid, invoice_id, amount, mode, status, when):
    return dict(payment_id=pid, payment_ref=f"PAY{when:%Y%m%d}{pid:07d}", invoice_id=invoice_id, amount=amount,
                mode=mode, status=status, paid_at=when, created_at=when, updated_at=when, is_deleted=False)


# --------------------------------------------------------------------------- students
def _student_changes(conn, cfg, day, rng, stats):
    st = s.student
    active = conn.execute(select(st.c.student_id, st.c.city).where(st.c.status == "active")
                          .order_by(st.c.student_id)).all()
    cities = [(c, state) for c, state, _ in CITIES]
    moves, phones, drops = [], [], []
    for row in active:
        roll = rng.random()
        when = at(day, rng.randint(9, 18), rng.randint(0, 59))
        if roll < 0.0015:
            city, state = rng.choice([c for c in cities if c[0] != row.city])
            moves.append(dict(b_id=row.student_id, b_city=city, b_state=state, b_at=when))
        elif roll < 0.0020:
            phone = invalid_phone(rng) if rng.random() < cfg.bad_data_rate * 5 else indian_phone(rng)
            phones.append(dict(b_id=row.student_id, b_phone=phone, b_at=when))
        elif roll < 0.0030 and student_profile(cfg.seed, row.student_id).kind == "at_risk":
            drops.append(dict(b_id=row.student_id, b_at=when))

    if moves:
        conn.execute(st.update().where(st.c.student_id == bindparam("b_id"))
                     .values(city=bindparam("b_city"), state=bindparam("b_state"), updated_at=bindparam("b_at")),
                     moves)
    if phones:
        conn.execute(st.update().where(st.c.student_id == bindparam("b_id"))
                     .values(phone=bindparam("b_phone"), updated_at=bindparam("b_at")), phones)
    for d in drops:
        _drop_out(conn, d["b_id"], d["b_at"])
    stats["students_moved"], stats["phones_changed"], stats["students_dropped"] = len(moves), len(phones), len(drops)


def _drop_out(conn, student_id, when):
    """A dropout touches four tables, all in the same transaction."""
    st, e, r, i, o = s.student, s.enrollment, s.course_registration, s.fee_invoice, s.course_offering
    conn.execute(st.update().where(st.c.student_id == student_id).values(status="dropped", updated_at=when))
    current = conn.execute(select(e.c.enrollment_id, e.c.term_id).where(e.c.student_id == student_id,
                                                                        e.c.status == "registered")).all()
    if not current:
        return
    enrollment_ids = [row.enrollment_id for row in current]
    term_ids = [row.term_id for row in current]
    conn.execute(e.update().where(e.c.enrollment_id.in_(enrollment_ids)).values(status="withdrawn", updated_at=when))
    # only this term's course registrations are dropped; past terms stay as they were
    this_term_offerings = select(o.c.offering_id).where(o.c.term_id.in_(term_ids))
    conn.execute(r.update().where(and_(r.c.student_id == student_id, r.c.status == "registered",
                                       r.c.offering_id.in_(this_term_offerings)))
                 .values(status="dropped", updated_at=when))
    current = enrollment_ids
    conn.execute(i.update().where(and_(i.c.enrollment_id.in_(current), i.c.status.in_(["open", "partially_paid"])))
                 .values(status="cancelled", updated_at=when))
