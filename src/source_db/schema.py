from sqlalchemy import (
    BigInteger, Index, MetaData, Numeric, Table, Column,
    Integer, SmallInteger, String, Boolean, Date, DateTime,
    Identity, ForeignKey, UniqueConstraint, CheckConstraint,
    func, false, text, true,
)

metadata = MetaData()

MONEY = Numeric(12, 2)  # Custom type for money values

def audit_columns():
    """The three columns every source table gets."""
    return [
        Column("created_at", DateTime, nullable=False, server_default=func.now()),
        Column("updated_at", DateTime, nullable=False, server_default=func.now()),
        Column("is_deleted", Boolean, nullable=False, server_default=false()),
    ]


department = Table(
    "department", metadata,
    Column("department_id", Integer, Identity(), primary_key=True),
    Column("dept_code", String(10), nullable=False, unique=True),
    Column("name", String(100), nullable=False),
    *audit_columns(),
)

program = Table(
    "program", metadata,
    Column("program_id", Integer, Identity(), primary_key=True),
    Column("department_id", ForeignKey("department.department_id"), nullable=False),
    Column("program_code", String(20), nullable=False, unique=True),
    Column("name", String(150), nullable=False),
    Column("degree_level", String(10), nullable=False),
    Column("duration_terms", SmallInteger, nullable=False),
    *audit_columns(),
    CheckConstraint("degree_level IN ('UG', 'PG')", name="ck_program_degree_level"),
)

academic_year = Table(
    "academic_year", metadata,
    Column("academic_year_id", Integer, Identity(), primary_key=True),
    Column("label", String(10), nullable=False, unique=True),
    Column("start_date", Date, nullable=False),
    Column("end_date", Date, nullable=False),
    *audit_columns(),
    CheckConstraint("end_date > start_date", name="ck_academic_year_dates"),
)

term = Table(
    "term", metadata,
    Column("term_id", Integer, Identity(), primary_key=True),
    Column("academic_year_id", ForeignKey("academic_year.academic_year_id"), nullable=False),
    Column("term_no", SmallInteger, nullable=False),
    Column("name", String(30), nullable=False),
    Column("start_date", Date, nullable=False),
    Column("end_date", Date, nullable=False),
    *audit_columns(),
    UniqueConstraint("academic_year_id", "term_no", name="uq_term_year_no"),
    CheckConstraint("term_no > 0", name="ck_term_no_positive"),
    CheckConstraint("end_date > start_date", name="ck_term_dates"),
)

course = Table(
    "course", metadata,
    Column("course_id", Integer, Identity(), primary_key=True),
    Column("department_id", ForeignKey("department.department_id"), nullable=False),
    Column("course_code", String(20), nullable=False, unique=True),
    Column("title", String(150), nullable=False),
    Column("credits", SmallInteger, nullable=False),
    Column("course_type", String(10), nullable=False),
    Column("max_marks", SmallInteger, nullable=False),
    *audit_columns(),
    CheckConstraint("course_type IN ('theory', 'lab')", name="ck_course_type")
)

program_course = Table(
    "program_course", metadata,
    Column("program_course_id", Integer, Identity(), primary_key=True),
    Column("program_id", ForeignKey("program.program_id"), nullable=False),
    Column("course_id", ForeignKey("course.course_id"), nullable=False),
    Column("semester_no", SmallInteger, nullable=False),
    Column("is_elective", Boolean, nullable=False, server_default=false()),
    *audit_columns(),
    UniqueConstraint("program_id", "course_id", name="uq_program_course"),
)

faculty = Table(
    "faculty", metadata,
    Column("faculty_id", Integer, Identity(), primary_key=True),
    Column("first_name", String(100), nullable=False),
    Column("last_name", String(100), nullable=False),
    Column("faculty_code", String(20), nullable=False, unique=True),
    Column("department_id", ForeignKey("department.department_id"), nullable=False),
    Column("designation", String(50), nullable=False),
    Column("email", String(100), nullable=True),
    Column("joined_on", Date, nullable=False),
    Column("status", String(20), nullable=False),
    *audit_columns(),
    CheckConstraint("status IN ('active', 'on_leave', 'exited')", name="ck_faculty_status")
)

student = Table(
    "student", metadata,
    Column("student_id", Integer, Identity(), primary_key=True),
    Column("first_name", String(100), nullable=False),
    Column("last_name", String(100), nullable=False),
    Column("roll_no", String(20), nullable=False, unique=True),
    Column("program_id", ForeignKey("program.program_id"), nullable=False),
    Column("admission_year", SmallInteger, nullable=False),
    Column("email", String(100)),
    Column("city", String(50), nullable=True),
    Column("state", String(50), nullable=True),
    Column("gender", String(10), nullable=True),
    Column("date_of_birth", Date, nullable=True),
    Column("phone", String(20), nullable=True),
    Column("status", String(20), nullable=False),
    *audit_columns(),
    CheckConstraint("status IN ('active', 'inactive', 'graduated', 'dropped')", name="ck_student_status")
)

enrollment = Table(
    "enrollment", metadata,
    Column("enrollment_id", Integer, Identity(), primary_key=True),
    Column("student_id", ForeignKey("student.student_id"), nullable=False),
    Column("term_id", ForeignKey("term.term_id"), nullable=False),
    Column("semester_no", SmallInteger, nullable=False),
    Column("status", String(20), nullable=False),
    *audit_columns(),
    UniqueConstraint("student_id", "term_id", name="uq_enrollment_student_term"),
    CheckConstraint("status IN ('registered', 'completed', 'withdrawn')", name="ck_enrollment_status")
)

course_offering = Table(
    "course_offering", metadata,
    Column("offering_id", Integer, Identity(), primary_key=True),
    Column("course_id", ForeignKey("course.course_id"), nullable=False),
    Column("term_id", ForeignKey("term.term_id"), nullable=False),
    Column("faculty_id", ForeignKey("faculty.faculty_id"), nullable=False),
    Column("section", String(5), nullable=False),
    Column("capacity", SmallInteger, nullable=False),
    *audit_columns(),
    UniqueConstraint("course_id", "term_id", "section", name="uq_course_offering_course_term_section")
)

course_registration = Table(
    "course_registration", metadata,
    Column("registration_id", Integer, Identity(), primary_key=True),
    Column("student_id", ForeignKey("student.student_id"), nullable=False),
    Column("offering_id", ForeignKey("course_offering.offering_id"), nullable=False),
    Column("registered_on", DateTime, nullable=False),
    Column("status", String(20), nullable=False),
    *audit_columns(),
    UniqueConstraint("student_id", "offering_id", name="uq_course_registration_student_offering"),
    CheckConstraint("status IN ('registered', 'dropped')", name="ck_course_registration_status")
)

class_session = Table(
    "class_session", metadata,
    Column("session_id", Integer, Identity(), primary_key=True),
    Column("offering_id", ForeignKey("course_offering.offering_id"), nullable=False),
    Column("session_date", Date, nullable=False),
    Column("slot_no", SmallInteger, nullable=False),
    Column("status", String(20), nullable=False),
    *audit_columns(),
    UniqueConstraint("offering_id", "session_date", "slot_no", name="uq_class_session_offering_date_slot"),
    CheckConstraint("status IN ('scheduled', 'held', 'cancelled')", name="ck_class_session_status")
)

attendance = Table(
    "attendance", metadata,
    Column("attendance_id", BigInteger, Identity(), primary_key=True),
    Column("session_id", ForeignKey("class_session.session_id"), nullable=False),
    Column("student_id", ForeignKey("student.student_id"), nullable=False),
    Column("status", String(20), nullable=False),
    Column("marked_at", DateTime, nullable=False),
    *audit_columns(),
    UniqueConstraint("session_id", "student_id", name="uq_attendance_session_student"),
    CheckConstraint("status IN ('present', 'absent','late', 'excused')", name="ck_attendance_status")

)

exam = Table(
    "exam", metadata,
    Column("exam_id", Integer, Identity(), primary_key=True),
    Column("term_id", ForeignKey("term.term_id"), nullable=False),
    Column("exam_type", String(15), nullable=False),
    Column("start_date", Date, nullable=False),
    Column("end_date", Date, nullable=False),
    *audit_columns(),
    CheckConstraint("exam_type IN ('midterm', 'endterm', 'supplementary')", name="ck_exam_type"),
    CheckConstraint("end_date >= start_date", name="ck_exam_dates"),
    UniqueConstraint("term_id", "exam_type", name="uq_exam_term_type")
)

exam_result = Table(
    "exam_result", metadata,
    Column("result_id", BigInteger, Identity(), primary_key=True),
    Column("exam_id", ForeignKey("exam.exam_id"), nullable=False),
    Column("registration_id", ForeignKey("course_registration.registration_id"), nullable=False),
    Column("marks_obtained", Numeric(5, 2), nullable=True),
    Column("max_marks", SmallInteger, nullable=False),
    Column("grade", String(2), nullable=True),
    Column("result_status", String(10), nullable=False),
    *audit_columns(),
    UniqueConstraint("exam_id", "registration_id", name="uq_exam_result_exam_registration"),
    CheckConstraint("result_status IN ('pass', 'fail', 'absent')", name="ck_exam_result_status"),
    CheckConstraint(
        "result_status = 'absent' OR marks_obtained IS NOT NULL",
        name="ck_exam_result_marks_present",
    )
)

fee_component = Table(
    "fee_component", metadata,
    Column("fee_component_id", Integer, Identity(), primary_key=True),
    Column("code", String(15), nullable=False, unique=True),
    Column("name", String(60), nullable=False),
    Column("is_refundable", Boolean, nullable=False, server_default=false()),
    *audit_columns()
)

fee_invoice = Table(
    "fee_invoice", metadata,
    Column("invoice_id", Integer, Identity(), primary_key=True),
    Column("invoice_no", String(25), nullable=False, unique=True),
    Column("enrollment_id", ForeignKey("enrollment.enrollment_id"), nullable=False),
    Column("fee_component_id", ForeignKey("fee_component.fee_component_id"), nullable=False),
    Column("issue_date", Date, nullable=False),
    Column("due_date", Date, nullable=False),
    Column("amount", MONEY, nullable=False),
    Column("status", String(20), nullable=False),
    *audit_columns(),
    UniqueConstraint("enrollment_id", "fee_component_id", name="uq_fee_invoice_enrollment_component"),
    CheckConstraint("status IN ('open','partially_paid', 'paid', 'cancelled')", name="ck_fee_invoice_status")
)

payment = Table(
    "payment", metadata,
    Column("payment_id", Integer, Identity(), primary_key=True),
    Column("payment_ref", String(30), nullable=False, unique=True),
    Column("invoice_id", ForeignKey("fee_invoice.invoice_id"), nullable=False),
    Column("amount", MONEY, nullable=False),
    Column("mode", String(15), nullable=False),
    Column("status", String(10), nullable=False),
    Column("paid_at", DateTime, nullable=False),
    *audit_columns(),
    CheckConstraint("mode IN ('upi', 'card', 'netbanking', 'cash')", name="ck_payment_mode"),
    CheckConstraint("status IN ('success', 'failed', 'refunded')", name="ck_payment_status")
)

# ------------------------------------------------------------------
# Indexes
# ------------------------------------------------------------------

# Capture the 18 source tables NOW, before the control table is added to metadata
SOURCE_TABLES = list(metadata.sorted_tables)

# 1. An index on updated_at for every source table (drives the watermark query)
for table in SOURCE_TABLES:
    Index(f"ix_{table.name}_updated_at", table.c.updated_at)

# 2. Foreign-key indexes on the big tables (Postgres does not create these automatically)
Index("ix_attendance_student_id", attendance.c.student_id)
Index("ix_class_session_offering_id", class_session.c.offering_id)
Index("ix_course_registration_offering_id", course_registration.c.offering_id)
Index("ix_payment_invoice_id", payment.c.invoice_id)
# ------------------------------------------------------------------
# ETL control table (lives in the "etl" schema, not a source table)
# ------------------------------------------------------------------

ingestion_control = Table(
    "ingestion_control", metadata,
    Column("source_table", String(100), primary_key=True),
    Column("load_type", String(12), nullable=False),            # 'full' or 'incremental'
    Column("watermark_column", String(50), nullable=False, server_default="updated_at"),
    Column("primary_key", String(50), nullable=False),
    Column("last_watermark", DateTime, nullable=False, server_default=text("'1900-01-01'")),
    Column("is_active", Boolean, nullable=False, server_default=true()),
    Column("last_run_status", String(15), nullable=True),
    Column("last_run_at", DateTime, nullable=True),
    Column("rows_copied", BigInteger, nullable=True),
    CheckConstraint("load_type IN ('full', 'incremental')", name="ck_ingestion_control_load_type"),
    schema="etl",
)

# Small reference tables that are reloaded completely every run
FULL_LOAD_TABLES = {
    "department", "program", "academic_year", "term", "course", "fee_component",
}

if __name__ == "__main__":
    from sqlalchemy.schema import CreateTable
    from sqlalchemy.dialects import postgresql, mssql
    from sqlalchemy.schema import CreateIndex

    for table in metadata.sorted_tables:
        print(CreateTable(table).compile(dialect=postgresql.dialect()))

    for table in metadata.sorted_tables:
        for index in table.indexes:
            print(CreateIndex(index).compile(dialect=postgresql.dialect()))