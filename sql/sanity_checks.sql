-- Row count of every table
CREATE TEMP TABLE IF NOT EXISTS tmp_counts (
    source_table TEXT, 
    row_count BIGINT
);
TRUNCATE tmp_counts;

DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN SELECT source_table FROM etl.ingestion_control LOOP
        EXECUTE format('INSERT INTO tmp_counts SELECT %L, COUNT(*) FROM %I', r.source_table, r.source_table);
    END LOOP;
END $$;

-- View your results
SELECT * FROM tmp_counts;


-- Student count by status
SELECT status, COUNT(*) AS student_count
FROM student
GROUP BY status
ORDER BY student_count DESC;

-- Overall attendance %, counting present + late as attended
SELECT 
    (COUNT(CASE WHEN status IN ('present', 'late') THEN 1 END) * 100.0 / COUNT(*)) AS attendance_percentage
FROM attendance;

-- Pass, fail and absent counts per exam_type
SELECT 
    e.exam_type,
    COUNT(CASE WHEN er.result_status = 'pass' THEN 1 END) AS pass_count,
    COUNT(CASE WHEN er.result_status = 'fail' THEN 1 END) AS fail_count,
    COUNT(CASE WHEN er.result_status = 'absent' THEN 1 END) AS absent_count
FROM exam_result er
JOIN exam e ON er.exam_id = e.exam_id
GROUP BY exam_type
ORDER BY exam_type;

-- Invoice count and total amount by status
SELECT
    status,
    COUNT(*) as invoice_count,
    SUM(amount) as total_amount
FROM fee_invoice
GROUP BY status
ORDER BY invoice_count DESC;

-- Data quality hunt: count students with no email, 
-- phones that aren’t exactly 10 digits, 
-- payments with amount ≤ 0, 
-- results where marks_obtained > max_marks, 
-- and probable duplicate students (same first name, last name and date of birth)


SELECT
    (SELECT COUNT(*) FROM student
      WHERE email IS NULL AND is_deleted = false)                         AS students_no_email,

    (SELECT COUNT(*) FROM student
      WHERE (phone IS NULL OR phone !~ '^[6-9][0-9]{9}$')
        AND is_deleted = false)                                            AS students_invalid_phone,

    (SELECT COUNT(*) FROM payment
      WHERE amount <= 0 AND status = 'success' AND is_deleted = false)    AS payments_non_positive,

    (SELECT COUNT(*) FROM exam_result
      WHERE marks_obtained > max_marks AND is_deleted = false)            AS results_above_max,

    (SELECT COUNT(*) FROM (
        SELECT first_name, last_name, date_of_birth
        FROM student
        WHERE is_deleted = false
        GROUP BY first_name, last_name, date_of_birth
        HAVING COUNT(*) > 1
     ) d)                                                                  AS duplicate_student_groups;