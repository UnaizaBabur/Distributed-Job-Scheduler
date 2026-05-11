-- SELECT job_id, job_type, status, submitted_at 
-- FROM jobs 
-- ORDER BY submitted_at DESC 
-- LIMIT 5;

-- SELECT job_id, job_type, s

SELECT status, COUNT(*) FROM jobs GROUP BY status;

-- SELECT job_id, job_type, status, submitted_at 
-- FROM jobs 
-- WHERE submitted_at > NOW() - INTERVAL '10 minutes'
-- ORDER BY submitted_at DESC;

-- SELECT job_id, job_type, status, submitted_at 
-- FROM jobs 
-- WHERE job_id = '12345678-1234-1234-1234-123456789012';

-- SELECT status, COUNT(*) 
-- FROM jobs 
-- GROUP BY status;