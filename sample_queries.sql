-- ============================================================================
-- Sample Queries — These represent real analyst workflows.
-- Your optimized pipeline & schema should make these FAST.
-- ============================================================================


-- 1. INCIDENT RESPONSE: Find all firewall traffic from a compromised host in a time window.
--    Analyst knows the source IP and approximate time of compromise.
SELECT *
FROM logs
WHERE log_type = 'firewall'
  AND parsed_data->>'src' = '192.168.1.42'
  AND timestamp BETWEEN '2024-01-15T05:00:00Z' AND '2024-01-15T06:00:00Z';


-- 2. THREAT HUNTING: Find all DNS queries to a known malicious domain.
--    Analyst has a list of IOCs (indicators of compromise).
SELECT *
FROM logs
WHERE log_type = 'dns'
  AND parsed_data->>'query_domain' = 'evil-domain.com';


-- 3. BRUTE FORCE DETECTION: Find all failed SSH login attempts from a specific IP.
--    Analyst investigating a potential brute-force attack.
SELECT *
FROM logs
WHERE log_type = 'auth'
  AND parsed_data->>'status' = 'Failed'
  AND parsed_data->>'source_ip' = '192.168.1.100';


-- 4. ANALYTICS: Event volume by log type per hour.
--    Used for capacity planning and anomaly detection.
SELECT
    log_type,
    DATE_TRUNC('hour', timestamp::timestamp) AS hour,
    COUNT(*) AS event_count
FROM logs
GROUP BY 1, 2
ORDER BY 2, 1;


-- 5. TOP TALKERS: Source IPs generating the most firewall events.
--    Used to identify noisy hosts or potential lateral movement.
SELECT
    parsed_data->>'src' AS source_ip,
    COUNT(*) AS event_count
FROM logs
WHERE log_type = 'firewall'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;


-- 6. LATERAL MOVEMENT: Find all connections from internal IPs to a specific destination port.
--    Analyst investigating potential RDP/SSH lateral movement.
SELECT
    parsed_data->>'src' AS source_ip,
    parsed_data->>'dst' AS dest_ip,
    COUNT(*) AS connection_count
FROM logs
WHERE log_type = 'firewall'
  AND parsed_data->>'dst_port' = '3389'
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 20;


-- ============================================================================
-- THINK ABOUT:
--   • How fast are these queries with the naive single-table JSONB schema?
--   • What indexes would dramatically improve them?
--   • Would separate typed tables (firewall_logs, dns_logs, auth_logs) help?
--   • At 100M+ rows, would partitioning by timestamp be beneficial?
-- ============================================================================

