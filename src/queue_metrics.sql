SELECT
    count(*) FILTER (WHERE state='failed') AS failed,
    count(*) FILTER (WHERE state='pending') AS pending,
    count(*) FILTER (WHERE state='retry') AS retry,
    count(*) FILTER (WHERE state='sending') AS sending,
    count(*) FILTER (WHERE state='sent') AS sent,
    count(*) FILTER (WHERE state='uncertain') AS uncertain,
    COALESCE(EXTRACT(EPOCH FROM now() - min(created_at) FILTER (WHERE state IN ('pending','sending','retry')))::double precision, 0) AS oldest,
    COALESCE(EXTRACT(EPOCH FROM max(updated_at) FILTER (WHERE state='sent'))::double precision, 0) AS last_success
FROM notifications;
