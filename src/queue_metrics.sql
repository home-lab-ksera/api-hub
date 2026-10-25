SELECT jsonb_build_object(
    'pending', count(*) FILTER (WHERE state='pending'),
    'sending', count(*) FILTER (WHERE state='sending'),
    'retry', count(*) FILTER (WHERE state='retry'),
    'sent', count(*) FILTER (WHERE state='sent'),
    'uncertain', count(*) FILTER (WHERE state='uncertain'),
    'failed', count(*) FILTER (WHERE state='failed')) AS counts,
    COALESCE(EXTRACT(EPOCH FROM now() - min(created_at) FILTER (WHERE state IN ('pending','sending','retry')))::double precision, 0) AS oldest,
    COALESCE(EXTRACT(EPOCH FROM max(updated_at) FILTER (WHERE state='sent'))::double precision, 0) AS last_success
FROM notifications;
