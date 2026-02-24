-- Benchmark seed: large profile (~50 KB/row documents)
-- 20 columns: large TEXT (~15 KB body, ~20 KB HTML), deep JSONB (~5 KB + ~2 KB)
-- Row count controlled by :bench_rows psql variable
-- Usage: psql -v bench_rows=10000 -f bench_seed_large.sql

DROP TABLE IF EXISTS public.bench_events;

CREATE TABLE public.bench_events (
    id                  BIGSERIAL PRIMARY KEY,
    document_id         UUID NOT NULL DEFAULT gen_random_uuid(),
    author_id           BIGINT NOT NULL,
    category_id         INTEGER NOT NULL,
    title               TEXT NOT NULL,
    slug                TEXT NOT NULL,
    status              TEXT NOT NULL,
    summary             TEXT NOT NULL,
    body_text           TEXT NOT NULL,
    body_html           TEXT NOT NULL,
    metadata            JSONB NOT NULL,
    settings            JSONB NOT NULL DEFAULT '{}',
    tags                TEXT NOT NULL,
    revision            INTEGER NOT NULL DEFAULT 1,
    word_count          INTEGER,
    view_count          BIGINT NOT NULL DEFAULT 0,
    is_published        BOOLEAN NOT NULL DEFAULT false,
    published_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.bench_events (
    document_id, author_id, category_id, title, slug, status,
    summary, body_text, body_html, metadata, settings,
    tags, revision, word_count, view_count, is_published,
    published_at, created_at, updated_at
)
SELECT
    gen_random_uuid(),
    (i % 5000) + 1,
    (i % 50) + 1,

    -- title (~80 chars)
    (ARRAY['Understanding','Building','Optimizing','Scaling','Deploying'])[1 + (i % 5)]
    || ' ' ||
    (ARRAY['Microservices','Data Pipelines','Cloud Infrastructure','Machine Learning Models',
           'Distributed Systems','Event-Driven Architecture','Real-Time Analytics',
           'Container Orchestration'])[1 + (i % 8)]
    || ': ' ||
    (ARRAY['A Comprehensive Guide','Best Practices and Patterns','Lessons from Production',
           'Performance Deep Dive'])[1 + (i % 4)],

    -- slug (~80 chars)
    lower(
        (ARRAY['understanding','building','optimizing','scaling','deploying'])[1 + (i % 5)]
        || '-' ||
        (ARRAY['microservices','data-pipelines','cloud-infrastructure','ml-models',
               'distributed-systems','event-driven-arch','real-time-analytics',
               'container-orchestration'])[1 + (i % 8)]
        || '-' || i::text
    ),

    -- status
    (ARRAY['draft','review','published','archived'])[1 + (i % 4)],

    -- summary (~500 chars)
    'This article explores the key concepts and practical approaches to ' ||
    (ARRAY['building scalable microservice architectures that handle millions of requests',
           'designing robust data pipelines for real-time event processing and analytics',
           'deploying cloud-native applications with zero-downtime and automated failover',
           'implementing machine learning models in production with proper monitoring',
           'managing distributed systems across multiple regions with strong consistency'])[1 + (i % 5)]
    || '. We cover ' ||
    (ARRAY['architecture patterns, implementation strategies, and operational best practices',
           'data modeling, stream processing, and exactly-once delivery guarantees',
           'infrastructure as code, service mesh configuration, and observability tooling',
           'feature engineering, model serving, A/B testing, and drift detection frameworks',
           'consensus protocols, partition tolerance, and conflict resolution mechanisms'])[1 + (i % 5)]
    || ' drawn from real-world experience at scale.',

    -- body_text (~15 KB): 5 different paragraph variants rotated per row
    CASE (i % 5)
        WHEN 0 THEN repeat(
            'Modern software architecture demands careful consideration of scalability, reliability, and maintainability. '
            || 'When designing distributed systems, engineers must balance consistency with availability, often making trade-offs '
            || 'that depend heavily on the specific use case and business requirements. This section examines the fundamental '
            || 'principles that guide these decisions and provides practical frameworks for evaluation. ', 40)
        WHEN 1 THEN repeat(
            'Data pipeline design has evolved significantly with the advent of stream processing frameworks and cloud-native '
            || 'infrastructure. Organizations now process billions of events daily, requiring architectures that can handle '
            || 'backpressure, provide exactly-once semantics, and scale horizontally without manual intervention. We explore '
            || 'proven patterns for building resilient data infrastructure at scale. ', 42)
        WHEN 2 THEN repeat(
            'Cloud infrastructure management requires a deep understanding of networking, security, and cost optimization. '
            || 'Teams that adopt infrastructure as code practices can achieve reproducible deployments across environments while '
            || 'maintaining compliance requirements. This guide covers the essential tooling, workflows, and organizational '
            || 'patterns that enable effective cloud operations at enterprise scale. ', 41)
        WHEN 3 THEN repeat(
            'Machine learning in production presents unique challenges around model versioning, feature stores, and serving '
            || 'infrastructure. Beyond training accuracy, teams must consider latency requirements, resource utilization, and '
            || 'monitoring for data drift. This comprehensive overview addresses the full lifecycle from experimentation through '
            || 'deployment and ongoing maintenance of ML systems. ', 43)
        WHEN 4 THEN repeat(
            'Distributed consensus and state management remain among the most challenging aspects of modern systems design. '
            || 'Whether implementing leader election, distributed locks, or replicated state machines, engineers face fundamental '
            || 'trade-offs described by the CAP theorem and its extensions. This analysis provides practical guidance for '
            || 'choosing the right consistency model for your application requirements. ', 40)
    END,

    -- body_html (~20 KB): HTML-wrapped body with tags and structure
    '<article class="content-body" data-doc-id="' || i::text || '">'
    || '<h1>' ||
        (ARRAY['Understanding','Building','Optimizing','Scaling','Deploying'])[1 + (i % 5)]
        || ' ' ||
        (ARRAY['Microservices','Data Pipelines','Cloud Infrastructure','ML Models',
               'Distributed Systems'])[1 + (i % 5)]
    || '</h1>'
    || '<div class="meta"><span class="author">Author ' || (i % 5000 + 1)::text || '</span>'
    || '<span class="date">2024-' || lpad((i % 12 + 1)::text, 2, '0') || '-15</span></div>'
    || '<section class="introduction"><p>' ||
        repeat(
            'This comprehensive guide provides an in-depth exploration of modern engineering practices. '
            || 'We examine real-world case studies, architectural patterns, and implementation strategies '
            || 'that have proven effective in production environments serving millions of users. ', 25)
    || '</p></section>'
    || '<section class="main-content"><h2>Key Concepts</h2><p>' ||
        repeat(
            'The fundamental principles underlying this domain include separation of concerns, '
            || 'loose coupling, and high cohesion. These principles manifest differently depending on '
            || 'the scale and complexity of the system under consideration. ', 30)
    || '</p></section>'
    || '<section class="conclusion"><p>' ||
        repeat(
            'In conclusion, adopting these practices requires organizational commitment and technical investment. '
            || 'The benefits compound over time as teams build institutional knowledge and automated tooling. ', 15)
    || '</p></section></article>',

    -- metadata (~5 KB deep nested JSONB)
    jsonb_build_object(
        'seo', jsonb_build_object(
            'title', 'SEO Title for Document ' || i::text || ' - Comprehensive Technical Guide',
            'description', 'An in-depth technical guide covering architecture, implementation, '
                || 'and operational best practices for modern software engineering teams.',
            'keywords', jsonb_build_array(
                'engineering', 'architecture', 'scalability', 'best-practices',
                'distributed-systems', 'cloud-native', 'devops', 'sre',
                (ARRAY['microservices','pipelines','infrastructure','machine-learning','consensus'])[1 + (i % 5)],
                (ARRAY['kubernetes','terraform','kafka','pytorch','etcd'])[1 + (i % 5)]
            ),
            'og_image', 'https://cdn.example.com/images/doc-' || (i % 1000)::text || '.png',
            'canonical_url', 'https://docs.example.com/articles/' || i::text
        ),
        'analytics', jsonb_build_object(
            'pageviews', (i * 37) % 100000,
            'unique_visitors', (i * 23) % 50000,
            'avg_time_on_page', ((i * 7) % 600) + 30,
            'bounce_rate', round(((i * 13) % 80 + 10)::numeric / 100, 2),
            'scroll_depth', round(((i * 11) % 60 + 40)::numeric / 100, 2),
            'traffic_sources', jsonb_build_object(
                'organic', (i * 17) % 40000,
                'direct', (i * 11) % 20000,
                'referral', (i * 7) % 15000,
                'social', (i * 3) % 10000
            ),
            'top_referrers', jsonb_build_array(
                'google.com', 'twitter.com', 'reddit.com', 'hackernews.com', 'linkedin.com'
            )
        ),
        'revisions', jsonb_build_array(
            jsonb_build_object('rev', 1, 'author', 'author-' || (i % 100)::text,
                'date', '2024-01-10', 'changes', 'Initial draft with outline and key sections',
                'diff_stats', jsonb_build_object('additions', 450, 'deletions', 0)),
            jsonb_build_object('rev', 2, 'author', 'editor-' || (i % 50)::text,
                'date', '2024-01-15', 'changes', 'Editorial review: restructured introduction and added examples',
                'diff_stats', jsonb_build_object('additions', 120, 'deletions', 45)),
            jsonb_build_object('rev', 3, 'author', 'author-' || (i % 100)::text,
                'date', '2024-02-01', 'changes', 'Added performance benchmarks section and updated diagrams',
                'diff_stats', jsonb_build_object('additions', 200, 'deletions', 30)),
            jsonb_build_object('rev', 4, 'author', 'reviewer-' || (i % 30)::text,
                'date', '2024-02-10', 'changes', 'Technical accuracy review and fact-checking',
                'diff_stats', jsonb_build_object('additions', 15, 'deletions', 8)),
            jsonb_build_object('rev', 5, 'author', 'author-' || (i % 100)::text,
                'date', '2024-03-01', 'changes', 'Final polish, SEO optimization, and publication prep',
                'diff_stats', jsonb_build_object('additions', 50, 'deletions', 20))
        ),
        'permissions', jsonb_build_object(
            'owner', 'user-' || (i % 5000 + 1)::text,
            'editors', jsonb_build_array(
                'editor-' || (i % 50)::text,
                'editor-' || ((i + 13) % 50)::text,
                'editor-' || ((i + 29) % 50)::text
            ),
            'viewers', jsonb_build_array(
                'team-engineering', 'team-product', 'team-docs',
                'viewer-' || (i % 200)::text,
                'viewer-' || ((i + 77) % 200)::text
            ),
            'public', (i % 4 = 0)
        ),
        'integrations', jsonb_build_object(
            'slack_channel', '#docs-' || (ARRAY['engineering','product','platform','infra','data'])[1 + (i % 5)],
            'jira_ticket', 'DOC-' || (i % 10000 + 1)::text,
            'github_pr', (i % 50000) + 1,
            'confluence_page_id', 'CONF-' || lpad((i % 99999 + 1)::text, 5, '0'),
            'notion_block_id', gen_random_uuid()::text
        )
    ),

    -- settings (~2 KB JSONB)
    jsonb_build_object(
        'editor', jsonb_build_object(
            'mode', (ARRAY['wysiwyg','markdown','html','split'])[1 + (i % 4)],
            'autosave_interval', (ARRAY[30, 60, 120, 300])[1 + (i % 4)],
            'spell_check', (i % 3 != 0),
            'grammar_check', (i % 2 = 0),
            'word_wrap', true,
            'font_size', (ARRAY[14, 16, 18, 20])[1 + (i % 4)],
            'line_height', (ARRAY[1.5, 1.6, 1.75, 2.0])[1 + (i % 4)]
        ),
        'publishing', jsonb_build_object(
            'auto_publish', false,
            'require_review', true,
            'min_reviewers', (i % 3) + 1,
            'notify_subscribers', true,
            'social_share', jsonb_build_object(
                'twitter', (i % 2 = 0),
                'linkedin', (i % 3 = 0),
                'hackernews', (i % 5 = 0)
            ),
            'rss_include', true
        ),
        'access_control', jsonb_build_object(
            'require_login', (i % 4 != 0),
            'allowed_domains', jsonb_build_array('acme.com', 'partner.io'),
            'ip_whitelist_enabled', false,
            'rate_limit', jsonb_build_object('requests_per_minute', 60, 'burst', 100)
        )
    ),

    -- tags (5-10 comma-separated)
    (ARRAY['architecture','engineering','devops','sre','platform'])[1 + (i % 5)] || ',' ||
    (ARRAY['tutorial','deep-dive','case-study','reference','guide'])[1 + ((i / 5) % 5)] || ',' ||
    (ARRAY['beginner','intermediate','advanced','expert'])[1 + (i % 4)] || ',' ||
    (ARRAY['cloud','on-prem','hybrid','serverless','edge'])[1 + ((i / 4) % 5)] || ',' ||
    (ARRAY['aws','gcp','azure','multi-cloud'])[1 + (i % 4)] ||
    CASE WHEN i % 3 = 0 THEN ',' || (ARRAY['featured','editors-pick','trending','evergreen'])[1 + (i % 4)] ELSE '' END ||
    CASE WHEN i % 5 = 0 THEN ',' || (ARRAY['sponsored','partner','community','official'])[1 + (i % 4)] ELSE '' END,

    -- revision
    (i % 10) + 1,

    -- word_count
    ((i * 17) % 8000) + 2000,

    -- view_count
    (i * 37) % 100000,

    -- is_published
    (i % 4 != 3),

    -- published_at (nullable for drafts)
    CASE WHEN i % 4 != 3 THEN NOW() - make_interval(days => (i % 365)) ELSE NULL END,

    NOW() - make_interval(days => (i % 730)),
    NOW() - make_interval(days => (i % 365))
FROM generate_series(1, :'bench_rows') AS s(i);
