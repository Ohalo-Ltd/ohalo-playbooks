-- Always drop only the policies we actually create below
DROP POLICY IF EXISTS secure_document_access ON documents;
DROP POLICY IF EXISTS secure_chunk_access ON document_chunks;

-- SECURE RLS DESIGN: Single comprehensive policy with explicit access control
CREATE POLICY secure_document_access ON documents
FOR SELECT
USING (
    -- Grant access ONLY if one of these specific conditions is met:
    (
        -- 1. User is explicitly entitled via email list
        entitled_emails @> ARRAY[current_setting('app.user_email', true)]::text[]
    ) OR (
        -- 2. Performance Review access: ONLY if it's their own review OR they're HR with permission
        category = 'Performance Review' AND (
            extracted_employee_id = current_setting('app.employee_id', true) OR
            current_setting('app.groups', true) LIKE '%can_see_all_reviews%'
        )
    ) OR (
        -- 3. Public policies: Only for specific public categories
        category IN ('Code of Conduct', 'Leave and Time-Off Policy')
    )
    -- Note: If none of these conditions match, access is DENIED by default
);

-- Document chunks: Inherit access control from parent document
CREATE POLICY secure_chunk_access ON document_chunks
FOR SELECT
USING (
    -- Access chunks ONLY if the user can access the parent document
    EXISTS (
        SELECT 1 
        FROM documents d 
        WHERE d.id = document_chunks.document_id
        -- Use the same logic as the main document policy
        AND (
            (
                d.entitled_emails @> ARRAY[current_setting('app.user_email', true)]::text[]
            ) OR (
                d.category = 'Performance Review' AND (
                    d.extracted_employee_id = current_setting('app.employee_id', true) OR
                    current_setting('app.groups', true) LIKE '%can_see_all_reviews%'
                )
            ) OR (
                d.category IN ('Code of Conduct', 'Leave and Time-Off Policy')
            )
        )
    )
);