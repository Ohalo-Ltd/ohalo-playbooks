-- Entitlements Feature - Database Migration Script
-- Version: 1.0
-- Date: 2025-11-25
-- Description: Add entitlements support to Graph RAG system

-- ============================================================================
-- STEP 1: Add entitlements_enabled flag to projects table
-- ============================================================================

ALTER TABLE projects 
ADD COLUMN IF NOT EXISTS entitlements_enabled BOOLEAN DEFAULT FALSE;

COMMENT ON COLUMN projects.entitlements_enabled IS 
'When true, queries are filtered based on user permissions. When false, all users see all documents.';


-- ============================================================================
-- STEP 2: Create project_users table
-- ============================================================================

CREATE TABLE IF NOT EXISTS project_users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    role VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure email is unique within a project
    CONSTRAINT unique_project_user_email UNIQUE(project_id, email)
);

COMMENT ON TABLE project_users IS 
'Mock IdP user profiles for testing entitlements. Each user represents an employee in the fake organization.';

COMMENT ON COLUMN project_users.email IS 
'User email address - used as unique identifier for access control.';

COMMENT ON COLUMN project_users.role IS 
'Optional role label (e.g., "Admin", "Engineer", "Manager") for display purposes only.';


-- ============================================================================
-- STEP 3: Create project_groups table
-- ============================================================================

CREATE TABLE IF NOT EXISTS project_groups (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name VARCHAR(255) NOT NULL,
    code VARCHAR(100) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Ensure group code is unique within a project
    CONSTRAINT unique_project_group_code UNIQUE(project_id, code)
);

COMMENT ON TABLE project_groups IS 
'Mock IdP groups for testing entitlements. Groups represent teams or departments in the fake organization.';

COMMENT ON COLUMN project_groups.code IS 
'Group identifier/code - matches realmAccountId from DXR WHO_CAN_ACCESS metadata.';

COMMENT ON COLUMN project_groups.name IS 
'Human-readable group name for display in UI.';


-- ============================================================================
-- STEP 4: Create indexes for performance
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_project_users_project_id 
ON project_users(project_id);

CREATE INDEX IF NOT EXISTS idx_project_users_email 
ON project_users(email);

CREATE INDEX IF NOT EXISTS idx_project_groups_project_id 
ON project_groups(project_id);

CREATE INDEX IF NOT EXISTS idx_project_groups_code 
ON project_groups(code);

-- Composite index for common query pattern
CREATE INDEX IF NOT EXISTS idx_project_users_project_email 
ON project_users(project_id, email);


-- ============================================================================
-- STEP 5: Create triggers for updated_at columns
-- ============================================================================

CREATE TRIGGER update_project_users_updated_at 
BEFORE UPDATE ON project_users
FOR EACH ROW 
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_project_groups_updated_at 
BEFORE UPDATE ON project_groups
FOR EACH ROW 
EXECUTE FUNCTION update_updated_at_column();


-- ============================================================================
-- STEP 6: Insert sample data for testing (optional)
-- ============================================================================

-- Uncomment to insert sample users and groups for testing

/*
-- Sample Project (assumes a project exists or creates one)
INSERT INTO projects (name, description, entitlements_enabled)
VALUES ('Sample Entitled Project', 'Test project with entitlements enabled', true)
ON CONFLICT DO NOTHING
RETURNING id;

-- Sample Users (replace <project_id> with actual UUID)
INSERT INTO project_users (project_id, name, email, role) VALUES
('<project_id>', 'Alice Smith', 'alice@example.com', 'Procurement Officer'),
('<project_id>', 'Bob Johnson', 'bob@example.com', 'Finance Manager'),
('<project_id>', 'Charlie Davis', 'charlie@example.com', 'Engineer')
ON CONFLICT DO NOTHING;

-- Sample Groups (replace <project_id> with actual UUID)
INSERT INTO project_groups (project_id, name, code) VALUES
('<project_id>', 'Engineering Team', 'eng-001'),
('<project_id>', 'Finance Department', 'fin-001'),
('<project_id>', 'Procurement Division', 'proc-001')
ON CONFLICT DO NOTHING;
*/


-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Check that new column exists
SELECT column_name, data_type, column_default 
FROM information_schema.columns 
WHERE table_name = 'projects' 
AND column_name = 'entitlements_enabled';

-- Check that new tables exist
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_name IN ('project_users', 'project_groups');

-- Check indexes
SELECT indexname, tablename 
FROM pg_indexes 
WHERE schemaname = 'public' 
AND tablename IN ('project_users', 'project_groups');

-- Count existing data
SELECT 
    (SELECT COUNT(*) FROM projects WHERE entitlements_enabled = true) as enabled_projects,
    (SELECT COUNT(*) FROM project_users) as total_users,
    (SELECT COUNT(*) FROM project_groups) as total_groups;


-- ============================================================================
-- ROLLBACK SCRIPT (in case of issues)
-- ============================================================================

/*
-- WARNING: This will delete all entitlements data!
-- Only run if you need to completely undo this migration

DROP TRIGGER IF EXISTS update_project_users_updated_at ON project_users;
DROP TRIGGER IF EXISTS update_project_groups_updated_at ON project_groups;

DROP INDEX IF EXISTS idx_project_users_project_id;
DROP INDEX IF EXISTS idx_project_users_email;
DROP INDEX IF EXISTS idx_project_users_project_email;
DROP INDEX IF EXISTS idx_project_groups_project_id;
DROP INDEX IF EXISTS idx_project_groups_code;

DROP TABLE IF EXISTS project_users CASCADE;
DROP TABLE IF EXISTS project_groups CASCADE;

ALTER TABLE projects DROP COLUMN IF EXISTS entitlements_enabled;
*/


-- ============================================================================
-- MIGRATION COMPLETE
-- ============================================================================

-- Log successful migration
DO $$
BEGIN
    RAISE NOTICE 'Entitlements migration completed successfully';
    RAISE NOTICE 'New tables: project_users, project_groups';
    RAISE NOTICE 'New column: projects.entitlements_enabled';
    RAISE NOTICE 'All projects default to entitlements_enabled = false';
END $$;
