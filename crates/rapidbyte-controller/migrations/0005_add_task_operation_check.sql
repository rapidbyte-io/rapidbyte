ALTER TABLE tasks ADD CONSTRAINT tasks_operation_check
    CHECK (operation IN ('sync', 'check_apply', 'teardown', 'assert'));
