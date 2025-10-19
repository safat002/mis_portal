# Tasks: Permitted Tables Visibility

**Input**: Design documents from `/specs/002-in-data-management/`
**Prerequisites**: plan.md, spec.md

## Phase 3.1: Setup
- [ ] T001 Review existing models in `mis_app/models.py` to identify how `User Group`, `Table Permission`, and `Database Connection` are represented.

## Phase 3.2: Tests First (TDD) ⚠️ MUST COMPLETE BEFORE 3.3
- [ ] T002 [P] Create `tests/test_permissions.py` to write a failing integration test for the primary user story: a user should only see permitted connections and tables.
- [ ] T003 [P] In `tests/test_permissions.py`, write a failing unit test to check that a user with no permissions sees no connections.

## Phase 3.3: Core Implementation (ONLY after tests are failing)
- [ ] T004 Modify `mis_app/models.py` to ensure `User Group`, `Table Permission`, and `Database Connection` entities can support the required permission logic. This may involve extending Django's built-in Group and Permission models.
- [ ] T005 Create a new file `mis_app/permissions.py` and implement a function `get_permitted_connections(user)` that returns a list of database connections the user is allowed to see.
- [ ] T006 In `mis_app/permissions.py`, implement a function `get_permitted_tables(user, connection)` that returns a list of tables the user is allowed to see for a given connection.
- [ ] T007 Refactor the view in `mis_app/data_views.py` to use `get_permitted_connections` and `get_permitted_tables` to filter the displayed connections and tables.
- [ ] T008 Refactor the view in `mis_app/report_views.py` to use the new permission functions.
- [ ] T009 Refactor the main dashboard view in `mis_app/views.py` to use the new permission functions.

## Phase 3.4: Integration
- [ ] T010 Ensure the changes to the models are correctly migrated to the database.

## Phase 3.5: Polish
- [ ] T011 [P] Add unit tests for the new functions in `mis_app/permissions.py` in the `tests/test_permissions.py` file.
- [ ] T012 Review the code for clarity, performance, and adherence to Django best practices.

## Dependencies
- T001 must be done before all other tasks.
- Tests (T002, T003) must be done before implementation (T004-T009).
- T004 (models) blocks T005 and T006.
- T005 and T006 block T007, T008, T009.
- Implementation (T004-T009) blocks polish (T011, T012).

## Parallel Example
```
# Launch T002 and T003 together:
Task: "Create tests/test_permissions.py to write a failing integration test for the primary user story: a user should only see permitted connections and tables."
Task: "In tests/test_permissions.py, write a failing unit test to check that a user with no permissions sees no connections."
```
