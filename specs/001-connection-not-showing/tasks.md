# Tasks: Connection Visibility for Permitted Users

**Input**: Design documents from `specs/001-connection-not-showing/`

## Phase 3.1: Setup & Models
- [ ] T001 [P] Define the `Role`, `Permission`, `UserProfile`, and `ObjectPermission` models in `mis_app/models.py` as specified in `data-model.md`.
- [ ] T002 [P] Create initial data migrations for the new models using `python manage.py makemigrations mis_app`.
- [ ] T003 [P] Apply the migrations to the database with `python manage.py migrate`.
- [ ] T004 [P] Register the new `Role`, `Permission`, `UserProfile`, and `ObjectPermission` models in `mis_app/admin.py` to make them accessible in the Django admin interface.

## Phase 3.2: Tests First (TDD)
- [ ] T005 [P] Create `tests/test_models.py` and write unit tests to verify the relationships and constraints of the new permission-related models.
- [ ] T006 [P] Create `tests/test_permissions.py` and write integration tests based on the scenarios in `quickstart.md`. These tests should initially fail.
  - Test that a user without permission cannot see a connection.
  - Test that granting a permission makes the connection visible.
  - Test that revoking a permission hides the connection.

## Phase 3.3: Core Implementation
- [ ] T007 Create a new file `mis_app/services.py` and implement a `has_permission` function that checks if a user has a specific permission for a given object, based on their role and any specific `ObjectPermission` entries.
- [ ] T008 Modify the view in `mis_app/views.py` that lists connections. Use the `has_permission` service to filter the connection list, ensuring only permitted connections are shown.
- [ ] T009 Create API views and serializers in `mis_app/api/` for managing `Role` and `ObjectPermission`.
  - `RoleViewSet`: Allow Admins and Moderators to list, create, update, and delete roles.
  - `ObjectPermissionViewSet`: Allow Admins and Moderators to grant and revoke permissions for users/groups on specific objects.

## Phase 3.4: Integration
- [ ] T010 Update the User Management page template to include UI elements for assigning roles to users and managing object-specific permissions.
- [ ] T011 Integrate the new API endpoints with the User Management page's frontend logic.
- [ ] T012 Ensure that the Django admin interface for the new models is functional and allows for easy management.

## Phase 3.5: Polish
- [ ] T013 [P] Add docstrings to the new models, services, and views.
- [ ] T014 [P] Run all tests and ensure they pass. Refactor code for clarity and performance if needed.
- [ ] T015 Manually run through the steps in `quickstart.md` to perform a final validation of the feature.

## Dependencies
- Model creation (T001-T003) must be done before any other tasks.
- Tests (T005-T006) should be written before the core implementation (T007-T009).
- The permission service (T007) is a dependency for the view modifications (T008).
- API endpoints (T009) are required for the UI integration (T010-T011).

## Parallel Example
```
# The following setup and test creation tasks can be run in parallel:
Task: "T001 [P] Define the `Role`, `Permission`, `UserProfile`, and `ObjectPermission` models..."
Task: "T005 [P] Create `tests/test_models.py`..."
Task: "T006 [P] Create `tests/test_permissions.py`..."
```
