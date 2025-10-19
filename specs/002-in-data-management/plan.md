# Implementation Plan: CSV Data Upload and Mapping

**Branch**: `002-in-data-management` | **Date**: 2025-09-28 | **Spec**: `C:\Users\safat\OneDrive - Bitopi Group\Desktop\django_mis_project\specs\002-in-data-management\spec.md`
**Input**: User description: "for a simple page which will be added with my existing project in which user will upload excel/csv file and it will add necessary data to the db as per db structure, \mis_app_db_project folder I have 2 files one is schema structure one is csv in csv report name consists the file name which will be uploaded to the app and system will autometically detect the db table and column and add data to it. it will show a view to the user which data is uploading where. also some tables like unit name, buyer name will not accept the duplicate data whereas it will convert the unitname as unit code from the existing table data and put it in the updated entries in the db. there will be a mapping correction option also where user can modify existing mapping."

## Execution Flow (/plan command scope)
```
1. Load feature spec from Input path
   → If not found: ERROR "No feature spec at {path}"
2. Fill Technical Context (scan for NEEDS CLARIFICATION)
   → Detect Project Type from file system structure or context (web=frontend+backend, mobile=app+api)
   → Set Structure Decision based on project type
3. Fill the Constitution Check section based on the content of the constitution document.
4. Evaluate Constitution Check section below
   → If violations exist: Document in Complexity Tracking
   → If no justification possible: ERROR "Simplify approach first"
   → Update Progress Tracking: Initial Constitution Check
5. Execute Phase 0 → research.md
   → If NEEDS CLARIFICATION remain: ERROR "Resolve unknowns"
6. Execute Phase 1 → contracts, data-model.md, quickstart.md, agent-specific template file (e.g., `CLAUDE.md` for Claude Code, `.github/copilot-instructions.md` for GitHub Copilot, `GEMINI.md` for Gemini CLI, `QWEN.md` for Qwen Code or `AGENTS.md` for opencode).
7. Re-evaluate Constitution Check section
   → If new violations: Refactor design, return to Phase 1
   → Update Progress Tracking: Post-Design Constitution Check
8. Plan Phase 2 → Describe task generation approach (DO NOT create tasks.md)
9. STOP - Ready for /tasks command
```

**IMPORTANT**: The /plan command STOPS at step 7. Phases 2-4 are executed by other commands:
- Phase 2: /tasks command creates tasks.md
- Phase 3-4: Implementation execution (manual or via tools)

## Summary
A new page will be added to allow users to upload Excel/CSV files. The system will automatically detect the target database table and columns based on the file name and content, and then import the data. The UI will show a preview of the data mapping. The system will handle duplicate entries for certain tables (e.g., 'unit name', 'buyer name') by looking up existing values and using corresponding codes. A mapping correction feature will allow users to modify the automatically detected mappings.

## Technical Context
**Language/Version**: Python 3.12, Django 4.2.7
**Primary Dependencies**: djangorestframework, pandas, celery, psycopg2-binary
**Storage**: PostgreSQL
**Testing**: pytest
**Target Platform**: Web Application
**Project Type**: single
**Performance Goals**: [NEEDS CLARIFICATION: What is the expected file size and frequency of uploads? What is the acceptable processing time for an uploaded file?]
**Constraints**: [NEEDS CLARIFICATION: Are there any specific security considerations for file uploads?]
**Scale/Scope**: [NEEDS CLARIFICATION: How many users are expected to use this feature concurrently?]

## Constitution Check
*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

No violations of the constitution template were found.

## Project Structure

### Documentation (this feature)
```
specs/002-in-data-management/
├── plan.md              # This file (/plan command output)
├── research.md          # Phase 0 output (/plan command)
├── data-model.md        # Phase 1 output (/plan command)
├── quickstart.md        # Phase 1 output (/plan command)
├── contracts/           # Phase 1 output (/plan command)
└── tasks.md             # Phase 2 output (/tasks command - NOT created by /plan)
```

### Source Code (repository root)
```
mis_app/
├── upload_handlers.py   # Logic for handling file uploads, parsing, and mapping
├── forms.py             # Django form for file upload
├── views.py             # Views for the upload page and mapping correction
├── models.py            # Models to store upload history and mappings
├── templates/
│   └── mis_app/
│       ├── upload_page.html
│       └── mapping_correction.html
└── urls.py              # URLs for the new pages

tests/
└── test_upload_handlers.py
```

**Structure Decision**: The new feature will be added to the existing `mis_app` Django application. This keeps the code organized within the main application structure.

## Phase 0: Outline & Research
1. **Extract unknowns from Technical Context** above:
   - Research task: Determine performance requirements for file uploads (size, frequency, processing time).
   - Research task: Identify security best practices for file uploads in a Django application.
   - Research task: Clarify the expected user load for this feature.

2. **Generate and dispatch research agents**:
   - Task: "Research best practices for large file uploads in Django with Celery for background processing."
   - Task: "Find robust libraries for fuzzy matching of column headers in Python/pandas."
   - Task: "Investigate strategies for creating a user-friendly data mapping correction UI in Django."

3. **Consolidate findings** in `research.md` using format:
   - Decision: [what was chosen]
   - Rationale: [why chosen]
   - Alternatives considered: [what else evaluated]

**Output**: research.md with all NEEDS CLARIFICATION resolved

## Phase 1: Design & Contracts
*Prerequisites: research.md complete*

1. **Extract entities from feature spec** → `data-model.md`:
   - **FileUpload**: Tracks uploaded files (file name, path, status, user, timestamp).
   - **DataMapping**: Stores the mapping configuration for each file upload (source column, destination table, destination column, transformation rules).
   - **TransformationRule**: Defines rules for data conversion (e.g., lookup `unit.name` to get `unit.unit_code`).

2. **Generate API contracts** from functional requirements → `/contracts/`:
   - `POST /api/upload/`: Endpoint to upload a file. Returns a task ID for monitoring the import process.
   - `GET /api/upload/{file_id}/mapping/`: Endpoint to retrieve the suggested data mapping for a file.
   - `POST /api/upload/{file_id}/mapping/`: Endpoint to save the corrected data mapping.
   - `POST /api/upload/{file_id}/process/`: Endpoint to start the data import process with the corrected mapping.

3. **Generate contract tests** from contracts:
   - One test file per endpoint, asserting request/response schemas.

4. **Extract test scenarios** from user stories:
   - Test scenario for uploading a CSV and verifying the data is correctly imported.
   - Test scenario for correcting a mapping and verifying the data is imported with the corrected mapping.

5. **Update agent file incrementally** (O(1) operation):
   - Run `.specify/scripts/powershell/update-agent-context.ps1 -AgentType codex`
     **IMPORTANT**: Execute it exactly as specified above. Do not add or remove any arguments.
   - If exists: Add only NEW tech from current plan
   - Preserve manual additions between markers
   - Update recent changes (keep last 3)
   - Keep under 150 lines for token efficiency
   - Output to repository root

**Output**: data-model.md, /contracts/*, failing tests, quickstart.md, agent-specific file

## Phase 2: Task Planning Approach
*This section describes what the /tasks command will do - DO NOT execute during /plan*

**Task Generation Strategy**:
- Load `.specify/templates/tasks-template.md` as base
- Generate tasks from Phase 1 design docs (contracts, data model, quickstart)
- Each contract → contract test task [P]
- Each entity → model creation task [P]
- Each user story → integration test task
- Implementation tasks to make tests pass

**Ordering Strategy**:
- TDD order: Tests before implementation
- Dependency order: Models before services before UI
- Mark [P] for parallel execution (independent files)

**Estimated Output**: 25-30 numbered, ordered tasks in tasks.md

**IMPORTANT**: This phase is executed by the /tasks command, NOT by /plan

## Phase 3+: Future Implementation
*These phases are beyond the scope of the /plan command*

**Phase 3**: Task execution (/tasks command creates tasks.md)
**Phase 4**: Implementation (execute tasks.md following constitutional principles)
**Phase 5**: Validation (run tests, execute quickstart.md, performance validation)

## Complexity Tracking
*Fill ONLY if Constitution Check has violations that must be justified*

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| N/A       | N/A        | N/A                                 |


## Progress Tracking
*This checklist is updated during execution flow*

**Phase Status**:
- [ ] Phase 0: Research complete (/plan command)
- [ ] Phase 1: Design complete (/plan command)
- [ ] Phase 2: Task planning complete (/plan command - describe approach only)
- [ ] Phase 3: Tasks generated (/tasks command)
- [ ] Phase 4: Implementation complete
- [ ] Phase 5: Validation passed

**Gate Status**:
- [X] Initial Constitution Check: PASS
- [ ] Post-Design Constitution Check: PASS
- [ ] All NEEDS CLARIFICATION resolved
- [ ] Complexity deviations documented

---
*Based on Constitution v2.1.1 - See `/memory/constitution.md`*