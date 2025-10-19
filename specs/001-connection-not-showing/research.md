# Research & Decisions

This document records the decisions made to resolve ambiguities from the initial specification.

## 1. UI Update Behavior on Permission Change

- **Question**: Should permission changes reflect in the UI in real-time, or is a refresh acceptable?
- **Decision**: A page refresh is acceptable.
- **Rationale**: The user-provided requirements stated: "Permission updates should be reflected for the user upon their next page load or refresh." This is a simpler implementation than real-time updates using WebSockets or polling, and it meets the stated requirement.

## 2. User Feedback Mechanism

- **Question**: What form of feedback should be provided to the user when their connection list updates?
- **Decision**: Use Django's built-in messaging framework.
- **Rationale**: This is a standard and simple way to provide feedback to users in a Django application. It allows for displaying informational messages, warnings, and errors without significant custom frontend work. For example, after an admin changes a permission, a message can be displayed at the top of the page on the user's next page load.
