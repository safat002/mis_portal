# Data Model

This document defines the data models for the role-based permission system. It extends Django's built-in User and Group models.

## Core Models

### 1. Role

Represents a user role within the system.

- **Model Name**: `Role`
- **Fields**:
  - `name`: (CharField) The name of the role (e.g., "Admin", "Moderator", "Uploader", "User"). Must be unique.
  - `description`: (TextField) A brief description of the role's purpose.

### 2. Permission

Represents a specific permission that can be assigned.

- **Model Name**: `Permission`
- **Fields**:
  - `name`: (CharField) A human-readable name for the permission (e.g., "Can view table", "Can edit dashboard").
  - `codename`: (CharField) A unique, programmatic name for the permission (e.g., "view_table", "edit_dashboard"). This will be used in the code to check for permissions.

### 3. UserProfile (or extending existing User)

We will extend the built-in Django `User` model using a one-to-one `UserProfile` model to store the user's role.

- **Model Name**: `UserProfile`
- **Fields**:
  - `user`: (OneToOneField to `User`) The associated user.
  - `role`: (ForeignKey to `Role`) The user's assigned role.

### 4. ObjectPermission

This model links a user or group to a specific permission on a specific object (e.g., a `Connection`, `Report`, or `Dashboard`).

- **Model Name**: `ObjectPermission`
- **Fields**:
  - `permission`: (ForeignKey to `Permission`) The permission being granted.
  - `user`: (ForeignKey to `User`, nullable) The user receiving the permission.
  - `group`: (ForeignKey to `Group`, nullable) The group receiving the permission.
  - `content_type`: (ForeignKey to `ContentType`) The type of object the permission is for.
  - `object_id`: (PositiveIntegerField) The ID of the object.
  - `content_object`: (GenericForeignKey) A generic relationship to the object itself.

## Relationships

- A `User` has one `UserProfile`, which has one `Role`.
- A `Role` can have many `Users`.
- `ObjectPermission` provides a many-to-many relationship between `Users`/`Groups` and any other model instance, qualified by a `Permission`.

## Example Usage

- To check if a user can view a specific `Connection`:
  1. Get the user's `Role` from their `UserProfile`.
  2. Check if the `Role` has a default permission for viewing connections.
  3. Check if the user or any of their groups have a specific `ObjectPermission` for the `view_table` codename on that `Connection` instance.
