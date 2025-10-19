# Quickstart Guide: Testing Connection Permissions

This guide provides a manual, end-to-end test to verify the core functionality of the connection permission feature.

## Prerequisites

- The application is running.
- You are logged in as a user with the 'Admin' role.

## Steps

### 1. Create Users and Roles

1.  Navigate to the **User Management** page.
2.  Create a new user named "TestViewer".
3.  Assign the "TestViewer" user the **User** role.
4.  Create another new user named "TestUploader".
5.  Assign the "TestUploader" user the **Uploader** role.

### 2. Create a Connection

1.  Navigate to the **Database Management** page.
2.  Create a new database connection named "TestConnection".
3.  Verify that "TestConnection" is visible to you (the Admin).

### 3. Verify Initial Permissions

1.  Log out from the Admin account.
2.  Log in as the "TestViewer" user.
3.  Navigate to the page that lists available connections.
4.  **Expected Result**: "TestConnection" should **not** be visible.
5.  Log out.

### 4. Grant Permission

1.  Log back in as the Admin user.
2.  Navigate to the **User Management** page.
3.  Find the "TestViewer" user.
4.  Grant the "TestViewer" user permission to view "TestConnection".

### 5. Verify Granted Permission

1.  Log out from the Admin account.
2.  Log in as the "TestViewer" user.
3.  Navigate to the page that lists available connections.
4.  **Expected Result**: "TestConnection" should **now be visible**.

### 6. Revoke Permission

1.  Log back in as the Admin user.
2.  Navigate to the **User Management** page.
3.  Find the "TestViewer" user.
4.  Revoke the permission to view "TestConnection".

### 7. Verify Revoked Permission

1.  Log out from the Admin account.
2.  Log in as the "TestViewer" user.
3.  Navigate to the page that lists available connections.
4.  **Expected Result**: "TestConnection" should **no longer be visible**.
