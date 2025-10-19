# Research: CSV Data Upload and Mapping

This document outlines the research tasks required to clarify the technical requirements for the CSV data upload and mapping feature.

## Research Tasks

### 1. Performance Requirements

- **Task**: Determine performance requirements for file uploads.
- **Questions to Answer**:
    - What is the expected maximum file size for uploads (in MB/GB)?
    - What is the expected frequency of uploads (e.g., per hour, per day)?
    - What is the acceptable processing time for an uploaded file, from upload to data being available in the database?
- **Method**: Discuss with stakeholders, analyze existing data import processes.

### 2. Security Considerations

- **Task**: Identify security best practices for file uploads in a Django application.
- **Questions to Answer**:
    - What are the potential security vulnerabilities associated with file uploads (e.g., directory traversal, cross-site scripting)?
    - What measures should be implemented to mitigate these risks (e.g., file type validation, virus scanning, storing files outside the web root)?
- **Method**: Review Django documentation, OWASP guidelines, and security best practices for web applications.

### 3. User Load

- **Task**: Clarify the expected user load for this feature.
- **Questions to Answer**:
    - How many users are expected to use this feature concurrently?
    - Are there any peak usage times?
- **Method**: Discuss with stakeholders, analyze user analytics if available.

### 4. Technical Implementation

- **Task**: Research best practices for large file uploads in Django with Celery for background processing.
- **Method**: Review tutorials, blog posts, and documentation on Django, Celery, and file handling.

- **Task**: Find robust libraries for fuzzy matching of column headers in Python/pandas.
- **Method**: Explore libraries like `fuzzywuzzy`, `thefuzz`, and evaluate their suitability for this project.

- **Task**: Investigate strategies for creating a user-friendly data mapping correction UI in Django.
- **Method**: Look for examples of similar UIs, consider using a JavaScript-based solution for a more interactive experience.
