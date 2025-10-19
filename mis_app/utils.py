"""
Django MIS Utility Functions
Helper functions for logging, validation, and common operations
"""

import logging
import os
from typing import Dict, Any, Optional
from django.utils import timezone
from django.contrib.auth import get_user_model
from django.db import transaction
from sqlalchemy import create_engine
from django.shortcuts import get_object_or_404
from django.conf import settings
from .models import ExternalConnection
from .permissions import PermissionManager

logger = logging.getLogger(__name__)
User = get_user_model()


from django.contrib.auth.models import AbstractUser

def log_user_action(user: AbstractUser, action: str, object_type: str, object_id: str,
                   description: str, details: Dict[str, Any] = None,
                   ip_address: str = None, user_agent: str = None):
    """
    Log user actions to audit trail
    
    Args:
        user: User performing the action
        action: Type of action (create, update, delete, etc.)
        object_type: Type of object being acted upon
        object_id: ID of the object
        description: Human-readable description
        details: Additional metadata
        ip_address: User's IP address
        user_agent: User's browser/client info
    """
    try:
        from .models import AuditLog
        
        AuditLog.objects.create(
            user=user,
            action=action,
            object_type=object_type,
            object_id=str(object_id),
            object_name=description,
            details=details or {},
            ip_address=ip_address,
            user_agent=user_agent[:500] if user_agent else ''  # Truncate long user agents
        )
        
        logger.info(f"User {user.username} performed {action} on {object_type} {object_id}")
        
    except Exception as e:
        logger.error(f"Failed to log user action: {e}")


def safe_json_loads(json_str: str, default: Any = None) -> Any:
    """
    Safely parse JSON string with fallback
    
    Args:
        json_str: JSON string to parse
        default: Default value if parsing fails
        
    Returns:
        Parsed JSON data or default value
    """
    if not json_str:
        return default
    
    try:
        import json
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Failed to parse JSON: {e}")
        return default


def safe_json_dumps(data: Any, default: str = '{}') -> str:
    """
    Safely serialize data to JSON string
    
    Args:
        data: Data to serialize
        default: Default JSON string if serialization fails
        
    Returns:
        JSON string or default value
    """
    try:
        import json
        return json.dumps(data, ensure_ascii=False, separators=(',', ':'))
    except (TypeError, ValueError) as e:
        logger.warning(f"Failed to serialize JSON: {e}")
        return default


def validate_report_config(config: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Validate report configuration structure
    
    Args:
        config: Report configuration dictionary
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    required_fields = ['connection_id', 'type']
    
    for field in required_fields:
        if field not in config:
            return False, f"Missing required field: {field}"
    
    # Validate connection_id format (should be UUID)
    connection_id = config.get('connection_id')
    if connection_id:
        try:
            import uuid
            uuid.UUID(str(connection_id))
        except (ValueError, TypeError):
            return False, "Invalid connection_id format"
    
    # Validate columns if present
    columns = config.get('columns', [])
    if not isinstance(columns, list):
        return False, "Columns must be a list"
    
    # Validate filters if present
    filters = config.get('filters', [])
    if not isinstance(filters, list):
        return False, "Filters must be a list"
    
    for i, filter_item in enumerate(filters):
        if not isinstance(filter_item, dict):
            return False, f"Filter {i} must be a dictionary"
        
        if 'field' not in filter_item or 'operator' not in filter_item:
            return False, f"Filter {i} missing required field or operator"
    
    return True, None


def sanitize_table_name(table_name: str) -> str:
    """
    Sanitize table name for safe SQL usage
    
    Args:
        table_name: Raw table name
        
    Returns:
        Sanitized table name
    """
    import re
    
    # Remove any characters that aren't alphanumeric, underscore, or dot
    sanitized = re.sub(r'[^a-zA-Z0-9_.]', '', table_name)
    
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"table_{sanitized}"
    
    return sanitized


def sanitize_column_name(column_name: str) -> str:
    """
    Sanitize column name for safe SQL usage
    
    Args:
        column_name: Raw column name
        
    Returns:
        Sanitized column name
    """
    import re
    
    # Remove any characters that aren't alphanumeric or underscore
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)
    
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_{2,}', '_', sanitized)
    
    # Remove leading/trailing underscores
    sanitized = sanitized.strip('_')
    
    return sanitized or 'unnamed_column'


def generate_cache_key(prefix: str, *args) -> str:
    """
    Generate consistent cache key
    
    Args:
        prefix: Cache key prefix
        *args: Additional components for the key
        
    Returns:
        Generated cache key
    """
    import hashlib
    
    key_parts = [str(prefix)]
    key_parts.extend([str(arg) for arg in args])
    
    # Create hash of all parts for consistent length
    content = ':'.join(key_parts)
    hash_suffix = hashlib.md5(content.encode()).hexdigest()[:8]
    
    # Use first part of key + hash for readability and uniqueness
    readable_part = ':'.join(key_parts[:2]) if len(key_parts) > 1 else key_parts[0]
    
    return f"{readable_part}:{hash_suffix}"


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human readable format
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted size string (e.g., "1.5 MB")
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    import math
    
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    
    return f"{s} {size_names[i]}"


def truncate_string(text: str, max_length: int = 100, suffix: str = "...") -> str:
    """
    Truncate string to maximum length with suffix
    
    Args:
        text: Text to truncate
        max_length: Maximum length including suffix
        suffix: Suffix to add when truncating
        
    Returns:
        Truncated string
    """
    if not text or len(text) <= max_length:
        return text
    
    return text[:max_length - len(suffix)] + suffix


def get_client_ip(request) -> Optional[str]:
    """
    Get client IP address from request
    
    Args:
        request: Django request object
        
    Returns:
        Client IP address or None
    """
    x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
    if x_forwarded_for:
        ip = x_forwarded_for.split(',')[0].strip()
    else:
        ip = request.META.get('REMOTE_ADDR')
    
    return ip


def get_user_agent(request) -> str:
    """
    Get user agent from request
    
    Args:
        request: Django request object
        
    Returns:
        User agent string
    """
    return request.META.get('HTTP_USER_AGENT', '')


def is_ajax(request) -> bool:
    """
    Check if request is AJAX
    
    Args:
        request: Django request object
        
    Returns:
        True if AJAX request
    """
    return request.META.get('HTTP_X_REQUESTED_WITH') == 'XMLHttpRequest'


def batch_process(items: list, batch_size: int = 100, processor_func=None):
    """
    Process items in batches
    
    Args:
        items: List of items to process
        batch_size: Size of each batch
        processor_func: Function to process each batch
        
    Yields:
        Processed batches
    """
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        
        if processor_func:
            yield processor_func(batch)
        else:
            yield batch


def retry_on_exception(func, max_retries: int = 3, delay: float = 1.0,
                      exceptions: tuple = (Exception,)):
    """
    Retry function on specified exceptions
    
    Args:
        func: Function to retry
        max_retries: Maximum number of retries
        delay: Delay between retries in seconds
        exceptions: Tuple of exceptions to catch
        
    Returns:
        Function result or raises last exception
    """
    import time
    
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            
            if attempt < max_retries:
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries + 1} attempts failed")
    
    raise last_exception


def merge_dicts(*dicts: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge multiple dictionaries, with later ones taking precedence
    
    Args:
        *dicts: Dictionaries to merge
        
    Returns:
        Merged dictionary
    """
    result = {}
    for d in dicts:
        if d:
            result.update(d)
    return result


def deep_get(dictionary: Dict[str, Any], key_path: str, default: Any = None) -> Any:
    """
    Get value from nested dictionary using dot notation
    
    Args:
        dictionary: Dictionary to search
        key_path: Dot-separated key path (e.g., "config.database.host")
        default: Default value if key not found
        
    Returns:
        Value at key path or default
    """
    keys = key_path.split('.')
    current = dictionary
    
    try:
        for key in keys:
            current = current[key]
        return current
    except (KeyError, TypeError):
        return default


def deep_set(dictionary: Dict[str, Any], key_path: str, value: Any) -> None:
    """
    Set value in nested dictionary using dot notation
    
    Args:
        dictionary: Dictionary to modify
        key_path: Dot-separated key path
        value: Value to set
    """
    keys = key_path.split('.')
    current = dictionary
    
    for key in keys[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    
    current[keys[-1]] = value


class TransactionContext:
    """Context manager for database transactions with rollback on exception"""
    
    def __init__(self, using=None):
        self.using = using
        self.transaction = None
    
    def __enter__(self):
        self.transaction = transaction.atomic(using=self.using)
        return self.transaction.__enter__()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        result = self.transaction.__exit__(exc_type, exc_val, exc_tb)
        
        if exc_type:
            logger.error(f"Transaction rolled back due to {exc_type.__name__}: {exc_val}")
        
        return result


def measure_execution_time(func):
    """
    Decorator to measure function execution time
    
    Args:
        func: Function to measure
        
    Returns:
        Decorated function
    """
    import time
    from functools import wraps
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.debug(f"{func.__name__} executed in {execution_time:.3f}s")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.3f}s: {e}")
            raise
    
    return wrapper

def get_external_engine(connection_id, user):
    """Helper to get a SQLAlchemy engine for an external DB with better error handling."""
    try:
        connection = get_object_or_404(ExternalConnection, id=connection_id)

        if not PermissionManager.user_can_access_connection(user, connection):
            logger.warning("User %s attempted to access connection %s without permission", getattr(user, 'username', 'unknown'), connection_id)
            return None

        if connection.db_type == 'postgresql':
            connection_string = (
                f"postgresql+psycopg2://{connection.username}:{connection.password}@{connection.host}:{connection.port}/{connection.db_name}"
            )
            return create_engine(connection_string, connect_args={'connect_timeout': 5}, pool_recycle=1800)

        if connection.db_type == 'mysql':
            connection_string = (
                f"mysql+pymysql://{connection.username}:{connection.password}@{connection.host}:{connection.port}/{connection.db_name}"
            )
            return create_engine(connection_string, connect_args={'connect_timeout': 5}, pool_recycle=1800)

        if connection.db_type == 'sqlite':
            filepath = connection.filepath
            db_name = connection.db_name
            final_path = None

            if filepath and filepath.strip():
                final_path = filepath.strip()
            elif db_name and db_name.strip():
                final_path = os.path.join(settings.BASE_DIR, f'{db_name.strip()}.db')
            else:
                final_path = os.path.join(settings.BASE_DIR, 'default_sqlite.db')

            logger.info(f"Attempting to connect to SQLite DB at path: '{final_path}'")

            db_directory = os.path.dirname(final_path)
            if db_directory:
                os.makedirs(db_directory, exist_ok=True)

            connection_string = f"sqlite:///{final_path}"
            return create_engine(connection_string)

        logger.error(f"Unsupported database type: {connection.db_type}")
        return None

    except Exception as e:
        logger.error(f"Failed to create engine for connection {connection_id}: {e}", exc_info=True)
        return None




def upgrade_or_default_config_v2(config: dict | None, title: str = "Untitled Dashboard") -> tuple[dict, bool]:
    """Ensure a dashboard config conforms to the v2 multi-page schema."""
    from datetime import datetime, timezone
    import uuid

    migrated = False

    def _now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    if not isinstance(config, dict):
        now = _now_iso()
        return ({
            "title": title or "Untitled Dashboard",
            "version": 2,
            "pages": [{
                "id": str(uuid.uuid4()),
                "title": "Page 1",
                "widgets": [],
            }],
            "theme": {"palette": "Tableau.Classic10"},
            "createdAt": now,
            "updatedAt": now,
        }, True)

    cfg = dict(config)

    if not isinstance(cfg.get("title"), str) or not cfg["title"].strip():
        cfg["title"] = title or "Untitled Dashboard"
        migrated = True

    if cfg.get("version") != 2:
        cfg["version"] = 2
        migrated = True

    theme = cfg.get("theme") if isinstance(cfg.get("theme"), dict) else None
    if theme is None:
        cfg["theme"] = {"palette": "Tableau.Classic10"}
        migrated = True
    else:
        if "palette" not in theme:
            theme["palette"] = "Tableau.Classic10"
            migrated = True

    if not cfg.get("createdAt"):
        cfg["createdAt"] = _now_iso()
        migrated = True
    if not cfg.get("updatedAt"):
        cfg["updatedAt"] = _now_iso()
        migrated = True

    pages = cfg.get("pages") if isinstance(cfg.get("pages"), list) else None
    if pages is None:
        legacy_widgets = cfg.get("widgets") if isinstance(cfg.get("widgets"), list) else []
        cfg["pages"] = [{
            "id": str(uuid.uuid4()),
            "title": "Page 1",
            "widgets": legacy_widgets,
        }]
        if "widgets" in cfg:
            del cfg["widgets"]
        migrated = True
        pages = cfg["pages"]

    normalized_pages: list[dict] = []
    for index, page in enumerate(pages):
        if not isinstance(page, dict):
            page = {}
            migrated = True

        page_id = page.get("id") if isinstance(page.get("id"), str) and page.get("id") else str(uuid.uuid4())
        if page_id != page.get("id"):
            migrated = True

        title_value = page.get("title") if isinstance(page.get("title"), str) and page.get("title").strip() else f"Page {index + 1}"
        if title_value != page.get("title"):
            migrated = True

        widgets = page.get("widgets") if isinstance(page.get("widgets"), list) else []
        if widgets is not page.get("widgets"):
            migrated = True

        norm_widgets: list[dict] = []
        for widget in widgets:
            if not isinstance(widget, dict):
                migrated = True
                continue

            widget_copy = dict(widget)
            widget_changed = False

            if not widget_copy.get("id"):
                widget_copy["id"] = str(uuid.uuid4())
                widget_changed = True

            for key, default in (("w", 4), ("h", 3), ("x", 0), ("y", 0)):
                if key not in widget_copy:
                    widget_copy[key] = default
                    widget_changed = True

            if widget_changed:
                migrated = True

            norm_widgets.append(widget_copy)

        if norm_widgets != widgets:
            migrated = True

        normalized_pages.append({
            "id": page_id,
            "title": title_value,
            "widgets": norm_widgets,
        })

    if normalized_pages != pages:
        cfg["pages"] = normalized_pages

    return cfg, migrated

# Validation patterns
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
UUID_PATTERN = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

def validate_email(email: str) -> bool:
    """Validate email format"""
    import re
    return bool(re.match(EMAIL_PATTERN, email))


def validate_uuid(uuid_string: str) -> bool:
    """Validate UUID format"""
    import re
    return bool(re.match(UUID_PATTERN, str(uuid_string).lower()))


def clean_html(text: str) -> str:
    """Remove HTML tags from text"""
    import re
    return re.sub('<.*?>', '', text)


def generate_random_string(length: int = 32) -> str:
    """Generate random string for tokens, etc."""
    import secrets
    import string
    
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))