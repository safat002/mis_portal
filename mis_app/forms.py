# mis_app/forms.py

"""
Enhanced Django Forms for MIS Application
Comprehensive forms with validation and user-friendly interfaces
"""

from django import forms
from django.contrib.auth.forms import UserCreationForm, UserChangeForm, PasswordResetForm
from django.core.exceptions import ValidationError
from django.contrib.auth import authenticate
from django.utils import timezone
from django.db import transaction
import re

from .models import User, UserGroup, GroupMembership, GroupPermission, UserPermission, ExternalConnection


class CustomUserCreationForm(UserCreationForm):
    """Enhanced user creation form with additional fields"""
    
    email = forms.EmailField(
        required=True,
        widget=forms.EmailInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter email address'
        }),
        help_text='A valid email address is required.'
    )
    
    first_name = forms.CharField(
        max_length=30,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'First name'
        })
    )
    
    last_name = forms.CharField(
        max_length=30,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Last name'
        })
    )
    
    user_type = forms.ChoiceField(
        choices=User.USER_TYPE_CHOICES,
        required=True,
        widget=forms.Select(attrs={
            'class': 'form-select',
        }),
        help_text='Select the user\'s access level.'
    )
    
    department = forms.CharField(
        max_length=100,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Department (optional)'
        })
    )
    
    job_title = forms.CharField(
        max_length=100,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Job title (optional)'
        })
    )
    
    phone_number = forms.CharField(
        max_length=20,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': '+1234567890',
            'pattern': r'^\+?1?\d{9,15}$'
        }),
        help_text='Enter a valid phone number with country code.'
    )
    
    manager = forms.ModelChoiceField(
        queryset=User.objects.none(),
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-select',
            'data-bs-toggle': 'dropdown'
        }),
        empty_label="Select manager (optional)",
        help_text='Choose a manager for this user.'
    )
    
    theme_preference = forms.ChoiceField(
        choices=User.THEME_CHOICES,
        required=False,
        initial='corporate',
        widget=forms.Select(attrs={
            'class': 'form-select'
        }),
        help_text='Select the default theme for this user.'
    )
    
    groups = forms.ModelMultipleChoiceField(
        queryset=UserGroup.objects.filter(is_active=True),
        required=False,
        widget=forms.SelectMultiple(attrs={
            'class': 'form-control',
            'multiple': True,
            'data-placeholder': 'Select groups...',
            'style': 'height: 120px;'
        }),
        help_text='Select groups to assign to this user.'
    )
    
    class Meta:
        model = User
        fields = ('username', 'email', 'first_name', 'last_name', 'password1', 'password2', 
                 'user_type', 'department', 'job_title', 'phone_number', 'manager', 
                 'theme_preference', 'groups')
        
    def __init__(self, *args, **kwargs):
        self.request_user = kwargs.pop('request_user', None)
        super().__init__(*args, **kwargs)
        
        # Style the username field
        self.fields['username'].widget.attrs.update({
            'class': 'form-control',
            'placeholder': 'Enter username'
        })
        self.fields['username'].help_text = 'Letters, digits, and @/./+/-/_ characters only.'
        
        # Style password fields
        self.fields['password1'].widget.attrs.update({
            'class': 'form-control',
            'placeholder': 'Enter password'
        })
        self.fields['password1'].help_text = 'Password must be at least 8 characters long.'
        
        self.fields['password2'].widget.attrs.update({
            'class': 'form-control',
            'placeholder': 'Confirm password'
        })
        self.fields['password2'].help_text = 'Enter the same password as before, for verification.'
        
        # Set manager queryset based on request user permissions
        if self.request_user and self.request_user.can_manage_users():
            self.fields['manager'].queryset = User.objects.filter(
                user_type__in=['Admin', 'Moderator']
            ).exclude(id=self.request_user.id if hasattr(self.request_user, 'id') else None)
        
        # Limit user_type choices based on request user
        if self.request_user:
            if self.request_user.user_type == 'Moderator':
                # Moderators can only create Users, Uploaders, and Viewers
                self.fields['user_type'].choices = [
                    ('User', 'Regular User'),
                    ('Uploader', 'Uploader'),
                    ('Viewer', 'Read-Only User'),
                ]
    
    def clean_email(self):
        email = self.cleaned_data.get('email')
        if email and User.objects.filter(email=email).exists():
            raise ValidationError("A user with this email address already exists.")
        return email
    
    def clean_phone_number(self):
        phone = self.cleaned_data.get('phone_number')
        if phone:
            # Remove all non-digit characters except +
            phone_clean = re.sub(r'[^\d+]', '', phone)
            if not re.match(r'^\+?1?\d{9,15}$', phone_clean):
                raise ValidationError("Please enter a valid phone number with country code (e.g., +1234567890).")
            return phone_clean
        return phone
    
    def clean(self):
        cleaned_data = super().clean()
        
        # Additional validation can be added here
        user_type = cleaned_data.get('user_type')
        manager = cleaned_data.get('manager')
        
        # Ensure Admins don't have managers
        if user_type == 'Admin' and manager:
            self.add_error('manager', 'Administrators cannot have managers.')
        
        return cleaned_data
    
    @transaction.atomic
    def save(self, commit=True):
        user = super().save(commit=False)
        user.email = self.cleaned_data['email']
        user.user_type = self.cleaned_data['user_type']
        user.department = self.cleaned_data.get('department', '')
        user.job_title = self.cleaned_data.get('job_title', '')
        user.phone_number = self.cleaned_data.get('phone_number', '')
        user.manager = self.cleaned_data.get('manager')
        user.theme_preference = self.cleaned_data.get('theme_preference', 'corporate')
        user.is_email_verified = False
        user.created_by = getattr(self, 'request_user', None)
        
        if commit:
            user.save()
            
            # Add user to selected groups
            groups = self.cleaned_data.get('groups')
            if groups:
                for group in groups:
                    GroupMembership.objects.create(
                        user=user,
                        group=group,
                        added_by=self.request_user
                    )
        
        return user


class CustomUserChangeForm(UserChangeForm):
    """Enhanced user change form with additional fields"""
    
    email = forms.EmailField(
        required=True,
        widget=forms.EmailInput(attrs={'class': 'form-control'})
    )
    
    user_type = forms.ChoiceField(
        choices=User.USER_TYPE_CHOICES,
        required=True,
        widget=forms.Select(attrs={'class': 'form-select'})
    )
    
    is_active = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'})
    )
    
    department = forms.CharField(
        max_length=100,
        required=False,
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    
    job_title = forms.CharField(
        max_length=100,
        required=False,
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    
    phone_number = forms.CharField(
        max_length=20,
        required=False,
        widget=forms.TextInput(attrs={'class': 'form-control'})
    )
    
    manager = forms.ModelChoiceField(
        queryset=User.objects.none(),
        required=False,
        widget=forms.Select(attrs={'class': 'form-select'}),
        empty_label="No manager"
    )
    
    theme_preference = forms.ChoiceField(
        choices=User.THEME_CHOICES,
        required=False,
        widget=forms.Select(attrs={'class': 'form-select'})
    )
    
    class Meta:
        model = User
        fields = ('username', 'email', 'first_name', 'last_name', 'user_type', 
                 'is_active', 'department', 'job_title', 'phone_number', 
                 'manager', 'theme_preference')
    
    def __init__(self, *args, **kwargs):
        self.request_user = kwargs.pop('request_user', None)
        super().__init__(*args, **kwargs)
        
        # Set manager queryset
        if self.instance and self.instance.pk:
            self.fields['manager'].queryset = User.objects.filter(
                user_type__in=['Admin', 'Moderator']
            ).exclude(id=self.instance.pk)


class UserGroupForm(forms.ModelForm):
    """Form for creating and editing user groups"""
    
    name = forms.CharField(
        max_length=100,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Group name'
        }),
        help_text='Enter a unique name for this group.'
    )
    
    description = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={
            'class': 'form-control',
            'placeholder': 'Group description (optional)',
            'rows': 3
        }),
        help_text='Describe the purpose of this group.'
    )
    
    color = forms.CharField(
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'type': 'color',
            'value': '#007bff'
        }),
        help_text='Choose a color to represent this group in the interface.'
    )
    
    parent_group = forms.ModelChoiceField(
        queryset=UserGroup.objects.filter(is_active=True),
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-select',
        }),
        empty_label="No parent group",
        help_text='Select a parent group to create a hierarchy.'
    )
    
    users = forms.ModelMultipleChoiceField(
        queryset=User.objects.filter(is_active=True),
        required=False,
        widget=forms.SelectMultiple(attrs={
            'class': 'form-control user-select-widget',
            'multiple': True,
            'data-placeholder': 'Select users...',
            'style': 'height: 200px;'
        }),
        help_text='Select users to add to this group.'
    )
    
    class Meta:
        model = UserGroup
        fields = ['name', 'description', 'color', 'parent_group', 'users']
    
    def __init__(self, *args, **kwargs):
        self.request_user = kwargs.pop('request_user', None)
        super().__init__(*args, **kwargs)
        
        # If editing, exclude self from parent group choices
        if self.instance and self.instance.pk:
            self.fields['parent_group'].queryset = self.fields['parent_group'].queryset.exclude(
                id=self.instance.pk
            )
    
    def clean_name(self):
        name = self.cleaned_data.get('name')
        if name:
            # Check for duplicate names
            queryset = UserGroup.objects.filter(name__iexact=name)
            if self.instance and self.instance.pk:
                queryset = queryset.exclude(pk=self.instance.pk)
            
            if queryset.exists():
                raise ValidationError("A group with this name already exists. Please choose a different name.")
        
        return name
    
    @transaction.atomic
    def save(self, commit=True):
        group = super().save(commit=False)
        if not group.pk:
            group.created_by = self.request_user
        
        if commit:
            group.save()
            
            # Handle user assignments
            if 'users' in self.cleaned_data:
                # Remove existing memberships
                GroupMembership.objects.filter(group=group).delete()
                
                # Add new memberships
                for user in self.cleaned_data['users']:
                    GroupMembership.objects.create(
                        user=user,
                        group=group,
                        added_by=self.request_user
                    )
        
        return group


class DatabaseConnectionForm(forms.ModelForm):
    """Resilient form for creating and editing database connections."""

    password = forms.CharField(
        required=False,
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter new password or leave blank'
        }),
        help_text='Password for database authentication.'
    )

    class Meta:
        model = ExternalConnection
        fields = [
            'nickname', 'db_type', 'host', 'port', 'username',
            'password', 'db_name', 'schema', 'filepath', 'is_default'
        ]
        widgets = {
            'nickname': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'e.g., Production DB'}),
            'db_type': forms.Select(attrs={'class': 'form-select', 'id': 'db-type-select'}),
            'host': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'localhost or IP'}),
            'port': forms.TextInput(attrs={'class': 'form-control', 'placeholder': '5432'}),
            'username': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'DB username'}),
            'db_name': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Database name'}),
            'schema': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'public (default)'}),
            'filepath': forms.TextInput(attrs={'class': 'form-control', 'placeholder': '/path/to/db.sqlite'}),
            'is_default': forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        }

    def __init__(self, *args, **kwargs):
        self.request_user = kwargs.pop('request_user', None)
        super().__init__(*args, **kwargs)

        # Hide password on edit
        if self.instance and self.instance.pk:
            self.fields['password'].initial = ''

    def clean_nickname(self):
        nickname = self.cleaned_data.get('nickname')
        if nickname and self.request_user:
            qs = ExternalConnection.objects.filter(owner=self.request_user, nickname__iexact=nickname)
            if self.instance.pk:
                qs = qs.exclude(pk=self.instance.pk)
            if qs.exists():
                raise ValidationError("You already have a connection with this name.")
        return nickname

    def clean(self):
        cleaned = super().clean()
        db_type = cleaned.get('db_type')
        host = cleaned.get('host')
        filepath = cleaned.get('filepath')
        port = cleaned.get('port')

        self._validate_required_fields(db_type, host, filepath)
        self._validate_port_format(port)

        return cleaned

    def _validate_required_fields(self, db_type, host, filepath):
        if db_type == 'sqlite':
            if not filepath:
                self.add_error('filepath', 'File path is required for SQLite.')
        else:
            if not host:
                self.add_error('host', f'Host is required for {db_type} connections.')

    def _validate_port_format(self, port):
        if port and not port.isdigit():
            self.add_error('port', 'Port must be numeric.')

    def save(self, commit=True):
        # This associates the form instance with a model object but doesn't save it yet.
        connection = super().save(commit=False)
        # Manually set the owner from the user object passed in from the view.
        connection.owner = self.request_user
        
        # Handle password separately: only update if a new one was provided
        password = self.cleaned_data.get('password')
        if password:
            # In a real production app, you would encrypt this password
            connection.password = password
        
        # Now, save the instance to the database with the owner set.
        if commit:
            connection.save()
        
        return connection


class BulkUserActionForm(forms.Form):
    """Form for bulk actions on users"""
    
    ACTION_CHOICES = [
        ('activate', 'Activate Users'),
        ('deactivate', 'Deactivate Users'),
        ('delete', 'Delete Users'),
        ('change_type', 'Change User Type'),
        ('assign_group', 'Assign to Group'),
        ('remove_group', 'Remove from Group'),
        ('reset_password', 'Reset Passwords'),
    ]
    
    action = forms.ChoiceField(
        choices=ACTION_CHOICES,
        widget=forms.Select(attrs={
            'class': 'form-select',
            'id': 'bulk-action-select'
        }),
        help_text='Select the action to perform on selected users.'
    )
    
    users = forms.ModelMultipleChoiceField(
        queryset=User.objects.all(),
        widget=forms.MultipleHiddenInput()
    )
    
    # Conditional fields
    new_user_type = forms.ChoiceField(
        choices=User.USER_TYPE_CHOICES,
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-select',
            'style': 'display: none;'
        }),
        help_text='Select the new user type to assign.'
    )
    
    target_group = forms.ModelChoiceField(
        queryset=UserGroup.objects.filter(is_active=True),
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-select',
            'style': 'display: none;'
        }),
        empty_label="Select group",
        help_text='Select the group to assign users to or remove users from.'
    )
    
    def __init__(self, *args, **kwargs):
        self.request_user = kwargs.pop('request_user', None)
        super().__init__(*args, **kwargs)
    
    def clean(self):
        cleaned_data = super().clean()
        action = cleaned_data.get('action')
        users = cleaned_data.get('users')
        
        if not users:
            raise ValidationError("Please select at least one user to perform the action on.")
        
        # Prevent users from performing actions on themselves for certain operations
        if self.request_user and action in ['delete', 'deactivate']:
            if self.request_user in users:
                raise ValidationError("You cannot perform this action on your own account.")
        
        # Validate conditional fields
        if action == 'change_type' and not cleaned_data.get('new_user_type'):
            raise ValidationError("Please select a user type for this action.")
        
        if action in ['assign_group', 'remove_group'] and not cleaned_data.get('target_group'):
            raise ValidationError("Please select a group for this action.")
        
        return cleaned_data


class UserSearchForm(forms.Form):
    """Form for searching and filtering users"""
    
    search = forms.CharField(
        max_length=100,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Search users by name, email, or department...',
            'id': 'user-search-input'
        })
    )
    
    user_type = forms.ChoiceField(
        choices=[('', 'All Types')] + User.USER_TYPE_CHOICES,
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-select',
            'id': 'user-type-filter'
        })
    )
    
    department = forms.CharField(
        max_length=100,
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Department',
            'id': 'department-filter'
        })
    )
    
    is_active = forms.ChoiceField(
        choices=[
            ('', 'All Users'),
            ('true', 'Active Users'),
            ('false', 'Inactive Users')
        ],
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-select',
            'id': 'status-filter'
        })
    )
    
    group = forms.ModelChoiceField(
        queryset=UserGroup.objects.filter(is_active=True),
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-select',
            'id': 'group-filter'
        }),
        empty_label="All Groups"
    )
    
    date_joined_from = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={
            'class': 'form-control',
            'type': 'date',
            'id': 'date-from-filter'
        })
    )
    
    date_joined_to = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={
            'class': 'form-control',
            'type': 'date',
            'id': 'date-to-filter'
        })
    )


class UserImportForm(forms.Form):
    """Form for importing users from CSV/Excel"""
    
    file = forms.FileField(
        widget=forms.FileInput(attrs={
            'class': 'form-control',
            'accept': '.csv,.xlsx,.xls',
            'id': 'import-file-input'
        }),
        help_text="Upload a CSV or Excel file containing user data (max 5MB)."
    )
    
    has_header = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={
            'class': 'form-check-input'
        }),
        label="File has header row",
        help_text="Check this if the first row contains column names."
    )
    
    send_welcome_emails = forms.BooleanField(
        required=False,
        initial=True,
        widget=forms.CheckboxInput(attrs={
            'class': 'form-check-input'
        }),
        label="Send welcome emails",
        help_text="Send welcome emails with login credentials to new users."
    )
    
    default_user_type = forms.ChoiceField(
        choices=User.USER_TYPE_CHOICES,
        initial='User',
        widget=forms.Select(attrs={
            'class': 'form-select'
        }),
        help_text="Default user type for users without a specified type in the file."
    )
    
    def clean_file(self):
        file = self.cleaned_data.get('file')
        if file:
            # Check file size (max 5MB)
            if file.size > 5 * 1024 * 1024:
                raise ValidationError("File size cannot exceed 5MB. Please upload a smaller file.")
            
            # Check file extension
            allowed_extensions = ['.csv', '.xlsx', '.xls']
            if not any(file.name.lower().endswith(ext) for ext in allowed_extensions):
                raise ValidationError("Only CSV and Excel files are allowed. Please upload a valid file.")
        
        return file
    
class GroupPermissionForm(forms.ModelForm):
    """Form for adding permissions to a group."""
    class Meta:
        model = GroupPermission
        fields = ['resource_type', 'resource_name', 'permission_level']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['resource_type'].widget.attrs.update({'class': 'form-select'})
        self.fields['resource_name'].widget.attrs.update({'class': 'form-control', 'placeholder': 'e.g., sales_data or a UUID'})
        self.fields['permission_level'].widget.attrs.update({'class': 'form-select'})