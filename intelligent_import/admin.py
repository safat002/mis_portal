from django.contrib import admin
from .models import ReportTemplate, ReportTemplateHeader, PendingMaster, UnitAlias

class ReportTemplateHeaderInline(admin.TabularInline):
    model = ReportTemplateHeader
    extra = 0

@admin.register(ReportTemplate)
class ReportTemplateAdmin(admin.ModelAdmin):
    list_display = ("name", "version", "is_active", "ownership_type", "updated_at")
    search_fields = ("name",)
    inlines = [ReportTemplateHeaderInline]

@admin.register(PendingMaster)
class PendingMasterAdmin(admin.ModelAdmin):
    list_display = ("entity", "status", "created_at")
    list_filter = ("entity", "status")

@admin.register(UnitAlias)
class UnitAliasAdmin(admin.ModelAdmin):
    list_display = ("unit_name_alias", "unit_code")
    search_fields = ("unit_name_alias", "unit_code")
