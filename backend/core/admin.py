from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as DjangoUserAdmin

from .models import User, PasswordResetRequest, EmailVerificationRequest


@admin.register(User)
class UserAdmin(DjangoUserAdmin):
    list_display = ("username", "email", "role", "email_verified", "is_staff", "is_superuser")
    list_filter = ("role", "email_verified", "is_staff", "is_superuser")
    search_fields = ("username", "email")
    ordering = ("username",)

    fieldsets = DjangoUserAdmin.fieldsets + (
        ("Role & Verification", {"fields": ("role", "email_verified")}),
    )
    add_fieldsets = DjangoUserAdmin.add_fieldsets + (
        ("Role & Verification", {"fields": ("role", "email_verified", "email")}),
    )


@admin.register(PasswordResetRequest)
class PasswordResetAdmin(admin.ModelAdmin):
    list_display = ("request_id", "user", "expires_at", "used_at", "created_at")
    search_fields = ("request_id", "user__username", "user__email")
    list_filter = ("used_at",)
    readonly_fields = ("request_id", "user", "code", "expires_at", "used_at", "created_at")

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(EmailVerificationRequest)
class EmailVerificationAdmin(admin.ModelAdmin):
    list_display = ("request_id", "user", "expires_at", "used_at", "created_at")
    search_fields = ("request_id", "user__username", "user__email")
    list_filter = ("used_at",)
    readonly_fields = ("request_id", "user", "code", "expires_at", "used_at", "created_at")

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False
