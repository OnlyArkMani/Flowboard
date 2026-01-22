from rest_framework.permissions import BasePermission, SAFE_METHODS


def _role(user):
    if not user or not user.is_authenticated:
        return None
    if user.is_superuser:
        return "admin"
    return getattr(user, "role", "user")


def is_admin(user):
    return _role(user) == "admin"


def is_moderator(user):
    return _role(user) in ["admin", "moderator"]


class UploadPermissions(BasePermission):
    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False
        if request.method in SAFE_METHODS:
            return True
        if getattr(view, "action", "") in ["create", "retry"]:
            return True
        return is_admin(request.user)


class JobRunPermissions(BasePermission):
    def has_permission(self, request, view):
        return bool(request.user and request.user.is_authenticated)


class JobPermissions(BasePermission):
    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False
        if request.method in SAFE_METHODS:
            return True
        if getattr(view, "action", "") == "trigger":
            return True
        return is_admin(request.user)


class IncidentPermissions(BasePermission):
    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False
        if request.method in SAFE_METHODS:
            return True
        return is_moderator(request.user)


class TicketPermissions(BasePermission):
    def has_permission(self, request, view):
        if not request.user or not request.user.is_authenticated:
            return False
        return is_moderator(request.user)
