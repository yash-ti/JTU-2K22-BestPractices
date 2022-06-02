from django.utils.deprecation import MiddlewareMixin


class DisableCSRF(MiddlewareMixin):
    def process_request(self, request):
        """ Turn of CSRF checks for a request """
        setattr(request, '_dont_enforce_csrf_checks', True)