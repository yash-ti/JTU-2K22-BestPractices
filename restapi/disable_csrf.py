from django.utils.deprecation import MiddlewareMixin
from logging import logger

class DisableCSRF(MiddlewareMixin):
    def process_request(self, request):
        logger.info("Turning off CSRF checks")
        """ Turn of CSRF checks for a request """
        setattr(request, '_dont_enforce_csrf_checks', True)