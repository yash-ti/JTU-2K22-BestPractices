from rest_framework.exceptions import APIException
from rest_framework import status

class UnauthorizedUserException(APIException):
    """ An Exception class for unauthorized users """
    status_code = status.HTTP_404_NOT_FOUND
    default_detail: str = "Not Found"
    default_code: str = "Records unavailable"