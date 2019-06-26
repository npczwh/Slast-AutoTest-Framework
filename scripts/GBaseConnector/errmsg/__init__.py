__all__ = [
    'get_client_errmsg'
]

from GBaseConnector import GBaseErrorCode

def get_client_errmsg(error, language='eng'):
    """Lookup client error

    This function will lookup the client error message based on the given
    error and return the error message. If the error was not found,
    None will be returned.

    Error can be either an integer or a string. For example:
        error: 2000
        error: CR_UNKNOWN_ERROR

    The language attribute can be used to retrieve a localized message, when
    available.

    Returns a string or None.
    """
    try:
        tmp = __import__('GBaseConnector.errmsg.%s' % language,
                            globals(), locals(), ['client_errmsg'])
    except ImportError:
        raise ImportError("No localization support for language '%s'" % (
                          language))
    client_error = tmp.client_errmsg

    if isinstance(error, int):
        errno = error
        for key, value in GBaseErrorCode.__dict__.items():
            if value == errno:
                error = key
                break

    if isinstance(error, (str)):
        try:
            return getattr(client_error, error)
        except AttributeError:
            return None

    raise ValueError("error argument needs to be either an integer or string")

if __name__ == '__main__':
    print get_client_errmsg(2001)
    print get_client_errmsg(2002)
    print get_client_errmsg(2003)
    print get_client_errmsg(2004)
