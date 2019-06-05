apilevel = 1.0
threadsafety = 1
paramstyle = 'pyformat'

from GBaseConnector.GBaseConnection import GBaseConnection
def Connect(*args, **kwargs):
    """Shortcut for creating a GBaseConnection object."""
    return GBaseConnection(*args, **kwargs)
connect = Connect