class PyneedException(Exception):
    """Base exception class for all errors raised by Pyneed Open Source.
    These exceptions results from invalid use cases and will be reported
    to the users.
    """

class FileIOException(PyneedException):
    """Raised if there is an error while doing file IO."""
