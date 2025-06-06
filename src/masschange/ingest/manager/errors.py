class IngestManagerError(RuntimeError):
    pass

class FileAlreadyRegisteredError(IngestManagerError):
    pass