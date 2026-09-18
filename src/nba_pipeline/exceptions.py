"""
Pipeline-specific errors.

They inherit from PipelineError so the job can tell "the data was bad" (exit code 2)
apart from "the code broke" (regular traceback).
"""

class PipelineError(Exception):
    """Base class for every data error in the pipeline."""


class SchemaDriftError(PipelineError):
    """The file does not have the columns we expected."""


class EmptySourceError(PipelineError):
    """The file exists but carries no rows."""


class UnknownSnapshotError(PipelineError):
    """A version value in the data has no snapshot date configured in SNAPSHOT_DATES."""


class ValidationError(PipelineError):
    """Rows failed quality rules and the run was configured to stop on that."""