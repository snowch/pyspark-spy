import logging
from typing import NamedTuple

from pyspark_spy.util import get_java_values, from_optional

logger = logging.getLogger(__name__)


class JavaClass:
    @classmethod
    def from_java(cls, jobj):
        try:
            return cls.try_convert(jobj)
        except Exception as e:  # Catch all exceptions for more informative logging
            logger.error(
                'Error converting Java object to "%s": %s. Java object fields: %s',
                cls.__name__, e, dir(jobj)
            )
            raise  # Re-raise the exception after logging

    @classmethod
    def try_convert(cls, jobj):
        raise NotImplementedError()


class JobEndEvent(JavaClass, NamedTuple('JobEndEvent', [
    ('jobId', int),
    ('time', int),
    ('jobResult', str)
])):

    @classmethod
    def try_convert(cls, jobj):
        """
        :rtype:  JobEndEvent
        """
        return cls(
            jobId=jobj.jobId(),
            time=jobj.time(),
            jobResult=jobj.jobResult().toString()
        )


class OutputMetrics(JavaClass, NamedTuple('OutputMetrics', [
    ('bytesWritten', int),
    ('recordsWritten', int)
])):
    @classmethod
    def try_convert(cls, jobj):
        """
        :rtype: OutputMetrics
        """
        return cls(**get_java_values(jobj, fields=cls._fields))


class InputMetrics(JavaClass, NamedTuple('InputMetrics', [
    ('bytesRead', int),
    ('recordsRead', int)
])):
    @classmethod
    def try_convert(cls, jobj):
        """
        :rtype: InputMetrics
        """
        return cls(**get_java_values(jobj, fields=cls._fields))


class ShuffleReadMetrics(JavaClass, NamedTuple('ShuffleReadMetrics', [
    ('fetchWaitTime', int),
    ('localBlocksFetched', int),
    ('localBytesRead', int),
    ('recordsRead', int),
    ('remoteBlocksFetched', int),
    ('remoteBytesRead', int),
    ('remoteBytesReadToDisk', int),
    ('totalBlocksFetched', int),
    ('totalBytesRead', int),
])):

    @classmethod
    def try_convert(cls, jobj):
        """
        :rtype: ShuffleReadMetrics
        """
        return cls(**get_java_values(jobj, fields=cls._fields))


class ShuffleWriteMetrics(JavaClass, NamedTuple('ShuffleWriteMetrics', [
    ('bytesWritten', int),
    ('recordsWritten', int),
    ('writeTime', int),
])):

    @classmethod
    def try_convert(cls, jobj):
        """
        :rtype: ShuffleWriteMetrics
        """
        return cls(**get_java_values(jobj, fields=cls._fields))


class TaskMetrics(JavaClass, NamedTuple('TaskMetrics', [
    # ... other fields ...
    ('outputMetrics', OutputMetrics),
    ('inputMetrics', InputMetrics),
    #  Spark 3.4+ Removed the shuffleReadMetrics and shuffleWriteMetrics
])):
    @classmethod
    def try_convert(cls, jobj):
        return cls(
            **get_java_values(jobj, fields=cls.__annotations__.keys(), exclude=('inputMetrics', 'outputMetrics')),
            inputMetrics=InputMetrics.from_java(jobj.inputMetrics()),
            outputMetrics=OutputMetrics.from_java(jobj.outputMetrics()),
            # No need to try from java for the shuffle metrics as these were removed.
        )



class StageInfo(JavaClass, NamedTuple('StageInfo', [
    ('name', str),
    ('numTasks', int),
    ('stageId', int),
    ('attemptNumber', int),
    ('submissionTime', int),
    ('completionTime', int),
    ('failureReason', str),
    ('taskMetrics', TaskMetrics),
])):

    @classmethod
    def try_convert(cls, jobj):
        """
        :rtype: StageInfo
        """
        return cls(
            **get_java_values(jobj, fields=('name', 'numTasks', 'stageId', 'attemptNumber')),
            submissionTime=from_optional(jobj.submissionTime()),
            completionTime=from_optional(jobj.completionTime()),
            failureReason=from_optional(jobj.failureReason()),
            taskMetrics=TaskMetrics.from_java(jobj.taskMetrics()),
        )


class StageCompletedEvent(JavaClass, NamedTuple('StageCompletedEvent', [
    ('stageInfo', StageInfo),
])):

    @classmethod
    def try_convert(cls, jobj):
        """
        :rtype: StageCompletedEvent
        """
        return cls(
            stageInfo=StageInfo.from_java(jobj.stageInfo())
        )
