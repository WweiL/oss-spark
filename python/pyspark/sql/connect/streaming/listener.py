#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import uuid
import json
from typing import Any, Optional, Dict, List
from abc import ABC, abstractmethod

__all__ = ["StreamingQueryListener"]

# TODO: add a abstract class to listener?
class StreamingQueryListener(ABC):
    """
    Interface for listening to events related to :class:`~pyspark.sql.streaming.StreamingQuery`.

    .. versionadded:: 3.5.0

    Notes
    -----
    The methods are not thread-safe as they may be called from different threads.
    The events received are identical with Scala API. Refer to its documentation.

    This API is evolving.

    Examples
    --------
    >>> class MyListener(StreamingQueryListener):
    ...    def onQueryStarted(self, event: QueryStartedEvent) -> None:
    ...        # Do something with event.
    ...        pass
    ...
    ...    def onQueryProgress(self, event: QueryProgressEvent) -> None:
    ...        # Do something with event.
    ...        pass
    ...
    ...    def onQueryIdle(self, event: QueryIdleEvent) -> None:
    ...        # Do something with event.
    ...        pass
    ...
    ...    def onQueryTerminated(self, event: QueryTerminatedEvent) -> None:
    ...        # Do something with event.
    ...        pass
    ...
    >>> spark.streams.addListener(MyListener())
    """

    @abstractmethod
    def onQueryStarted(self, event: "QueryStartedEvent") -> None:
        """
        Called when a query is started.

        Notes
        -----
        This is called synchronously with :py:meth:`~pyspark.sql.streaming.DataStreamWriter.start`,
        that is, `onQueryStart` will be called on all listeners before `DataStreamWriter.start()`
        returns the corresponding :class:`~pyspark.sql.streaming.StreamingQuery`.
        Please don't block this method as it will block your query.
        """
        pass

    @abstractmethod
    def onQueryProgress(self, event: "QueryProgressEvent") -> None:
        """
        Called when there is some status update (ingestion rate updated, etc.)

        Notes
        -----
        This method is asynchronous. The status in :class:`~pyspark.sql.streaming.StreamingQuery`
        will always be latest no matter when this method is called. Therefore, the status of
        :class:`~pyspark.sql.streaming.StreamingQuery`.
        may be changed before/when you process the event. E.g., you may find
        :class:`~pyspark.sql.streaming.StreamingQuery` is terminated when you are
        processing `QueryProgressEvent`.
        """
        pass

    @abstractmethod
    def onQueryIdle(self, event: "QueryIdleEvent") -> None:
        """
        Called when the query is idle and waiting for new data to process.
        """
        pass

    @abstractmethod
    def onQueryTerminated(self, event: "QueryTerminatedEvent") -> None:
        """
        Called when a query is stopped, with or without error.
        """
        pass


class QueryStartedEvent:
    """
    Event representing the start of a query.

    .. versionadded:: 3.5.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, id: str, runId: str, name: Optional[str], timestamp: str) -> None:
        self._id: str = id
        self._runId: str = runId
        self._name: Optional[str] = name
        self._timestamp: str = timestamp

    # TODO (wei): change back to UUID? what about connect/StreamingQuery
    @property
    def id(self) -> str:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._id

    @property
    def runId(self) -> str:
        """
        A query id that is unique for every start/restart. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.runId`.
        """
        return self._runId

    @property
    def name(self) -> Optional[str]:
        """
        User-specified name of the query, `None` if not specified.
        """
        return self._name

    @property
    def timestamp(self) -> str:
        """
        The timestamp to start a query.
        """
        return self._timestamp

    @classmethod
    def fromJson(cls, j) -> "QueryStartedEvent":
        return cls(j["id"], j["runId"], j["name"], j["timestamp"])


class QueryIdleEvent:
    """
    Event representing that query is idle and waiting for new data to process.

    .. versionadded:: 3.5.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, id: str, runId: str, name: Optional[str], timestamp: str) -> None:
        self._id: str = id
        self._runId: str = runId
        self._name: Optional[str] = name
        self._timestamp: str = timestamp

    @property
    def id(self) -> str:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._id

    @property
    def runId(self) -> str:
        """
        A query id that is unique for every start/restart. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.runId`.
        """
        return self._runId

    @property
    def name(self) -> Optional[str]:
        """
        User-specified name of the query, `None` if not specified.
        """
        return self._name

    @property
    def timestamp(self) -> str:
        """
        The timestamp to start a query.
        """
        return self._timestamp

    @classmethod
    def fromJson(cls, j) -> "QueryStartedEvent":
        return cls(j["id"], j["runId"], j["name"], j["timestamp"])


class QueryProgressEvent:
    """
    Event representing any progress updates in a query.

    .. versionadded:: 3.5.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, progress: "StreamingQueryProgress") -> None:
        self._progress: StreamingQueryProgress = progress

    @property
    def progress(self) -> "StreamingQueryProgress":
        """
        The query progress updates.
        """
        return self._progress

    @classmethod
    def fromJson(cls, j) -> "QueryProgressEvent":
        return cls(QueryProgressEvent.fromJson(j["progress"]))

class QueryIdleEvent:
    """
    Event representing that query is idle and waiting for new data to process.

    .. versionadded:: 3.5.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, id: str, runId: str, name: Optional[str], timestamp: str) -> None:
        self._id: str = id
        self._runId: str = runId
        self._name: Optional[str] = name
        self._timestamp: str = timestamp

    @property
    def id(self) -> uuid.UUID:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._id

    @property
    def runId(self) -> uuid.UUID:
        """
        A query id that is unique for every start/restart. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.runId`.
        """
        return self._runId

    @property
    def timestamp(self) -> str:
        """
        The timestamp when the latest no-batch trigger happened.
        """
        return self._timestamp

    @classmethod
    def fromJson(cls, j) -> "QueryIdleEvent":
        return cls(j["id"], j["runId"], j["name"], j["timestamp"])

class QueryTerminatedEvent:
    """
    Event representing that termination of a query.

    .. versionadded:: 3.5.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(self, id: str, runId: str, exception: Optional[str], errorClassOnException: Optional[str]) -> None:
        self._id: str = id
        self._runId: str = runId
        self._exception: Optional[str] = exception
        self._errorClassOnException: Optional[str] = errorClassOnException

    @property
    def id(self) -> uuid.UUID:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._id

    @property
    def runId(self) -> uuid.UUID:
        """
        A query id that is unique for every start/restart. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.runId`.
        """
        return self._runId

    @property
    def exception(self) -> Optional[str]:
        """
        The exception message of the query if the query was terminated
        with an exception. Otherwise, it will be `None`.
        """
        return self._exception

    @property
    def errorClassOnException(self) -> Optional[str]:
        """
        The error class from the exception if the query was terminated
        with an exception which is a part of error class framework.
        If the query was terminated without an exception, or the
        exception is not a part of error class framework, it will be
        `None`.

        .. versionadded:: 3.5.0
        """
        return self._errorClassOnException

    @classmethod
    def fromJson(cls, j) -> "QueryTerminatedEvent":
        return cls(j["id"], j["runId"], j["exception"], j["errorClassOnException"])


class StreamingQueryProgress:
    """
    .. versionadded:: 3.5.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(
        self,
        jsonDict: Dict[str, Any],
        id: str,
        runId: str,
        name: Optional[str],
        timestamp: str,
        batchId: int,
        batchDuration: int,
        durationMs: Dict[str, int],
        eventTime: Dict[str, str],
        stateOperators: List[StateOperatorProgress],
        sources: List[SourceProgress],
        sink: SinkProgress,
        # observedMetrics: Dict[str, Row] # TODO: This?
    ):
        self._jsonDict: Dict[str, Any] = jsonDict
        self._id: str = id
        self._runId: str = runId
        self._name: Optional[str] = name
        self._timestamp: str = timestamp
        self._batchId: int = batchId
        self._batchDuration: int = batchDuration
        self._durationMs: Dict[str, int] = durationMs
        self._eventTime: Dict[str, str] = eventTime
        self._stateOperators: List[StateOperatorProgress] = stateOperators
        self._sources: List[SourceProgress] = sources
        self._sink: SinkProgress = sink

        # self._observedMetrics: Dict[str, Row] = {
        #     k: cloudpickle.loads(
        #         SparkContext._jvm.PythonSQLUtils.toPyRow(jr)  # type: ignore[union-attr]
        #     )
        #     for k, jr in dict(jprogress.observedMetrics()).items()
        }

    @property
    def id(self) -> uuid.UUID:
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._id

    @property
    def runId(self) -> uuid.UUID:
        """
        A query id that is unique for every start/restart. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.runId`.
        """
        return self._runId

    @property
    def name(self) -> Optional[str]:
        """
        User-specified name of the query, `None` if not specified.
        """
        return self._name

    @property
    def timestamp(self) -> str:
        """
        The timestamp to start a query.
        """
        return self._timestamp

    @property
    def batchId(self) -> int:
        """
        A unique id for the current batch of data being processed.  Note that in the
        case of retries after a failure a given batchId my be executed more than once.
        Similarly, when there is no data to be processed, the batchId will not be
        incremented.
        """
        return self._batchId

    @property
    def batchDuration(self) -> int:
        """
        The process duration of each batch.
        """
        return self._batchDuration

    @property
    def durationMs(self) -> Dict[str, int]:
        """
        The amount of time taken to perform various operations in milliseconds.
        """
        return self._durationMs

    @property
    def eventTime(self) -> Dict[str, str]:
        """
        Statistics of event time seen in this batch. It may contain the following keys:

        .. code-block:: python

            {
                "max": "2016-12-05T20:54:20.827Z",  # maximum event time seen in this trigger
                "min": "2016-12-05T20:54:20.827Z",  # minimum event time seen in this trigger
                "avg": "2016-12-05T20:54:20.827Z",  # average event time seen in this trigger
                "watermark": "2016-12-05T20:54:20.827Z"  # watermark used in this trigger
            }

        All timestamps are in ISO8601 format, i.e. UTC timestamps.
        """
        return self._eventTime

    @property
    def stateOperators(self) -> List["StateOperatorProgress"]:
        """
        Information about operators in the query that store state.
        """
        return self._stateOperators

    @property
    def sources(self) -> List["SourceProgress"]:
        """
        detailed statistics on data being read from each of the streaming sources.
        """
        return self._sources

    @property
    def sink(self) -> "SinkProgress":
        """
        A unique query id that persists across restarts. See
        py:meth:`~pyspark.sql.streaming.StreamingQuery.id`.
        """
        return self._sink

    # @property
    # def observedMetrics(self) -> Dict[str, Row]:
    #     return self._observedMetrics

    @property
    def numInputRows(self) -> Optional[str]:
        """
        The aggregate (across all sources) number of records processed in a trigger.
        """
        return self._jprogress.numInputRows()

    @property
    def inputRowsPerSecond(self) -> str:
        """
        The aggregate (across all sources) rate of data arriving.
        """
        return self._jprogress.inputRowsPerSecond()

    @property
    def processedRowsPerSecond(self) -> str:
        """
        The aggregate (across all sources) rate at which Spark is processing data..
        """
        return self._jprogress.processedRowsPerSecond()

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        return json.dumps(self._jsonDict)

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        return json.dumps(self._jsonDict, indent=4)

    def __str__(self) -> str:
        return self.prettyJson

    @classmethod
    def fromJson(cls, j) -> "StreamingQueryProgress":
        return cls(
            j,
            j["id"],
            j["runId"],
            j["name"],
            j["timestamp"],
            j["batchId"],
            j["batchDuration"],
            j["durationMs"],
            j["eventTime"],
            [StateOperatorProgress(s) for s in j["stateOperators"]],
            [SourceProgress(s) for s in j["sources"]],
            j["sink"],
        )

    class StateOperatorProgress:
        """
        .. versionadded:: 3.4.0

        Notes
        -----
        This API is evolving.
        """

        def __init__(
            self,
            jsonDict: Dict[str, Any],
            operatorName: str,
            numRowsTotal: int,
            numRowsUpdated: int,
            numRowsRemoved: int,
            allUpdatesTimeMs: int,
            allRemovalsTimeMs: int,
            commitTimeMs: int,
            memoryUsedBytes: int,
            numRowsDroppedByWatermark: int,
            numShufflePartitions: int,
            numStateStoreInstances: int,
            customMetrics: Dict[str, int],
        ):
            self._jsonDict: Dict[str, Any] = jsonDict
            self._operatorName: str = operatorName
            self._numRowsTotal: int = numRowsTotal
            self._numRowsUpdated: int = numRowsUpdated
            self._numRowsRemoved: int = numRowsRemoved
            self._allUpdatesTimeMs: int = allUpdatesTimeMs
            self._allRemovalsTimeMs: int = allRemovalsTimeMs
            self._commitTimeMs: int = commitTimeMs
            self._memoryUsedBytes: int = memoryUsedBytes
            self._numRowsDroppedByWatermark: int = numRowsDroppedByWatermark
            self._numShufflePartitions: int = numShufflePartitions
            self._numStateStoreInstances: int = numStateStoreInstances
            self._customMetrics: Dict[str, int] = customMetrics

        @property
        def operatorName(self) -> str:
            return self._operatorName

        @property
        def numRowsTotal(self) -> int:
            return self._numRowsTotal

        @property
        def numRowsUpdated(self) -> int:
            return self._numRowsUpdated

        @property
        def allUpdatesTimeMs(self) -> int:
            return self._allUpdatesTimeMs

        @property
        def numRowsRemoved(self) -> int:
            return self._numRowsRemoved

        @property
        def allRemovalsTimeMs(self) -> int:
            return self._allRemovalsTimeMs

        @property
        def commitTimeMs(self) -> int:
            return self._commitTimeMs

        @property
        def memoryUsedBytes(self) -> int:
            return self._memoryUsedBytes

        @property
        def numRowsDroppedByWatermark(self) -> int:
            return self._numRowsDroppedByWatermark

        @property
        def numShufflePartitions(self) -> int:
            return self._numShufflePartitions

        @property
        def numStateStoreInstances(self) -> int:
            return self._numStateStoreInstances

        @property
        def customMetrics(self) -> Dict[str, int]:
            return self._customMetrics

        @property
        def json(self) -> str:
            """
            The compact JSON representation of this progress.
            """
            return json.dumps(self._jsonDict)

        @property
        def prettyJson(self) -> str:
            """
            The pretty (i.e. indented) JSON representation of this progress.
            """
            return json.dumps(self._jsonDict, indent=4)

        def __str__(self) -> str:
            return self.prettyJson

        @classmethod
        def fromJson(cls, j) -> "StateOperatorProgress":
            return cls(
                j,
                j["operatorName"],
                j["numRowsTotal"],
                j["numRowsUpdated"],
                j["numRowsRemoved"],
                j["allUpdatesTimeMs"],
                j["allRemovalsTimeMs"],
                j["commitTimeMs"],
                j["memoryUsedBytes"],
                j["numRowsDroppedByWatermark"],
                j["numShufflePartitions"],
                j["numStateStoreInstances"],
                j["customMetrics"],
            )

class SourceProgress:
    """
    .. versionadded:: 3.5.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(
        self,
        jsonDict: Dict[str, Any],
        description: str,
        startOffset: str,
        endOffset: str,
        latestOffset: str,
        numInputRows: int,
        inputRowsPerSecond: float,
        processedRowsPerSecond: float,
        metrics: Dict[str, str]
    ) -> None:
        self._jsonDict: Dict[str, Any] = jsonDict
        self._description: str = description
        self._startOffset: str = startOffset
        self._endOffset: str = endOffset
        self._latestOffset: str = latestOffset
        self._numInputRows: int = numInputRows
        self._inputRowsPerSecond: float = inputRowsPerSecond
        self._processedRowsPerSecond: float = processedRowsPerSecond
        self._metrics: Dict[str, str] = metrics

    @property
    def description(self) -> str:
        """
        Description of the source.
        """
        return self._description

    @property
    def startOffset(self) -> str:
        """
        The starting offset for data being read.
        """
        return self._startOffset

    @property
    def endOffset(self) -> str:
        """
        The ending offset for data being read.
        """
        return self._endOffset

    @property
    def latestOffset(self) -> str:
        """
        The latest offset from this source.
        """
        return self._latestOffset

    @property
    def numInputRows(self) -> int:
        """
        The number of records read from this source.
        """
        return self._numInputRows

    @property
    def inputRowsPerSecond(self) -> float:
        """
        The rate at which data is arriving from this source.
        """
        return self._inputRowsPerSecond

    @property
    def processedRowsPerSecond(self) -> float:
        """
        The rate at which data from this source is being processed by Spark.
        """
        return self._processedRowsPerSecond

    @property
    def metrics(self) -> Dict[str, str]:
        return self._metrics

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        return json.dumps(self._jsonDict)

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        return json.dumps(self._jsonDict, indent=4)

    def __str__(self) -> str:
        return self.prettyJson

    @classmethod
    def fromJson(cls, j) -> "SourceProgress":
        return cls(
            j,
            j["description"],
            j["startOffset"],
            j["endOffset"],
            j["latestOffset"],
            j["numInputRows"],
            j["inputRowsPerSecond"],
            j["processedRowsPerSecond"],
            j["metrics"]
        )

class SinkProgress:
    """
    .. versionadded:: 3.4.0

    Notes
    -----
    This API is evolving.
    """

    def __init__(
        self,
        jsonDict: Dict[str, Any],
        description: str,
        numOutputRows: int,
        metrics: Dict[str, str]
    ) -> None:
        self._jsonDict: Dict[str, Any] = jsonDict
        self._description: str = description
        self._numOutputRows: int = numOutputRows
        self._metrics: Dict[str, str] = metrics

    @property
    def description(self) -> str:
        """
        Description of the source.
        """
        return self._description

    @property
    def numOutputRows(self) -> int:
        """
        Number of rows written to the sink or -1 for Continuous Mode (temporarily)
        or Sink V1 (until decommissioned).
        """
        return self._numOutputRows

    @property
    def metrics(self) -> Dict[str, str]:
        return self._metrics

    @property
    def json(self) -> str:
        """
        The compact JSON representation of this progress.
        """
        return json.dumps(self._jsonDict)

    @property
    def prettyJson(self) -> str:
        """
        The pretty (i.e. indented) JSON representation of this progress.
        """
        return json.dumps(self._jsonDict, indent=4)

    def __str__(self) -> str:
        return self.prettyJson

    @classmethod
    def fromJson(cls, j) -> "SinkProgress":
        return cls(
            j,
            j["description"],
            j["numOutputRows"],
            j["metrics"]
        )
