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
import unittest

from pyspark.pandas.tests.series.test_compute import SeriesComputeMixin
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils


class SeriesParityComputeTests(SeriesComputeMixin, PandasOnSparkTestUtils, ReusedConnectTestCase):
    @unittest.skip(
        "TODO(SPARK-43610): Enable `InternalFrame.attach_distributed_column` in Spark Connect."
    )
    def test_at_time(self):
        super().test_at_time()

    @unittest.skip(
        "TODO(SPARK-43610): Enable `InternalFrame.attach_distributed_column` in Spark Connect."
    )
    def test_between_time(self):
        super().test_between_time()

    @unittest.skip("TODO(SPARK-43663): Enable SeriesParityTests.test_compare.")
    def test_compare(self):
        super().test_compare()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_diff(self):
        super().test_diff()

    @unittest.skip("TODO(SPARK-43620): Support `Column` for SparkConnectColumn.__getitem__.")
    def test_factorize(self):
        super().test_factorize()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_shift(self):
        super().test_shift()

    @unittest.skip(
        "TODO(SPARK-43611): Fix unexpected `AnalysisException` from Spark Connect client."
    )
    def test_unstack(self):
        super().test_unstack()


if __name__ == "__main__":
    from pyspark.pandas.tests.connect.series.test_parity_compute import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
