# #
# # Licensed to the Apache Software Foundation (ASF) under one or more
# # contributor license agreements.  See the NOTICE file distributed with
# # this work for additional information regarding copyright ownership.
# # The ASF licenses this file to You under the Apache License, Version 2.0
# # (the "License"); you may not use this file except in compliance with
# # the License.  You may obtain a copy of the License at
# #
# #    http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.
# #

# import unittest

# from pyspark.testing.connectutils import should_test_connect
# from pyspark.sql.tests.streaming.test_streaming_foreach_family import StreamingTestsForeachFamilyMixin
# from pyspark.testing.connectutils import ReusedConnectTestCase

# class StreamingTests(StreamingTestsForeachFamilyMixin, ReusedConnectTestCase):
#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreach_with_simple_function(self):
#         super().test_streaming_foreach_with_simple_function()

#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreach_with_basic_open_process_close(self):
#         super().test_streaming_foreach_with_basic_open_process_close()

#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreach_with_open_returning_false(self):
#         super().test_streaming_foreach_with_open_returning_false()

#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreach_without_open_method(self):
#         super().test_streaming_foreach_without_open_method()

#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreach_without_close_method(self):
#         super().test_streaming_foreach_without_close_method()

#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreach_with_process_throwing_error(self):
#         super().test_streaming_foreach_with_process_throwing_error()

#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreach_with_invalid_writers(self):
#         super().test_streaming_foreach_with_invalid_writers()

#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreachBatch(self):
#         super().test_streaming_foreachBatch()

#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreachBatch_tempview(self):
#         super().test_streaming_foreachBatch_tempview()

#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreachBatch_propagates_python_errors(self):
#         super().test_streaming_foreachBatch_propagates_python_errors()

#     @unittest.skip("Streaming foreach and foreachBatch are going to be supported in the future.")
#     def test_streaming_foreachBatch_graceful_stop(self):
#         super().test_streaming_foreachBatch_graceful_stop()

# if __name__ == "__main__":
#     import unittest
#     from pyspark.sql.tests.connect.streaming.test_parity_streaming_foreach_family import *  # noqa: F401

#     try:
#         import xmlrunner  # type: ignore[import]

#         testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
#     except ImportError:
#         testRunner = None
#     unittest.main(testRunner=testRunner, verbosity=2)
