/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package accord.maelstrom;

import java.io.IOException;

import org.junit.jupiter.api.Test;

import static accord.maelstrom.Runner.test;

// TODO (correctness) : if you run the tests with the same seed, you get different outcomes... this makes it hard to rerun a failure found from CI
public class SimpleRandomTest
{
    @Test
    public void testLaunch() throws IOException
    {
        test().run();
    }

    @Test
    public void testEmptyRead() throws IOException
    {
        test().run("{\"src\":\"c1\",\"dest\":\"n1\",\"body\":{\"type\":\"txn\",\"msg_id\":1,\"txn\":"
                   + "[[\"r\", 1, null]]}}");
    }

    @Test
    public void testReadAndWrite() throws IOException
    {
        test().run("{\"src\":\"c1\",\"dest\":\"n1\",\"body\":{\"type\":\"txn\",\"msg_id\":1,\"txn\":"
                   + "[[\"r\", 1, null],[\"append\", 1, 1]]}}",
                   "{\"src\":\"c1\",\"dest\":\"n1\",\"body\":{\"type\":\"txn\",\"msg_id\":1,\"txn\":"
                   + "[[\"r\", 1, null],[\"append\", 1, 2]]}}",
                   "{\"src\":\"c1\",\"dest\":\"n1\",\"body\":{\"type\":\"txn\",\"msg_id\":1,\"txn\":"
                   + "[[\"r\", 1, null]]}}");
    }

    @Test
    public void testReadAndWriteRandomMultiKey() throws IOException
    {
        test().nodeCount(5).run("{\"src\":\"c1\",\"dest\":\"n1\",\"body\":{\"type\":\"txn\",\"msg_id\":1,\"txn\":"
                                + "[[\"r\", 1, null],[\"r\", 2, null],[\"r\", 3, null],[\"append\", 1, 1],[\"append\", 2, 3]]}}",
                                "{\"src\":\"c1\",\"dest\":\"n2\",\"body\":{\"type\":\"txn\",\"msg_id\":2,\"txn\":"
                                + "[[\"r\", 1, null],[\"r\", 2, null],[\"r\", 3, null],[\"append\", 1, 2],[\"append\", 3, 3]]}}",
                                "{\"src\":\"c1\",\"dest\":\"n3\",\"body\":{\"type\":\"txn\",\"msg_id\":3,\"txn\":"
                                + "[[\"r\", 1, null],[\"r\", 2, null],[\"r\", 3, null],[\"append\", 1, 3],[\"append\", 2, 2]]}}",
                                "{\"src\":\"c1\",\"dest\":\"n1\",\"body\":{\"type\":\"txn\",\"msg_id\":4,\"txn\":"
                                + "[[\"r\", 1, null],[\"r\", 2, null],[\"r\", 3, null],[\"append\", 1, 4],[\"append\", 3, 2]]}}",
                                "{\"src\":\"c1\",\"dest\":\"n2\",\"body\":{\"type\":\"txn\",\"msg_id\":5,\"txn\":"
                                + "[[\"r\", 1, null],[\"r\", 2, null],[\"r\", 3, null],[\"append\", 1, 5],[\"append\", 2, 1]]}}",
                                "{\"src\":\"c1\",\"dest\":\"n3\",\"body\":{\"type\":\"txn\",\"msg_id\":6,\"txn\":"
                                + "[[\"r\", 1, null],[\"r\", 2, null],[\"r\", 3, null],[\"append\", 1, 6],[\"append\", 3, 1]]}}");
    }
}
