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

package accord.api;

/**
 * The result of having performed a read at some node and potentially reading incomplete data
 * not written by Accord
 */
public interface UnresolvedData
{
    /**
     * Combine the contents of the parameter with this object and return the resultant object.
     * This method may modify the current object and return itself.
     */
    UnresolvedData merge(UnresolvedData data);

    static UnresolvedData mergeForReduce(UnresolvedData left, UnresolvedData right)
    {
        if (left == null)
            return right;
        if (right == null)
            return null;

        return left.merge(right);
    }
}
