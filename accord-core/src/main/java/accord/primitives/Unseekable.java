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

package accord.primitives;

/**
 * Something that can only be routed.
 * In particular, it purposely does not contain enough information to found it on disk (in contrast to {@link Seekable}).
 * Those two interfaces were created to explicitly distinguish what information it is intended to be serialized when
 * exchanging data between nodes. {@link Seekable} contains more information, but it is not always necessary to send it.
 */
public interface Unseekable extends Routable
{
}
