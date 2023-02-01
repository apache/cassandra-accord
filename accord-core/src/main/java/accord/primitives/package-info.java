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
 * Routable: a RoutingKey, Key or Range. Something that can address a replica in the cluster.
 * Unseekable: a RoutingKey or Range (of RoutingKey). Something that can ONLY address a replica in the cluster.
 * Seekable: a Key or Range (of either RoutingKey or Key). Something that can address some physical data on a node.
 * Routables: a collection of Routable
 * Unseekables: a collection of Unseekable
 * Seekables: a collection of Seekable
 * Route: a collection of Routable including a homeKey. Represents a consistent slice (or slices) of token ranges.
 *        Either a PartialRoute or a FullRoute.
 */
