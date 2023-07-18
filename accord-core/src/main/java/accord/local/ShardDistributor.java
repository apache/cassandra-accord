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

package accord.local;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.utils.Invariants;

public interface ShardDistributor
{
    // TODO (expected, topology): this is overly simplistic: need to supply existing distribution, and support
    //                            gradual local redistribution to keep number of shards eventually the same
    List<Ranges> split(Ranges ranges);

    /**
     * Return a single subSpit from a range given the total number of splits the range should be divided into and
     * the index of the split to be returned.
     *
     * If the range is not sufficiently divisible then the range will be returned unmodified.
     */
    Range splitRange(Range range, int splitIndex, int totalSplits);

    class EvenSplit<T> implements ShardDistributor
    {
        public interface Splitter<T>
        {
            T sizeOf(Range range);
            Range subRange(Range range, T start, T end);

            T zero();
            T add(T a, T b);
            T subtract(T a, T b);
            T divide(T a, int i);
            T multiply(T a, int i);
            int min(T v, int i);
            int compare(T a, T b);
        }

        final int numberOfShards;
        final Function<Ranges, ? extends Splitter<T>> splitter;

        public EvenSplit(int numberOfShards, Function<Ranges, ? extends Splitter<T>> splitter)
        {
            this.numberOfShards = numberOfShards;
            this.splitter = splitter;
        }

        @Override
        public Range splitRange(Range range, int splitIndex, int totalSplits)
        {
            Invariants.checkArgument(splitIndex < totalSplits);
            Splitter<T> splitter = this.splitter.apply(Ranges.single(range));
            T size = splitter.sizeOf(range);
            T splitSize = splitter.divide(size, totalSplits);
            // Small ranges aren't divisible
            if (splitter.compare(splitSize, splitter.zero()) <= 0)
                return range;
            T splitBegin = splitter.multiply(splitSize, splitIndex);
            // For the last split use size as the end in case there is an issue with division and remainder
            T splitEnd = splitIndex == (totalSplits - 1) ? size : splitter.add(splitBegin, splitSize);
            return splitter.subRange(range, splitBegin, splitEnd);
        }

        @Override
        public List<Ranges> split(Ranges ranges)
        {
            if (ranges.isEmpty())
                return Collections.emptyList();

            Splitter<T> splitter = this.splitter.apply(ranges);
            T totalSize = splitter.zero();
            for (int i = 0 ; i < ranges.size() ; ++i)
                totalSize = splitter.add(totalSize, splitter.sizeOf(ranges.get(i)));

            if (splitter.compare(totalSize, splitter.zero()) <= 0)
                throw new IllegalStateException();

            int numberOfShards = splitter.min(totalSize, this.numberOfShards);
            T shardSize = splitter.divide(totalSize, numberOfShards);
            List<Range> buffer = new ArrayList<>(ranges.size());
            List<Ranges> result = new ArrayList<>(numberOfShards);
            int ri = 0;
            T rOffset = splitter.zero();
            T rSize = splitter.sizeOf(ranges.get(0));
            while (result.size() < numberOfShards)
            {
                T required = result.size() < numberOfShards - 1 ? shardSize : splitter.subtract(totalSize, splitter.multiply(shardSize, (numberOfShards - 1)));
                while (true)
                {
                    if (splitter.compare(splitter.subtract(rSize, rOffset), required) >= 0)
                    {
                        buffer.add(splitter.subRange(ranges.get(ri), rOffset, splitter.add(rOffset, required)));
                        result.add(Ranges.ofSortedAndDeoverlapped(buffer.toArray(new Range[0])));
                        buffer.clear();
                        rOffset = splitter.add(rOffset, required);
                        if (splitter.compare(rOffset, rSize) >= 0 && ++ri < ranges.size())
                        {
                            Invariants.checkState(splitter.compare(rOffset, rSize) == 0);
                            rOffset = splitter.zero();
                            rSize = splitter.sizeOf(ranges.get(ri));
                        }
                        break;
                    }
                    else
                    {
                        buffer.add(splitter.subRange(ranges.get(ri), rOffset, rSize));
                        required = splitter.subtract(required, splitter.subtract(rSize, rOffset));
                        rOffset = splitter.zero();
                        rSize = splitter.sizeOf(ranges.get(++ri));
                    }
                }
            }
            return result;
        }
    }
}
