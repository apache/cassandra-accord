package accord.local;

import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.utils.Invariants;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public interface ShardDistributor
{
    // TODO: this is overly simplistic: need to supply existing distribution,
    //  and support gradual local redistribution to keep number of shards eventually the same
    List<Ranges> split(Ranges ranges);

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
