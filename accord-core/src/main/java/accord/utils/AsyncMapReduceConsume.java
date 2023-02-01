package accord.utils;

public interface AsyncMapReduceConsume<I, O> extends AsyncMapReduce<I, O>, ReduceConsume<O>
{
    void accept(O result, Throwable failure);
}
