package accord.burn;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

class ReconcilingOutputStreams
{
    final PrintStream matches;
    final PrintStream mismatches;
    final ReconcilingOutputStream[] streams;
    int waiting;
    int epoch;

    class ReconcilingOutputStream extends ByteArrayOutputStream
    {
        public void flush() throws IOException
        {
            synchronized (ReconcilingOutputStreams.this)
            {
                ++waiting;
                if (waiting == streams.length)
                {
                    byte[] check = streams[0].toByteArray();
                    boolean equal = true;
                    for (int i = 1; equal && i < streams.length; ++i)
                        equal = Arrays.equals(check, streams[i].toByteArray());

                    if (equal) matches.write(check);
                    else
                    {
                        mismatches.write(check);
                        for (int i = 1; i < streams.length; ++i)
                            mismatches.write(streams[i].toByteArray());
                    }

                    waiting = 0;
                    epoch++;
                    ReconcilingOutputStreams.this.notifyAll();
                }
                else
                {
                    int until = epoch + 1;
                    try
                    {
                        while (epoch < until) ReconcilingOutputStreams.this.wait();
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                }

                reset();
            }
        }
    }

    ReconcilingOutputStreams(PrintStream matches, PrintStream mismatches, int count)
    {
        this.matches = matches;
        this.mismatches = mismatches;
        this.streams = new ReconcilingOutputStream[count];
        for (int i = 0; i < count; ++i)
            streams[i] = new ReconcilingOutputStream();
    }

    ReconcilingOutputStream get(int i)
    {
        return streams[i];
    }
}
