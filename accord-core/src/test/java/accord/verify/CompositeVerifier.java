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

package accord.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompositeVerifier implements Verifier
{
    private final List<Verifier> delegates;

    private CompositeVerifier(List<Verifier> delegates)
    {
        this.delegates = delegates;
    }

    public static Verifier create(Verifier... verifiers)
    {
        return create(Arrays.asList(verifiers));
    }

    public static Verifier create(List<Verifier> verifiers)
    {
        switch (verifiers.size())
        {
            case 0:  throw new IllegalArgumentException("Unable to create Verifier from nothing");
            case 1:  return verifiers.get(0);
            default: return new CompositeVerifier(verifiers);
        }
    }

    @Override
    public Checker witness(int start, int end)
    {
        List<Checker> sub = new ArrayList<>(delegates.size());
        delegates.forEach(v -> sub.add(v.witness(start, end)));
        return new Checker()
        {
            @Override
            public void read(int index, int[] seq)
            {
                sub.forEach(c -> c.read(index, seq));
            }

            @Override
            public void write(int index, int value)
            {
                sub.forEach(c -> c.write(index, value));
            }

            @Override
            public void close()
            {
                sub.forEach(Checker::close);
            }
        };
    }

    @Override
    public void close()
    {
        Verifier.super.close();
    }
}
