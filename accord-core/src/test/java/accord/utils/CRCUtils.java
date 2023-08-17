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

package accord.utils;

import java.util.zip.CRC32;

public class CRCUtils
{
    private static final int POLY = 0xedb88320;

    private CRCUtils() {}

    public static int crc32LittleEnding(int key)
    {
        CRC32 crc32c = new CRC32();
        crc32c.update(key);
        crc32c.update(key >> 8);
        crc32c.update(key >> 16);
        crc32c.update(key >> 24);
        return (int) crc32c.getValue();
    }

    public static int reverseCRC32LittleEnding(int c)
    {
        for (int i = 0; i < 32; i++)
        {
            c = (c >>> 31) == 0 ?
                (c ^ POLY) << 1 | 1
                : c << 1;
        }
        // flip bits
        c = c ^ 0xffffffff;
        return c;
    }
}
