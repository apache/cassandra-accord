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

import static accord.utils.Invariants.require;

/**
 * State scoped to a single request that references global state
 */
public abstract class SafeState<V>
{
    private static final byte ABANDONED_UNINITIALISED = -1;
    private static final byte UNINITIALISED = 0;
    private static final byte SAFE = 1;
    private static final byte ABANDONED_SAFE = 2;
    private static final byte RELEASED = 3;

    private byte status;
    private boolean modified;
    private byte extensionByte;
    protected V current;

    protected boolean hasChanged(V original, V updated) { return original != updated; }

    public final V current()
    {
        requireSafe();
        return current;
    }

    public final V unsafeCurrent()
    {
        return current;
    }

    public final void set(V value)
    {
        requireSafe();
        modified |= hasChanged(current, value);
        current = value;
    }

    public final boolean isUninitialised()
    {
        return status == UNINITIALISED;
    }

    public final boolean isSafe()
    {
        return status == SAFE;
    }

    public final boolean isModified()
    {
        return modified;
    }

    public final boolean isReleased()
    {
        return status == RELEASED;
    }

    public final boolean isAbandoned()
    {
        return status == ABANDONED_UNINITIALISED || status == ABANDONED_SAFE;
    }

    public final void requireUninitialised()
    {
        require(isUninitialised());
    }

    public final void requireSafe()
    {
        require(isSafe());
    }

    protected final void setSafe()
    {
        requireUninitialised();
        require(current != null);
        status = SAFE;
    }

    public final void setAbandoned()
    {
        require(status <= SAFE);
        modified = false;
        current = null;
        status = status == UNINITIALISED ? ABANDONED_UNINITIALISED : ABANDONED_SAFE;
    }

    public final boolean setReleased()
    {
        require(!isReleased());
        boolean wasLocked = status >= SAFE;
        current = null;
        status = RELEASED;
        return wasLocked;
    }

    protected final byte status()
    {
        return status;
    }

    public final byte extensionByte()
    {
        return extensionByte;
    }

    public final void setExtensionByte(byte extensionByte)
    {
        this.extensionByte = extensionByte;
    }

    public final String statusString()
    {
        switch (status)
        {
            default: return "UNKNOWN";
            case UNINITIALISED: return "UNINITIALISED";
            case SAFE: return "SAFE";
            case ABANDONED_SAFE: return "ABANDONED_SAFE";
            case ABANDONED_UNINITIALISED: return "ABANDONED_UNINITIALISED";
            case RELEASED: return "RELEASED";
        }
    }
}
