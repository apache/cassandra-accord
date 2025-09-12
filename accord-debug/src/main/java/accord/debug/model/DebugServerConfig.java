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

package accord.debug.model;

import java.util.List;

public class DebugServerConfig
{
    private List<HostConfig> hosts;
    private ServerConfig server;

    public DebugServerConfig(List<HostConfig> hosts, ServerConfig server)
    {
        this.hosts = hosts;
        this.server = server;
    }
    
    public List<HostConfig> getHosts()
    {
        return hosts;
    }
    
    public void setHosts(List<HostConfig> hosts)
    {
        this.hosts = hosts;
    }
    
    public ServerConfig getServer()
    {
        return server;
    }
    
    public void setServer(ServerConfig server)
    {
        this.server = server;
    }
    
    public static class HostConfig
    {
        public final String host;
        public final int port;

        public HostConfig(String host, int port)
        {
            this.host = host;
            this.port = port;
        }

        public String toString()
        {
            return String.format("%s:%d", host, port);
        }
    }
    
    public static class ServerConfig
    {
        public final int port;
        public final String host;

        public ServerConfig(int port, String host)
        {
            this.port = port;
            this.host = host;
        }
    }
}