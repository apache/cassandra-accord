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

package accord.cluster.debug.server;

import accord.debug.model.DebugServerConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ConfigLoader
{
    private static final Logger logger = LoggerFactory.getLogger(ConfigLoader.class);
    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    public static DebugServerConfig loadConfig(String configPath) throws IOException
    {
        Path path = Paths.get(configPath);
        if (!Files.exists(path))
        {
            logger.warn("Config file not found at {}, creating default config", configPath);
            DebugServerConfig defaultConfig = createDefaultConfig();
            saveConfig(defaultConfig, configPath);
            return defaultConfig;
        }
        
        try (FileReader reader = new FileReader(configPath))
        {
            DebugServerConfig config = gson.fromJson(reader, DebugServerConfig.class);
            logger.info("Loaded configuration from {}", configPath);
            return config;
        }
        catch (Exception e)
        {
            logger.error("Failed to load configuration from {}", configPath, e);
            throw new IOException("Failed to load configuration: " + e.getMessage(), e);
        }
    }
    
    public static void saveConfig(DebugServerConfig config, String configPath) throws IOException
    {
        try
        {
            String json = gson.toJson(config);
            Files.write(Paths.get(configPath), json.getBytes());
            logger.info("Saved configuration to {}", configPath);
        }
        catch (Exception e)
        {
            logger.error("Failed to save configuration to {}", configPath, e);
            throw new IOException("Failed to save configuration: " + e.getMessage(), e);
        }
    }
    
    private static DebugServerConfig createDefaultConfig()
    {
        DebugServerConfig.ServerConfig serverConfig = new DebugServerConfig.ServerConfig(8081, "0.0.0.0");
        
        java.util.List<DebugServerConfig.HostConfig> hosts = java.util.Arrays.asList(
            new DebugServerConfig.HostConfig("127.0.0.1", 9042),
            new DebugServerConfig.HostConfig("127.0.0.2", 9042),
            new DebugServerConfig.HostConfig("127.0.0.3", 9042)
        );
        
        return new DebugServerConfig(hosts, serverConfig);
    }
}