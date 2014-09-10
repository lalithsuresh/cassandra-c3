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
package org.apache.cassandra.locator;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SeedProviderDef;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Loader;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;

public class SimpleSeedProvider implements SeedProvider
{
    private static final Logger logger = LoggerFactory.getLogger(SimpleSeedProvider.class);

    List<InetAddress> seeds;
    public SimpleSeedProvider(Map<String, String> args) {
        try
        {
            seeds = loadSeeds();
        }
        catch (Exception e)
        {
            throw new AssertionError(e);
        }
    }

    public List<InetAddress> getSeeds()
    {
        try
        {
            seeds = loadSeeds();
        }
        catch (Exception e)
        {
            logger.warn("Could not refresh seeds from configuration file: {}", e);
        }
        return Collections.unmodifiableList(seeds);
    }

    private List<InetAddress> loadSeeds() throws IOException, ConfigurationException
    {
        Config conf = DatabaseDescriptor.loadConfig();
        String[] hosts = conf.seed_provider.parameters.get("seeds").split(",", -1);
        List<InetAddress> seeds = new ArrayList<InetAddress>(hosts.length);
        for (String host : hosts)
        {
            try
            {
                seeds.add(InetAddress.getByName(host.trim()));
            }
            catch (UnknownHostException ex)
            {
                // not fatal... DD will bark if there end up being zero seeds.
                logger.warn("Seed provider couldn't lookup host " + host);
            }
        }
        return seeds;
    }
}
