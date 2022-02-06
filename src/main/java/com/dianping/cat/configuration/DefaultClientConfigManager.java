/*
 * Copyright (c) 2011-2018, Meituan Dianping. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dianping.cat.configuration;

import com.dianping.cat.Cat;
import com.dianping.cat.configuration.client.entity.ClientConfig;
import com.dianping.cat.configuration.client.entity.Domain;
import com.dianping.cat.configuration.client.entity.Server;
import com.dianping.cat.configuration.client.transform.DefaultSaxParser;
import com.dianping.cat.message.spi.MessageTree;
import com.site.helper.JsonBuilder;
import com.site.helper.Splitters;
import com.site.utils.Files;
import com.site.utils.Urls;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;


import javax.annotation.PostConstruct;
import java.io.File;
import java.io.InputStream;
import java.util.*;
@Slf4j
@Component
@Configuration
public class DefaultClientConfigManager implements  ClientConfigManager {

	@Value("${spring.profiles.active}")
	private String env;

	@Value("${spring.application.name}")
	private String applicationName;

	private final String PROD_SERVER = "collect.hawk.lianjia.com";
	private final String TEST_SERVER = "test-collect.hawk.lianjia.com";
	private ClientConfig m_config;

	private volatile double m_sampleRate = 1d;

	private volatile boolean m_block = false;

	private String m_routers;

	private JsonBuilder m_jsonBuilder = new JsonBuilder();

	private AtomicTreeParser m_atomicTreeParser = new AtomicTreeParser();

	private Map<String, List<Integer>> m_longConfigs = new LinkedHashMap<String, List<Integer>>();


	@Override
	public Domain getDomain() {
		Domain domain = null;

		if (m_config != null) {
			Map<String, Domain> domains = m_config.getDomains();

			domain = domains.isEmpty() ? null : domains.values().iterator().next();
		}

		if (domain != null) {
			return domain;
		} else {
			return new Domain("UNKNOWN").setEnabled(false);
		}
	}

	@Override
	public int getMaxMessageLength() {
		if (m_config == null) {
			return 5000;
		} else {
			return getDomain().getMaxMessageSize();
		}
	}

	@Override
	public String getRouters() {
		if (m_routers == null) {
			refreshConfig();
		}
		return m_routers;
	}

	public double getSampleRatio() {
		return m_sampleRate;
	}

	private String getServerConfigUrl() {
		String host  = env.contains("prod")? "hawk.lianjia.com" : "test-hawk.lianjia.com";
		return String.format("http://%s/cat/s/router?domain=%s&ip=%s&op=json", host,
									getDomain().getId(), NetworkInterfaceManager.INSTANCE.getLocalHostAddress());
	}

	@Override
	public List<Server> getServers() {
		if (m_config == null) {
			return Collections.emptyList();
		} else {
			return m_config.getServers();
		}
	}

	@Override
	public int getTaggedTransactionCacheSize() {
		return 1024;
	}


	@PostConstruct
	public void initialize()  {
			ClientConfig config = new ClientConfig();
			config.addDomain(new Domain(applicationName.trim()));
			config.accept(new ClientConfigValidator());
			String host = env.contains("prod")? PROD_SERVER: TEST_SERVER;
			int socketPort = env.contains("prod")? 8101 :14064;
			Server server = new Server();
			server.setIp(host);
			server.setPort(socketPort);
			config.addServer(server);
			m_config = config;
			refreshConfig();
	}

	@Override
	public boolean isAtomicMessage(MessageTree tree) {
		return m_atomicTreeParser.isAtomicMessage(tree);
	}

	public boolean isBlock() {
		return m_block;
	}

	@Override
	public boolean isCatEnabled() {
		if (m_config == null) {
			return false;
		} else {
			return m_config.isEnabled();
		}
	}

	@Override
	public boolean isDumpLocked() {
		if (m_config == null) {
			return false;
		} else {
			return m_config.isDumpLocked();
		}
	}


	public void refreshConfig() {
		String url = getServerConfigUrl();

		try {
			InputStream inputstream = Urls.forIO().readTimeout(2000).connectTimeout(1000).openStream(url);
			String content = Files.forIO().readFrom(inputstream, "utf-8");
			KVConfig routerConfig = (KVConfig) m_jsonBuilder.parse(content.trim(), KVConfig.class);

			m_routers = routerConfig.getValue("routers");
			m_block = Boolean.valueOf(routerConfig.getValue("block").trim());

			m_sampleRate = Double.valueOf(routerConfig.getValue("sample").trim());
			if (m_sampleRate <= 0) {
				m_sampleRate = 0;
			}

			String startTypes = routerConfig.getValue("startTransactionTypes");
			String matchTypes = routerConfig.getValue("matchTransactionTypes");

			m_atomicTreeParser.init(startTypes, matchTypes);

			for (ProblemLongType longType : ProblemLongType.values()) {
				final String name = longType.getName();
				String propertyName = name + "s";
				String values = routerConfig.getValue(propertyName);

				if (values != null) {
					List<String> valueStrs = Splitters.by(',').trim().split(values);
					List<Integer> thresholds = new LinkedList<Integer>();

					for (String valueStr : valueStrs) {
						try {
							thresholds.add(Integer.parseInt(valueStr));
						} catch (Exception e) {
							// ignore
						}
					}
					if (!thresholds.isEmpty()) {
						m_longConfigs.put(name, thresholds);
					}
				}
			}
		} catch (Exception e) {
			log.warn("error when connect cat server config url " + url);
		}
	}

	@Override
	public int getLongThresholdByDuration(String key, int duration) {
		List<Integer> values = m_longConfigs.get(key);

		if (values != null) {
			for (int i = values.size() - 1; i >= 0; i--) {
				int userThreshold = values.get(i);

				if (duration >= userThreshold) {
					return userThreshold;
				}
			}
		}

		return -1;
	}

}
