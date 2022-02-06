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
package com.dianping.cat.message.io;

import com.dianping.cat.configuration.ClientConfigManager;
import com.dianping.cat.configuration.client.entity.Server;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import javax.annotation.PostConstruct;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

@Component
@Slf4j
public class DefaultTransportManager implements TransportManager{
	@Autowired
	private ClientConfigManager clientConfigManager;

	@Autowired
	private TcpSocketSender tcpSocketSender;


	@Override
	public MessageSender getSender() {
		return tcpSocketSender;
	}

	@PostConstruct
	public void initialize()  {
		List<Server> servers = clientConfigManager.getServers();

		if (!clientConfigManager.isCatEnabled()) {
			tcpSocketSender = null;
			log.warn("CAT was DISABLED due to not initialized yet!");
		} else {
			List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();

			for (Server server : servers) {
				if (server.isEnabled()) {
					addresses.add(new InetSocketAddress(server.getIp(), server.getPort()));
				}
			}

			log.info("Remote CAT servers: " + addresses);

			if (addresses.isEmpty()) {
				throw new RuntimeException("All servers in configuration are disabled!\r\n" + servers);
			} else {
				tcpSocketSender.initialize(addresses);
			}
		}
	}

}
