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
import com.dianping.cat.message.internal.MessageIdFactory;
import com.dianping.cat.util.Threads;
import com.site.helper.Splitters;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;


import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
@Slf4j
public class ChannelManager implements Threads.Task {

	private ClientConfigManager m_configManager;

	private Bootstrap m_bootstrap;

	private boolean m_active = true;

	private int m_channelStalledTimes = 0;

	private ChannelHolder m_activeChannelHolder;

	private MessageIdFactory m_idFactory;

	private AtomicInteger m_attempts = new AtomicInteger();



	public ChannelManager( List<InetSocketAddress> serverAddresses, ClientConfigManager configManager,
							MessageIdFactory idFactory) {

		m_configManager = configManager;
		m_idFactory = idFactory;

		EventLoopGroup group = new NioEventLoopGroup(1, new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r);
				t.setDaemon(true);
				return t;
			}
		});

		Bootstrap bootstrap = new Bootstrap();
		bootstrap.group(group).channel(NioSocketChannel.class);
		bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
		bootstrap.handler(new ChannelInitializer<Channel>() {
			@Override
			protected void initChannel(Channel ch) throws Exception {
			}
		});
		m_bootstrap = bootstrap;

		String routerConfig = m_configManager.getRouters();

		if (StringUtils.isNotEmpty(routerConfig)) {
			List<InetSocketAddress> configedAddresses = parseSocketAddress(routerConfig);
			ChannelHolder holder = initChannel(configedAddresses, routerConfig);

			if (holder != null) {
				m_activeChannelHolder = holder;
			} else {
				m_activeChannelHolder = new ChannelHolder();
				m_activeChannelHolder.setServerAddresses(configedAddresses);
			}
		} else {
			ChannelHolder holder = initChannel(serverAddresses, null);

			if (holder != null) {
				m_activeChannelHolder = holder;
			} else {
				m_activeChannelHolder = new ChannelHolder();
				m_activeChannelHolder.setServerAddresses(serverAddresses);
				log.error("error when init cat module due to error config xml in client.xml");
			}
		}
	}

	public ChannelFuture channel() {
		if (m_activeChannelHolder != null) {
			ChannelFuture future = m_activeChannelHolder.getActiveFuture();

			if (checkWritable(future)) {
				return future;
			}
		}
		return null;
	}

	private boolean checkActive(ChannelFuture future) {
		boolean isActive = false;

		if (future != null) {
			Channel channel = future.channel();

			if (channel.isActive() && channel.isOpen()) {
				isActive = true;
			} else {
				log.warn("channel buf is not active ,current channel " + future.channel().remoteAddress());
			}
		}

		return isActive;
	}

	private void checkServerChanged() {
		MutablePair<Boolean, String> pair = routerConfigChanged();

		if (pair.getKey()) {
			log.info("router config changed :" + pair.getValue());
			String servers = pair.getValue();
			List<InetSocketAddress> serverAddresses = parseSocketAddress(servers);
			ChannelHolder newHolder = initChannel(serverAddresses, servers);

			if (newHolder != null) {
				if (newHolder.isConnectChanged()) {
					ChannelHolder last = m_activeChannelHolder;

					m_activeChannelHolder = newHolder;
					closeChannelHolder(last);
					log.info("switch active channel to " + m_activeChannelHolder);
				} else {
					m_activeChannelHolder = newHolder;
				}
			}
		}
	}

	private boolean checkWritable(ChannelFuture future) {
		boolean isWriteable = false;

		if (future != null) {
			Channel channel = future.channel();

			if (channel.isActive() && channel.isOpen()) {
				if (channel.isWritable()) {
					isWriteable = true;
				} else {
					channel.flush();
				}
			} else {
				int count = m_attempts.incrementAndGet();

				if (count % 1000 == 0 || count == 1) {
					log.warn("channel buf is is close when send msg! Attempts: " + count);
				}
			}
		}

		return isWriteable;
	}

	private void closeChannel(ChannelFuture channel) {
		try {
			if (channel != null) {
				SocketAddress address = channel.channel().remoteAddress();

				if (address != null) {
					log.info("close channel " + address);
				}
				channel.channel().close();
			}
		} catch (Exception e) {
			// ignore
		}
	}

	private void closeChannelHolder(ChannelHolder channelHolder) {
		try {
			ChannelFuture channel = channelHolder.getActiveFuture();

			closeChannel(channel);
		} catch (Exception e) {
			// ignore
		}
	}

	private ChannelFuture createChannel(InetSocketAddress address) {
		log.info("start connect server" + address.toString());
		ChannelFuture future = null;

		try {
			future = m_bootstrap.connect(address);
			future.awaitUninterruptibly(100, TimeUnit.MILLISECONDS); // 100 ms

			if (!future.isSuccess()) {
				log.error("Error when try connecting to " + address);
				closeChannel(future);
			} else {
				log.info("Connected to CAT server at " + address);
				return future;
			}
		} catch (Throwable e) {
			log.error("Error when connect server " + address.getAddress(), e);

			if (future != null) {
				closeChannel(future);
			}
		}
		return null;
	}

	private void doubleCheckActiveServer(ChannelHolder channelHolder) {
		try {
			if (isChannelStalled(channelHolder)) {
				closeChannelHolder(m_activeChannelHolder);
				channelHolder.setActiveIndex(-1);
			}
		} catch (Throwable e) {
			log.error(e.getMessage(), e);
		}
	}

	@Override
	public String getName() {
		return "TcpSocketSender-ChannelManager";
	}

	private ChannelHolder initChannel(List<InetSocketAddress> addresses, String serverConfig) {
		try {
			int len = addresses.size();

			for (int i = 0; i < len; i++) {
				InetSocketAddress address = addresses.get(i);
				String hostAddress = address.getAddress().getHostAddress();
				ChannelHolder holder = null;

				if (m_activeChannelHolder != null && hostAddress.equals(m_activeChannelHolder.getIp())) {
					holder = new ChannelHolder();
					holder.setActiveFuture(m_activeChannelHolder.getActiveFuture()).setConnectChanged(false);
				} else {
					ChannelFuture future = createChannel(address);

					if (future != null) {
						holder = new ChannelHolder();
						holder.setActiveFuture(future).setConnectChanged(true);
					}
				}
				if (holder != null) {
					holder.setActiveIndex(i).setIp(hostAddress);
					holder.setActiveServerConfig(serverConfig).setServerAddresses(addresses);

					log.info("success when init CAT server, new active holder" + holder.toString());
					return holder;
				}
			}
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}

		try {
			StringBuilder sb = new StringBuilder();

			for (InetSocketAddress address : addresses) {
				sb.append(address.toString()).append(";");
			}
			log.info("Error when init CAT server " + sb.toString());
		} catch (Exception e) {
			// ignore
		}
		return null;
	}

	private boolean isChannelStalled(ChannelHolder holder) {
		ChannelFuture future = holder.getActiveFuture();
		boolean active = checkActive(future);

		if (!active) {
			if ((++m_channelStalledTimes) % 3 == 0) {
				return true;
			} else {
				return false;
			}
		} else {
			if (m_channelStalledTimes > 0) {
				m_channelStalledTimes--;
			}
			return false;
		}
	}

	private List<InetSocketAddress> parseSocketAddress(String content) {
		try {
			List<String> strs = Splitters.by(";").noEmptyItem().split(content);
			List<InetSocketAddress> address = new ArrayList<InetSocketAddress>();

			for (String str : strs) {
				List<String> items = Splitters.by(":").noEmptyItem().split(str);

				address.add(new InetSocketAddress(items.get(0), Integer.parseInt(items.get(1))));
			}
			return address;
		} catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		return new ArrayList<InetSocketAddress>();
	}

	private void reconnectDefaultServer(ChannelFuture activeFuture, List<InetSocketAddress> serverAddresses) {
		try {
			int reconnectServers = m_activeChannelHolder.getActiveIndex();

			if (reconnectServers == -1) {
				reconnectServers = serverAddresses.size();
			}
			for (int i = 0; i < reconnectServers; i++) {
				ChannelFuture future = createChannel(serverAddresses.get(i));

				if (future != null) {
					ChannelFuture lastFuture = activeFuture;

					m_activeChannelHolder.setActiveFuture(future);
					m_activeChannelHolder.setActiveIndex(i);
					closeChannel(lastFuture);
					break;
				}
			}
		} catch (Throwable e) {
			log.error(e.getMessage(), e);
		}
	}

	private MutablePair<Boolean, String> routerConfigChanged() {
		String routerConfig = m_configManager.getRouters();

		if (!StringUtils.isEmpty(routerConfig) && !routerConfig.equals(m_activeChannelHolder.getActiveServerConfig())) {
			return new MutablePair<Boolean, String>(true, routerConfig);
		} else {
			return new MutablePair<Boolean, String>(false, routerConfig);
		}
	}

	@Override
	public void run() {
		while (m_active) {
			// make save message id index asyc
			m_idFactory.saveMark();
			checkServerChanged();

			ChannelFuture activeFuture = m_activeChannelHolder.getActiveFuture();
			List<InetSocketAddress> serverAddresses = m_activeChannelHolder.getServerAddresses();

			doubleCheckActiveServer(m_activeChannelHolder);
			reconnectDefaultServer(activeFuture, serverAddresses);

			try {
				Thread.sleep(10 * 1000L); // check every 10 seconds
			} catch (InterruptedException e) {
				// ignore
			}
		}
	}

	@Override
	public void shutdown() {
		m_active = false;
	}

	public static class ChannelHolder {
		private ChannelFuture m_activeFuture;

		private int m_activeIndex = -1;

		private String m_activeServerConfig;

		private List<InetSocketAddress> m_serverAddresses;

		private String m_ip;

		private boolean m_connectChanged;

		public ChannelFuture getActiveFuture() {
			return m_activeFuture;
		}

		public ChannelHolder setActiveFuture(ChannelFuture activeFuture) {
			m_activeFuture = activeFuture;
			return this;
		}

		public int getActiveIndex() {
			return m_activeIndex;
		}

		public ChannelHolder setActiveIndex(int activeIndex) {
			m_activeIndex = activeIndex;
			return this;
		}

		public String getActiveServerConfig() {
			return m_activeServerConfig;
		}

		public ChannelHolder setActiveServerConfig(String activeServerConfig) {
			m_activeServerConfig = activeServerConfig;
			return this;
		}

		public String getIp() {
			return m_ip;
		}

		public ChannelHolder setIp(String ip) {
			m_ip = ip;
			return this;
		}

		public List<InetSocketAddress> getServerAddresses() {
			return m_serverAddresses;
		}

		public ChannelHolder setServerAddresses(List<InetSocketAddress> serverAddresses) {
			m_serverAddresses = serverAddresses;
			return this;
		}

		public boolean isConnectChanged() {
			return m_connectChanged;
		}

		public ChannelHolder setConnectChanged(boolean connectChanged) {
			m_connectChanged = connectChanged;
			return this;
		}

		public String toString() {
			StringBuilder sb = new StringBuilder();

			sb.append("active future :").append(m_activeFuture.channel().remoteAddress());
			sb.append(" index:").append(m_activeIndex);
			sb.append(" ip:").append(m_ip);
			sb.append(" server config:").append(m_activeServerConfig);
			return sb.toString();
		}
	}

	public class ClientMessageHandler extends SimpleChannelInboundHandler<Object> {

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
			log.info("receiver msg from server:" + msg);
		}
	}

}