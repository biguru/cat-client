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
package com.dianping.cat.message.internal;

import com.dianping.cat.Cat;
import com.dianping.cat.message.*;
import com.dianping.cat.message.spi.MessageTree;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


import java.io.PrintWriter;
import java.io.StringWriter;

@Component
public class DefaultMessageProducer implements MessageProducer {
	@Autowired
	private DefaultMessageManager defaultMessageManager;

	@Autowired
	private MessageIdFactory messageIdFactory;

	@Override
	public String createRpcServerId(String domain) {
		return messageIdFactory.getNextId(domain);
	}

	@Override
	public String createMessageId() {
		return messageIdFactory.getNextId();
	}

	@Override
	public boolean isEnabled() {
		return defaultMessageManager.isMessageEnabled();
	}

	@Override
	public void logError(String message, Throwable cause) {
		if (Cat.getManager().isCatEnabled()) {
			if (defaultMessageManager.shouldLog(cause)) {
				defaultMessageManager.getThreadLocalMessageTree().setDiscard(false);

				StringWriter writer = new StringWriter(2048);

				if (message != null) {
					writer.write(message);
					writer.write(' ');
				}

				cause.printStackTrace(new PrintWriter(writer));

				String detailMessage = writer.toString();

				if (cause instanceof Error) {
					logEvent("Error", cause.getClass().getName(), "ERROR", detailMessage);
				} else if (cause instanceof RuntimeException) {
					logEvent("RuntimeException", cause.getClass().getName(), "ERROR", detailMessage);
				} else {
					logEvent("Exception", cause.getClass().getName(), "ERROR", detailMessage);
				}
			}
		} else {
			cause.printStackTrace();
		}
	}

	@Override
	public void logError(Throwable cause) {
		logError(null, cause);
	}

	@Override
	public void logEvent(String type, String name) {
		logEvent(type, name, Message.SUCCESS, null);
	}

	@Override
	public void logEvent(String type, String name, String status, String nameValuePairs) {
		Event event = newEvent(type, name);

		if (nameValuePairs != null && nameValuePairs.length() > 0) {
			event.addData(nameValuePairs);
		}

		event.setStatus(status);
		event.complete();
	}

	@Override
	public void logHeartbeat(String type, String name, String status, String nameValuePairs) {
		Heartbeat heartbeat = newHeartbeat(type, name);

		heartbeat.addData(nameValuePairs);
		heartbeat.setStatus(status);
		heartbeat.complete();
	}

	@Override
	public void logMetric(String name, String status, String nameValuePairs) {
		String type = "";
		Metric metric = newMetric(type, name);

		if (nameValuePairs != null && nameValuePairs.length() > 0) {
			metric.addData(nameValuePairs);
		}

		metric.setStatus(status);
		metric.complete();
	}

	@Override
	public void logMetric(String name, String status, String statusValuePairs, String... tagKeyValuePairs) {
		String type = "";
		Metric metric = newMetric(type, name);

		if (statusValuePairs != null && statusValuePairs.length() > 0) {
			metric.addData(statusValuePairs);
		}

		metric.setStatus(status);

		if (tagKeyValuePairs != null && tagKeyValuePairs.length > 0) {
			if (tagKeyValuePairs.length % 2 == 1) {
				throw new IllegalArgumentException("tagKeyValuePairs must be even number!");
			}
			StringBuilder tags = new StringBuilder("");
			for (int i = 0; i < tagKeyValuePairs.length; i+=2) {
				if (null == tagKeyValuePairs[i] || null == tagKeyValuePairs[i + 1]
						|| "".equals(tagKeyValuePairs[i].trim())
						|| "".equals(tagKeyValuePairs[i + 1].trim())) {
					continue;
				}
				tags.append(tagKeyValuePairs[i]).append("=").append(tagKeyValuePairs[i + 1]);
				tags.append("&");
			}
			if (tags.length() > 0) {
				tags.deleteCharAt(tags.length() - 1);
			}
			metric.setTags(tags.toString());
		}
		metric.complete();
	}

	@Override
	public void logTrace(String type, String name) {
		logTrace(type, name, Message.SUCCESS, null);
	}

	@Override
	public void logTrace(String type, String name, String status, String nameValuePairs) {
		if (defaultMessageManager.isTraceMode()) {
			Trace trace = newTrace(type, name);

			if (nameValuePairs != null && nameValuePairs.length() > 0) {
				trace.addData(nameValuePairs);
			}

			trace.setStatus(status);
			trace.complete();
		}
	}

	@Override
	public Event newEvent(String type, String name) {
		if (!defaultMessageManager.hasContext()) {
			defaultMessageManager.setup();
		}

		return new DefaultEvent(type, name, defaultMessageManager);
	}

	public Event newEvent(Transaction parent, String type, String name) {
		if (!defaultMessageManager.hasContext()) {
			defaultMessageManager.setup();
		}

		DefaultEvent event = new DefaultEvent(type, name);

		parent.addChild(event);
		return event;
	}

	@Override
	public ForkedTransaction newForkedTransaction(String type, String name) {
		// this enable CAT client logging cat message without explicit setup
		if (!defaultMessageManager.hasContext()) {
			defaultMessageManager.setup();
		}

		MessageTree tree = defaultMessageManager.getThreadLocalMessageTree();

		if (tree.getMessageId() == null) {
			tree.setMessageId(createMessageId());
		}

		DefaultForkedTransaction transaction = new DefaultForkedTransaction(type, name, defaultMessageManager);

		if (defaultMessageManager instanceof DefaultMessageManager) {
			((DefaultMessageManager) defaultMessageManager).linkAsRunAway(transaction);
		}
		defaultMessageManager.start(transaction, true);
		return transaction;
	}

	@Override
	public Heartbeat newHeartbeat(String type, String name) {
		if (!defaultMessageManager.hasContext()) {
			defaultMessageManager.setup();
		}

		DefaultHeartbeat heartbeat = new DefaultHeartbeat(type, name, defaultMessageManager);

		defaultMessageManager.getThreadLocalMessageTree().setDiscard(false);
		return heartbeat;
	}

	@Override
	public Metric newMetric(String type, String name) {
		if (!defaultMessageManager.hasContext()) {
			defaultMessageManager.setup();
		}

		DefaultMetric metric = new DefaultMetric(type == null ? "" : type, name, defaultMessageManager);

		defaultMessageManager.getThreadLocalMessageTree().setDiscard(false);
		return metric;
	}

	@Override
	public TaggedTransaction newTaggedTransaction(String type, String name, String tag) {
		// this enable CAT client logging cat message without explicit setup
		if (!defaultMessageManager.hasContext()) {
			defaultMessageManager.setup();
		}

		MessageTree tree = defaultMessageManager.getThreadLocalMessageTree();

		if (tree.getMessageId() == null) {
			tree.setMessageId(createMessageId());
		}
		DefaultTaggedTransaction transaction = new DefaultTaggedTransaction(type, name, tag, defaultMessageManager);

		defaultMessageManager.start(transaction, true);
		return transaction;
	}

	@Override
	public Trace newTrace(String type, String name) {
		if (!defaultMessageManager.hasContext()) {
			defaultMessageManager.setup();
		}

		return new DefaultTrace(type, name, defaultMessageManager);
	}

	@Override
	public Transaction newTransaction(String type, String name) {
		// this enable CAT client logging cat message without explicit setup
		if (!defaultMessageManager.hasContext()) {
			defaultMessageManager.setup();
		}

		DefaultTransaction transaction = new DefaultTransaction(type, name, defaultMessageManager);

		defaultMessageManager.start(transaction, false);
		return transaction;
	}

	public Transaction newTransaction(Transaction parent, String type, String name) {
		// this enable CAT client logging cat message without explicit setup
		if (!defaultMessageManager.hasContext()) {
			defaultMessageManager.setup();
		}

		DefaultTransaction transaction = new DefaultTransaction(type, name, defaultMessageManager);

		parent.addChild(transaction);
		transaction.setStandalone(false);
		return transaction;
	}

}
