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

import com.dianping.cat.message.Metric;
import com.dianping.cat.message.spi.MessageManager;

public class DefaultMetric extends AbstractMessage implements Metric {
	private MessageManager m_manager;
	private String tags;
	public DefaultMetric(String type, String name) {
		super(type, name);
	}

	public DefaultMetric(String type, String name, MessageManager manager) {
		super(type, name);

		m_manager = manager;
	}

	@Override
	public void complete() {
		setCompleted(true);

		if (m_manager != null) {
			m_manager.add(this);
		}
	}
	@Override
	public String getTags() {
		return tags;
	}

	@Override
	public void setTags(String tags) {
//        if (tags != null && !"".equals(tags.trim())) {
//            List<String> list = Splitter.on("&").omitEmptyStrings().trimResults().splitToList(tags);
//            for (String item : list) {
//                String[] strs = item.split("=");
//                if (strs.length != 2) {
//                    throw new IllegalArgumentException("tags must be KV pairs! for example: a=b&c=d.");
//                }
//            }
//        }
		this.tags = tags;
	}

}