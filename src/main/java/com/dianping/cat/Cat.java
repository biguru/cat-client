package com.dianping.cat;


import com.dianping.cat.analyzer.LocalAggregator;
import com.dianping.cat.configuration.DefaultClientConfigManager;
import com.dianping.cat.message.*;
import com.dianping.cat.message.internal.*;
import com.dianping.cat.message.io.DefaultTransportManager;
import com.dianping.cat.message.spi.MessageManager;
import com.dianping.cat.message.spi.MessageStatistics;
import com.dianping.cat.message.spi.MessageTree;
import com.dianping.cat.message.spi.internal.DefaultMessageStatistics;
import com.dianping.cat.status.StatusUpdateTask;
import com.dianping.cat.util.Threads;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.text.MessageFormat;
import java.util.Date;
import java.util.concurrent.locks.LockSupport;

/**
 * This is the main entry point to the system.
 */
@Component
@Slf4j
public class Cat {

	public static final String VERSION = "2.0.0";

	private static Cat s_instance = new Cat();

	private static volatile boolean s_init = false;
	private static volatile boolean s_multiInstances = false;

	@Autowired
	private DefaultMessageProducer defaultMessageProducer;

	@Autowired
	private DefaultMessageManager defaultMessageManager;

	@Autowired
	private  DefaultClientConfigManager defaultClientConfigManager;

	@Autowired
	private DefaultTransportManager defaultTransportManager;
	//private PlexusContainer m_container;
	@Autowired
	private DefaultMessageStatistics defaultMessageStatistics;
	@PostConstruct
	public void init() {
		MilliSecondTimer.initialize();
		Threads.addListener(new CatThreadListener());
		if (defaultClientConfigManager.isCatEnabled()) {
			// start status update task
			StatusUpdateTask statusUpdateTask = new StatusUpdateTask(defaultMessageStatistics, defaultClientConfigManager);
			Threads.forGroup("cat").start(statusUpdateTask);

			Threads.forGroup("cat").start(new LocalAggregator.DataUploader());

			LockSupport.parkNanos(10 * 1000 * 1000L); // wait 10 ms
			// MmapConsumerTask mmapReaderTask = ctx.lookup(MmapConsumerTask.class);
			// Threads.forGroup("cat").start(mmapReaderTask);
		}
		s_instance = this;
		s_init = true;
	}
	public static String createMessageId() {
		return Cat.getProducer().createMessageId();
	}

	public static void destroy() {

		s_instance = new Cat();
	}

	public static String getCatHome() {
		String catHome = CatPropertyProvider.INST.getProperty("CAT_HOME", CatConstants.CAT_HOME_DEFAULT_DIR);
		if (!catHome.endsWith("/")) {
			catHome = catHome + "/";
		}
		return catHome;
	}

	public static String getCurrentMessageId() {
		MessageTree tree = Cat.getManager().getThreadLocalMessageTree();

		if (tree != null) {
			String messageId = tree.getMessageId();

			if (messageId == null) {
				messageId = Cat.createMessageId();
				tree.setMessageId(messageId);
			}
			return messageId;
		} else {
			return null;
		}
	}

	public static Cat getInstance() {
		return s_instance;
	}

	public static MessageManager getManager() {
		try {

			MessageManager manager = s_instance.defaultMessageManager;

			if (manager != null) {
				return manager;
			} else {
				return NullMessageManager.NULL_MESSAGE_MANAGER;
			}
		} catch (Exception e) {
			return NullMessageManager.NULL_MESSAGE_MANAGER;
		}
	}

	public static MessageProducer getProducer() {
		try {

			MessageProducer producer = s_instance.defaultMessageProducer;

			if (producer != null) {
				return producer;
			} else {
				return NullMessageProducer.NULL_MESSAGE_PRODUCER;
			}
		} catch (Exception e) {
			return NullMessageProducer.NULL_MESSAGE_PRODUCER;
		}
	}

	public static boolean isInitialized() {
        // sync方式存在死锁风险，因此改用判断volatile变量
        //synchronized (s_instance) {
        //    return s_instance.m_container != null;
        //}
        return s_init;
	}

	static void log(String severity, String message) {
		MessageFormat format = new MessageFormat("[{0,date,MM-dd HH:mm:ss.sss}] [{1}] [{2}] {3}");

		System.out.println(format.format(new Object[] { new Date(), severity, "cat", message }));
	}

	public static void logError(String message, Throwable cause) {
		Cat.getProducer().logError(message, cause);
	}

	public static void logError(Throwable cause) {
		Cat.getProducer().logError(cause);
	}

	public static void logEvent(String type, String name) {
		Cat.getProducer().logEvent(type, name);
	}

	public static void logEvent(String type, String name, String status, String nameValuePairs) {
		Cat.getProducer().logEvent(type, name, status, nameValuePairs);
	}

	public static void logHeartbeat(String type, String name, String status, String nameValuePairs) {
		Cat.getProducer().logHeartbeat(type, name, status, nameValuePairs);
	}

	/**
	 * Increase the counter specified by <code>name</code> by one.
	 * 
	 * @param name
	 *           the name of the metric default count value is 1
	 */
	public static void logMetricForCount(String name) {
		logMetricInternal(name, "C", "1");
	}

	/**
	 * Increase the counter specified by <code>name</code> by one.
	 *
	 * <pre>
	 *     Cat.logMetricForCount("Order.Count", tagKey1, tagValue1, tagKey2, tagValue2)
	 * </pre>
	 * @param name the name of the metric default count value is 1
	 * @param tagKeyValuePairs the tag key and value pairs
	 */
	public static void logMetricForCount(String name, String... tagKeyValuePairs) {
		logMetricInternal(name, "C", "1", tagKeyValuePairs);
	}

	/**
	 * Increase the counter specified by <code>name</code> by one.
	 * 
	 * @param name
	 *           the name of the metric
	 */
	public static void logMetricForCount(String name, int quantity) {
		logMetricInternal(name, "C", String.valueOf(quantity));
	}

	/**
	 * Increase the counter specified by <code>name</code> by one.
	 * <pre>
	 *     Cat.logMetricForCount("Order.Count", 1, tagKey1, tagValue1, tagKey2, tagValue2)
	 * </pre>
	 *
	 * @param name the name of the metric
	 * @param quantity quantity
	 * @param tagKeyValuePairs the tag key and value pairs
	 */
	public static void logMetricForCount(String name, int quantity, String... tagKeyValuePairs) {
		logMetricInternal(name, "C", String.valueOf(quantity), tagKeyValuePairs);
	}

	/**
	 * Increase the metric specified by <code>name</code> by <code>durationInMillis</code>.
	 * 
	 * @param name
	 *           the name of the metric
	 * @param durationInMillis
	 *           duration in milli-second added to the metric
	 */
	public static void logMetricForDuration(String name, long durationInMillis) {
		logMetricInternal(name, "T", String.valueOf(durationInMillis));
	}
	/**
	 * Increase the metric specified by <code>name</code> by <code>durationInMillis</code>.
	 *	<pre>
	 *     Map<String, String> tags = new HashMap<String, String>();
	 *     tags.put("cityId", "110100");
	 *     tags.put("orgCode", "Order");
	 *     Cat.logMetricForDuration("Order.Count", 1, map)
	 * </pre>
	 *
	 * @param name
	 *           the name of the metric
	 * @param durationInMillis
	 *           duration in milli-second added to the metric
	 *
	 * @param tagKeyValuePairs the tag key and value pairs
	 */
	public static void logMetricForDuration(String name, long durationInMillis, String... tagKeyValuePairs) {
		logMetricInternal(name, "T", String.valueOf(durationInMillis), tagKeyValuePairs);
	}

	/**
	 * Increase the sum specified by <code>name</code> by <code>value</code> only for one item.
	 * 
	 * @param name
	 *           the name of the metric
	 * @param value
	 *           the value added to the metric
	 */

	@Deprecated
	public static void logMetricForSum(String name, double value) {
		logMetricInternal(name, "S", String.format("%.2f", value));
	}

	@Deprecated
	public static void logMetricForSum(String name, double value, String... tagKeyValuePairs) {
		logMetricInternal(name, "S", String.format("%.2f", value), tagKeyValuePairs);
	}

	/**
	 * Increase the metric specified by <code>name</code> by <code>sum</code> for multiple items.
	 * 
	 * @param name
	 *           the name of the metric
	 * @param sum
	 *           the sum value added to the metric
	 * @param quantity
	 *           the quantity to be accumulated
	 */

	@Deprecated
	public static void logMetricForSum(String name, double sum, int quantity) {
		logMetricInternal(name, "S,C", String.format("%s,%.2f", quantity, sum));
	}

	/**
	 * Increase the metric specified by <code>name</code> by <code>sum</code> for multiple items.
	 *	<pre>
	 *     Map<String, String> tags = new HashMap<String, String>();
	 *     tags.put("cityId", "110100");
	 *     tags.put("orgCode", "Order");
	 *     Cat.logMetricForSum("Order.Count", 1, 1, map)
	 * </pre>
	 * @param name
	 *           the name of the metric
	 * @param sum
	 *           the sum value added to the metric
	 * @param quantity
	 *           the quantity to be accumulated
	 *
	 * @param tagKeyValuePairs tags
	 */

	@Deprecated
	public static void logMetricForSum(String name, double sum, int quantity, String... tagKeyValuePairs) {
		logMetricInternal(name, "S,C", String.format("%s,%.2f", quantity, sum), tagKeyValuePairs);
	}

	private static void logMetricInternal(String name, String status, String keyValuePairs) {
		Cat.getProducer().logMetric(name, status, keyValuePairs);
	}

	private static void logMetricInternal(String name, String status, String keyValuePairs, String... tagKeyValuePairs) {
		Cat.getProducer().logMetric(name, status, keyValuePairs, tagKeyValuePairs);
	}

	public static void logRemoteCallClient(Context ctx) {
		MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
		String messageId = tree.getMessageId();

		if (messageId == null) {
			messageId = Cat.createMessageId();
			tree.setMessageId(messageId);
		}

		String childId = Cat.createMessageId();
		Cat.logEvent(CatConstants.TYPE_REMOTE_CALL, "", Event.SUCCESS, childId);

		String root = tree.getRootMessageId();

		if (root == null) {
			root = messageId;
		}

		ctx.addProperty(Context.ROOT, root);
		ctx.addProperty(Context.PARENT, messageId);
		ctx.addProperty(Context.CHILD, childId);
	}

	public static void logRemoteCallServer(Context ctx) {
		MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
		String messageId = ctx.getProperty(Context.CHILD);
		String rootId = ctx.getProperty(Context.ROOT);
		String parentId = ctx.getProperty(Context.PARENT);

		if (messageId != null) {
			tree.setMessageId(messageId);
		}
		if (parentId != null) {
			tree.setParentMessageId(parentId);
		}
		if (rootId != null) {
			tree.setRootMessageId(rootId);
		}
	}

	public static void logTrace(String type, String name) {
		Cat.getProducer().logTrace(type, name);
	}

	public static void logTrace(String type, String name, String status, String nameValuePairs) {
		Cat.getProducer().logTrace(type, name, status, nameValuePairs);
	}

	public static Event newEvent(String type, String name) {
		return Cat.getProducer().newEvent(type, name);
	}

	public static ForkedTransaction newForkedTransaction(String type, String name) {
		return Cat.getProducer().newForkedTransaction(type, name);
	}

	public static Heartbeat newHeartbeat(String type, String name) {
		return Cat.getProducer().newHeartbeat(type, name);
	}

	public static TaggedTransaction newTaggedTransaction(String type, String name, String tag) {
		return Cat.getProducer().newTaggedTransaction(type, name, tag);
	}

	public static Trace newTrace(String type, String name) {
		return Cat.getProducer().newTrace(type, name);
	}

	public static Transaction newTransaction(String type, String name) {
		return Cat.getProducer().newTransaction(type, name);
	}
	
	// this should be called when a thread ends to clean some thread local data
	public static void reset() {
		// remove me
	}

	// this should be called when a thread starts to create some thread local data
	public static void setup(String sessionToken) {
		Cat.getManager().setup();
	}

	public static boolean isMultiInstanceEnable() {
		return s_multiInstances;
	}

	public static void enableMultiInstances() {
		s_multiInstances = true;
	}

	public static interface Context {

		public final String ROOT = "_catRootMessageId";

		public final String PARENT = "_catParentMessageId";

		public final String CHILD = "_catChildMessageId";

		public void addProperty(String key, String value);

		public String getProperty(String key);
	}

}
