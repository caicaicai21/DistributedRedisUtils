package com.caicaicai21.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.caicaicai21.redis.SynchronizedExecutor.IOperate;
import com.caicaicai21.redis.serializer.FstSerializer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.util.SafeEncoder;

public class DistributedRedisUtils {
	private DistributedRedisUtils() {
	}

	public interface IJedisPoolCreateHelper {
		public JedisPool createJedisPool();
	}

	public interface IRedisProcess {
		public Object execute(Jedis jedis, ISerializer serializer);
	}

	public interface ISerializer {
		public byte[] serialize(Object object);

		public Object deserialize(byte[] data);
	}

	public interface ILogger {
		public void log(Throwable throwable);
	}

	private static boolean isInit = false;

	private static JedisPool jedisPool = null;
	private static ISerializer serializer = new FstSerializer();
	private static ILogger logger = new ILogger() {
		@Override
		public void log(Throwable throwable) {
			throwable.printStackTrace();
		}
	};

	public static final String KEY_NULL = "#null#";
	public static final String VALUE_NULL_OBJECT = "#I am null object#";

	public static void init(String host, int port, String password) {
		init(new JedisPoolConfig(), host, port, password);
	}

	public synchronized static void init(GenericObjectPoolConfig<?> config, String host, int port, String password) {
		jedisPool = new JedisPool(config, host, port, Protocol.DEFAULT_TIMEOUT, password);
		isInit = true;
	}

	public synchronized static void init(IJedisPoolCreateHelper helper) {
		if (helper != null) {
			jedisPool = helper.createJedisPool();
			isInit = true;
		}
	}

	private static void checkInit() {
		if (!isInit)
			throw new RuntimeException("Please init jedisPool first!");
	}

	public static void setSerializer(ISerializer serializer) {
		DistributedRedisUtils.serializer = serializer;
	}

	public static void setLogger(ILogger logger) {
		DistributedRedisUtils.logger = logger;
	}

	private static void log(Throwable throwable) {
		if (logger != null)
			logger.log(throwable);
		else
			throwable.printStackTrace();
	}

	public static Jedis getJedis() {
		if (jedisPool == null)
			return null;
		return jedisPool.getResource();
	}

	public static void closeJedis(Jedis jedis) {
		if (jedis != null)
			jedis.close();
	}

	private static String checkNullKey(String key) {
		if (key == null)
			return KEY_NULL;
		return key;
	}

	private static Object checkNullObject(Object object) {
		if (object == null)
			return VALUE_NULL_OBJECT;
		if (object instanceof String && VALUE_NULL_OBJECT.equals(object))
			return null;
		return object;
	}

	public static byte[] keyToByte(String key) {
		return SafeEncoder.encode(key);
	}

	@SuppressWarnings("unchecked")
	public static <T> T get(String key) {
		checkInit();
		key = checkNullKey(key);
		Jedis jedis = getJedis();
		Object object = null;
		try {
			object = serializer.deserialize(jedis.get(keyToByte(key)));
		} catch (Exception e) {
			log(e);
		}
		closeJedis(jedis);
		return (T) (object == null ? null : checkNullObject(object));
	}

	@SuppressWarnings("unchecked")
	public static <T> T getSync(String key) {
		checkInit();
		final String finalKey = checkNullKey(key);
		Jedis jedis = getJedis();
		SynchronizedExecutor executor = new SynchronizedExecutor(jedis, finalKey, new IOperate() {
			@Override
			public Object execute(Jedis jedis) {
				return jedis.get(keyToByte(finalKey));
			}
		}).setAutoClose();
		Object object = null;
		try {
			object = serializer.deserialize((byte[]) executor.start());
		} catch (Exception e) {
			log(e);
		}
		return (T) (object == null ? null : checkNullObject(object));
	}

	public static void set(String key, Object object) {
		checkInit();
		key = checkNullKey(key);
		Jedis jedis = getJedis();
		try {
			jedis.set(keyToByte(key), serializer.serialize(checkNullObject(object)));
		} catch (Exception e) {
			log(e);
		} finally {
			closeJedis(jedis);
		}
	}

	public static void setSync(String key, Object object) {
		checkInit();
		final Object finalObject = checkNullObject(object);
		final String finalKey = checkNullKey(key);
		Jedis jedis = getJedis();
		try {
			new SynchronizedExecutor(jedis, finalKey, new IOperate() {
				@Override
				public Object execute(Jedis jedis) {
					jedis.set(keyToByte(finalKey), serializer.serialize(finalObject));
					return null;
				}
			}).setAutoClose().start();
		} catch (Exception e) {
			log(e);
		}
	}

	public static boolean del(String key) {
		checkInit();
		Jedis jedis = getJedis();
		boolean success = jedis.del(checkNullKey(key)) > 0;
		closeJedis(jedis);
		return success;
	}

	public static boolean delSync(String key) {
		checkInit();
		final String delKey = checkNullKey(key);
		Jedis jedis = getJedis();
		SynchronizedExecutor executor = new SynchronizedExecutor(jedis, delKey, new IOperate() {
			@Override
			public Object execute(Jedis jedis) {
				return jedis.del(delKey) > 0;
			}
		}).setAutoClose();
		return (boolean) executor.start();
	}

	public static boolean isKeyExist(String key) {
		checkInit();
		key = checkNullKey(key);
		Jedis jedis = getJedis();
		boolean exist = jedis.exists(key);
		closeJedis(jedis);
		return exist;
	}

	public static Object doRedisProcess(IRedisProcess redisProcess) {
		checkInit();
		if (redisProcess == null)
			return null;
		Jedis jedis = getJedis();
		try {
			return redisProcess.execute(jedis, serializer);
		} catch (Exception e) {
			log(e);
		} finally {
			closeJedis(jedis);
		}
		return null;
	}

	public static Object doRedisProcessSync(String uniqueFlagKey, IRedisProcess redisProcess) {
		checkInit();
		if (redisProcess == null)
			return null;
		Jedis jedis = getJedis();
		uniqueFlagKey = checkNullKey(uniqueFlagKey);
		SynchronizedExecutor executor = new SynchronizedExecutor(jedis, uniqueFlagKey, new IOperate() {
			@Override
			public Object execute(Jedis jedis) {
				return redisProcess.execute(jedis, serializer);
			}
		}).setAutoClose();

		try {
			return executor.start();
		} catch (Exception e) {
			log(e);
		}
		return null;
	}
}

class SynchronizedExecutor {
	public interface IOperate {
		public Object execute(Jedis jedis);
	}

	private static final String KEY_SYNC = "#RedisSync#_";

	private Jedis jedis;
	private String key;
	private IOperate operate;
	private boolean autoClose = false;
	private long maxWaitMillis = 200;

	public SynchronizedExecutor(Jedis jedis, String key, IOperate operate) {
		this.jedis = jedis;
		this.key = key;
		this.operate = operate;
	}

	public SynchronizedExecutor setAutoClose() {
		this.autoClose = true;
		return this;
	}

	public SynchronizedExecutor setMaxWaitMillis(long maxWaitMillis) {
		this.maxWaitMillis = maxWaitMillis;
		return this;
	}

	private String createSyncKey(String key) {
		if (key.startsWith(KEY_SYNC))
			return key;
		return KEY_SYNC + key;
	}

	private void tryGetSyncLock(Jedis jedis, String key) {
		key = createSyncKey(key);
		boolean success = false;
		do {
			success = jedis.setnx(key, Long.valueOf(System.currentTimeMillis()).toString()) == 1;
			if (!success) {
				String data = jedis.get(key);
				if (data != null && System.currentTimeMillis() - Long.valueOf(data) > maxWaitMillis)
					syncUnlock(jedis, key);
			}
		} while (!success);
	}

	private void syncUnlock(Jedis jedis, String key) {
		key = createSyncKey(key);
		jedis.del(key);
	}

	public Object start() {
		if (jedis == null || operate == null)
			return null;
		try {
			tryGetSyncLock(jedis, key);
			return operate.execute(jedis);
		} finally {
			syncUnlock(jedis, key);
			if (autoClose)
				DistributedRedisUtils.closeJedis(jedis);
		}
	}
}
