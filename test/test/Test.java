package test;

import java.util.concurrent.ExecutorService;

import com.caicaicai21.redis.DistributedRedisUtils;
import com.caicaicai21.redis.DistributedRedisUtils.IRedisProcess;
import com.caicaicai21.redis.DistributedRedisUtils.ISerializer;

import redis.clients.jedis.Jedis;

public class Test {

	public static void main(String[] args) throws Exception {
		ExecutorService service = ThreadPoolCreateHelper.getNewThreadPool();

		DistributedRedisUtils.init("127.0.0.1", 6379, null);

		for (int j = 0; j < 8; j++) {
			service.execute(new Runnable() {
				@Override
				public void run() {
					for (int i = 0; i < 100; i++) {
						DistributedRedisUtils.doRedisProcessSync("my", new IRedisProcess() {
							public byte[] keyToByte(String key) {
								return DistributedRedisUtils.keyToByte(key);
							}

							@Override
							public Object execute(Jedis jedis, ISerializer serializer) {
								Integer now = (Integer) serializer.deserialize(jedis.get(keyToByte("sum")));
								if (now == null)
									now = 1;
								else
									now++;
								jedis.set(keyToByte("sum"), serializer.serialize(now));
								return null;
							}
						});
					}
					System.out.println(Thread.currentThread().getName() + " complete");
				}
			});
		}

		service.shutdown();
		while (!service.isTerminated()) {
			Thread.sleep(200);
		}

		Integer sum = DistributedRedisUtils.getSync("sum");
		System.out.println(DistributedRedisUtils.delSync("sum"));
		System.out.println(sum);
	}
}
