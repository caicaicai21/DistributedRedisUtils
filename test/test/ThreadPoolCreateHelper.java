package test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolCreateHelper {
	/**
	 * corePoolSize 线程池中的核心线程数量；
	 * 
	 * maximumPoolSize 当前线程池正在运行的最大线程数量；
	 * 
	 * keepAliveTime 超过 corePoolSize 线程数量的线程最大空闲时间；
	 * 
	 * unit 以秒为时间单位；
	 * 
	 * workQueue 创建工作队列，用于存放提交的等待执行任务；
	 * -----------------------------------------------------
	 * 
	 * ArrayBlockingQueue 底层是数组，有界队列，如果我们要使用生产者-消费者模式，这是非常好的选择。
	 * 
	 * LinkedBlockingQueue 底层是链表，可以当做无界和有界队列来使用，所以大家不要以为它就是无界队列。
	 * 
	 * SynchronousQueue 本身不带有空间来存储任何元素，使用上可以选择公平模式和非公平模式。
	 * 
	 * PriorityBlockingQueue 是无界队列，基于数组，数据结构为二叉堆，数组第一个也是树的根节点总是最小值。
	 * 
	 * -----------------------------------------------------
	 * https://www.jianshu.com/p/6f82b738ac58
	 */

	private static ExecutorService threadPool = null;

	public static ExecutorService getThreadPool() {
		if (threadPool == null)
			threadPool = getNewThreadPool();
		return threadPool;
	}

	public static ExecutorService getNewThreadPool() {
		int corePoolSize = 5;
		int maximumPoolSize = 10;
		int maximumBlockingQueueSize = 10;
		long keepAilveTime = 60;
		return getNewThreadPool(corePoolSize, maximumPoolSize, keepAilveTime, maximumBlockingQueueSize);
	}

	public static ExecutorService getNewThreadPool(int corePoolSize, int maximumPoolSize, long keepAilveTime,
			int maximumBlockingQueueSize) {
		if (maximumBlockingQueueSize <= 0)
			maximumBlockingQueueSize = 1;
		BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(maximumBlockingQueueSize);
		return new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAilveTime, TimeUnit.SECONDS, queue);
	}
}
