//package com.ignite.streamer;
//
//import com.google.common.util.concurrent.MoreExecutors;
//import com.google.common.util.concurrent.ThreadFactoryBuilder;
//
//import java.util.concurrent.*;
//
//public class TimeLimiter {
//
//	private static ThreadPoolExecutor		timeLimiterThreadPool		= new ThreadPoolExecutor(300, 500, 60L,
//			TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(500),
//			new ThreadFactoryBuilder().setNameFormat("time-limiter-thread-%d").build());
//
//	private static ThreadPoolExecutor		activeTimeLimiterThreadPool	= new ThreadPoolExecutor(50, 300, 60L,
//			TimeUnit.SECONDS, new LinkedBlockingQueue<>(500),
//			new ThreadFactoryBuilder().setNameFormat("active-time-limiter-thread-%d").build());
//
//	private static ExecutorService timeLimiterExecutor			= MoreExecutors
//			.getExitingExecutorService(timeLimiterThreadPool);
//
//	private static ExecutorService			activeTimeLimiterExecutor	= MoreExecutors
//			.getExitingExecutorService(activeTimeLimiterThreadPool);
//
//	private static ScheduledExecutorService monitor						= Executors.newSingleThreadScheduledExecutor(
//			new ThreadFactoryBuilder().setNameFormat("time-limiter-monitor-thread").build());
//	static {
//		monitor.scheduleAtFixedRate(() -> {
//			System.out.println("core=" + timeLimiterThreadPool.getCorePoolSize() + ",max="
//					+ timeLimiterThreadPool.getMaximumPoolSize() + ",active=" + timeLimiterThreadPool.getActiveCount()
//					+ ",largest=" + timeLimiterThreadPool.getLargestPoolSize() + ",completed="
//					+ timeLimiterThreadPool.getCompletedTaskCount() + ",task=" + timeLimiterThreadPool.getTaskCount()
//					+ ",pending=" + timeLimiterThreadPool.getQueue().size());
//			System.out.println("a_core=" + activeTimeLimiterThreadPool.getCorePoolSize() + ",a_max="
//					+ activeTimeLimiterThreadPool.getMaximumPoolSize() + ",a_active="
//					+ activeTimeLimiterThreadPool.getActiveCount() + ",a_largest="
//					+ activeTimeLimiterThreadPool.getLargestPoolSize() + ",a_completed="
//					+ activeTimeLimiterThreadPool.getCompletedTaskCount() + ",a_task="
//					+ activeTimeLimiterThreadPool.getTaskCount() + ",a_pending="
//					+ activeTimeLimiterThreadPool.getQueue().size());
//		}, 60, 60, TimeUnit.SECONDS);
//	}
//
//	public static <V> V withTimeoutOrNull(Callable<V> callable, int timeout, TimeUnit timeUnit, String tag) {
//        long begin = System.currentTimeMillis();
//		try {
//
//            if (timeLimiterThreadPool.getActiveCount() >= 500) {
//				System.out.println("core=" + timeLimiterThreadPool.getCorePoolSize() + ",max="
//                        + timeLimiterThreadPool.getMaximumPoolSize() + ",active="
//                        + timeLimiterThreadPool.getActiveCount() + ",largest="
//                        + timeLimiterThreadPool.getLargestPoolSize() + ",completed="
//                        + timeLimiterThreadPool.getCompletedTaskCount() + ",task="
//                        + timeLimiterThreadPool.getTaskCount() + ",pending=" + timeLimiterThreadPool.getQueue().size());
//                return null;
//            }
//
//            Future<V> future = timeLimiterExecutor.submit(callable);
//            V v = future.get(timeout, timeUnit);
//            long endTime = System.currentTimeMillis();
//            if (endTime - begin > 500) {
//                System.out.println("------------------query timeout:" + tag + "------------------");
//            }
//            return v;
//		} catch (Exception e) {
//            long endTime = System.currentTimeMillis();
//            if (endTime - begin > 500) {
//                System.out.println("------------------query timeout:" + tag + "------------------");
//            }
//            e.printStackTrace();
//			return null;
//		}
//	}
//
//	/**
//	 * 为配网单独分配线程池
//	 *
//	 * @param callable
//	 * @param timeout
//	 * @param timeUnit
//	 * @param <V>
//	 * @return
//	 */
//	public static <V> V activeWithTimeoutOrNull(Callable<V> callable, int timeout, TimeUnit timeUnit) {
//		try {
//            if (activeTimeLimiterThreadPool.getActiveCount() >= 300) {
//				System.out.println("a_core=" + activeTimeLimiterThreadPool.getCorePoolSize() + ",a_max="
//                        + activeTimeLimiterThreadPool.getMaximumPoolSize() + ",a_active="
//                        + activeTimeLimiterThreadPool.getActiveCount() + ",a_largest="
//                        + activeTimeLimiterThreadPool.getLargestPoolSize() + ",a_completed="
//                        + activeTimeLimiterThreadPool.getCompletedTaskCount() + ",a_task="
//                        + activeTimeLimiterThreadPool.getTaskCount() + ",a_pending="
//                        + activeTimeLimiterThreadPool.getQueue().size());
//                return null;
//            }
//			Future<V> future = activeTimeLimiterExecutor.submit(callable);
//			return future.get(timeout, timeUnit);
//		} catch (Exception e) {
//			System.out.println("active timeout occur" + e);
//			return null;
//		}
//	}
//
//	public static void withoutReturn(Runnable runnable, String tag) {
//		try {
//
//			if (timeLimiterThreadPool.getActiveCount() >= 500) {
//				System.out.println("core=" + timeLimiterThreadPool.getCorePoolSize() + ",max="
//						+ timeLimiterThreadPool.getMaximumPoolSize() + ",active="
//						+ timeLimiterThreadPool.getActiveCount() + ",largest="
//						+ timeLimiterThreadPool.getLargestPoolSize() + ",completed="
//						+ timeLimiterThreadPool.getCompletedTaskCount() + ",task="
//						+ timeLimiterThreadPool.getTaskCount() + ",pending=" + timeLimiterThreadPool.getQueue().size());
//			}
//			timeLimiterExecutor.submit(runnable);
//		} catch (Exception e) {
//			System.out.println("timeout occur, tag = " +tag);
//		}
//	}
//
//}