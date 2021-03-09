package com.pk;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class CountDownLatchExample {

	private abstract class BaseHealthChecker implements Runnable {

		public BaseHealthChecker(String serviceName, CountDownLatch latch) {
			this.serviceName = serviceName;
			this.latch = latch;
		}

		private CountDownLatch latch;

		private String serviceName;

		private boolean serviceUp;

		@Override
		public void run() {
			try {
				verifyService();
				serviceUp = true;
			} catch (Throwable t) {
				t.printStackTrace();
				serviceUp = false;
			} finally {
				if (latch != null) {
					latch.countDown();
				}
			}
		}

		public String getServiceName() {
			return serviceName;
		}

		public boolean isServiceUp() {
			return serviceUp;
		}

		public abstract void verifyService();

	}

	private class NetworkHealthChecker extends BaseHealthChecker {

		public NetworkHealthChecker(String serviceName,CountDownLatch latch) {
			super(serviceName, latch);
			// TODO Auto-generated constructor stub
		}

		@Override
		public void verifyService() {
			System.out.println("Checking " + this.getServiceName());
			try {
				Thread.sleep(7000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println(this.getServiceName() + " is UP");
		}

	}

	public static void main(String[] args) {
		CountDownLatch countDownLatch = new CountDownLatch(10);
		List<NetworkHealthChecker> list = new ArrayList(10);
		for(int i=1;i<=10;i++) {
			list.add(new CountDownLatchExample().new NetworkHealthChecker("service "+i, countDownLatch));
		}
		Executor executor = Executors.newFixedThreadPool(10);
		for (Iterator iterator = list.iterator(); iterator.hasNext();) {
			NetworkHealthChecker networkHealthChecker = (NetworkHealthChecker) iterator.next();
			executor.execute(networkHealthChecker);
		}
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("Waiting to other threads complete");
	}
}
