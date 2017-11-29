package io.github.yangziwen.nettyhttpclient;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.yangziwen.nettyhttpclient.NettyPooledHttpClient.Response;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

public class Test {
	
	public static void main(String[] args) throws Exception {
		NettyPooledHttpClient client = new NettyPooledHttpClient(20, 5, 1, TimeUnit.SECONDS);
		URI uri = new URI("http://localhost:8045/job/codeline/metrics");
		AtomicInteger cnt = new AtomicInteger();
		long t = System.currentTimeMillis();
		int n = 500;
		CountDownLatch latch = new CountDownLatch(n);
		for (int i = 0; i < n; i++) {
			final int index = i;
			client.sendGet(uri).addListener(new FutureListener<Response>() {
				@Override
				public void operationComplete(Future<Response> future) throws Exception {
					if (future.isSuccess()) {
						System.out.println(index + ":" + future.get());
					} else {
						System.out.println(index + ":" + future.cause());
					}
					cnt.incrementAndGet();
					latch.countDown();
				}
			});
		}
		latch.await(30, TimeUnit.SECONDS);
		System.out.println(System.currentTimeMillis() - t);
		System.out.println(cnt);
		Thread.sleep(3000);
		System.out.println(client.getReleasedChannelCount(new InetSocketAddress("localhost", 8045)));
		client.close();
		
	}

}
