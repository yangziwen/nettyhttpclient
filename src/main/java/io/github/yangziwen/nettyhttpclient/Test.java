package io.github.yangziwen.nettyhttpclient;

import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import io.github.yangziwen.nettyhttpclient.NettyPooledHttpClient.Response;

public class Test {

	public static void main(String[] args) throws Exception {
		NettyPooledHttpClient client = new NettyPooledHttpClient(500, 3, 5, TimeUnit.SECONDS);
		String url = "http://localhost:8030/test";

		sendRequests(client, url, 6000, response -> {}, error -> {});
		System.out.println("warm up finished");

		int n = 10000;
		long t = System.currentTimeMillis();
		sendRequests(client, url, n, response -> {}, System.err::println);
		System.out.println("cost " + (System.currentTimeMillis() - t) + "ms to send " + n + " requests");
		client.close();
	}

	private static void sendRequests(NettyPooledHttpClient client, String url, int times,
			Consumer<Response> successCallback, Consumer<Throwable> errorCallback) throws Exception {
		URI uri = new URI(url);
		CountDownLatch latch = new CountDownLatch(times);
		for (int i = 0; i < times; i++) {
			client.sendGet(uri).addListener(future -> {
				if (future.isSuccess()) {
					successCallback.accept((Response) future.get());
				} else {
					errorCallback.accept(future.cause());
				}
				latch.countDown();
			});
		}
		latch.await();
	}

}
