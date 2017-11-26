package io.github.yangziwen.nettyhttpclient;

import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.yangziwen.nettyhttpclient.NettyPooledHttpClient.Response;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

public class Test {
	
	public static void main(String[] args) throws Exception {
		NettyPooledHttpClient client = new NettyPooledHttpClient(5);
		URI uri = new URI("http://localhost:8070/stats/errors.json?startDate=2017-06-02&endDate=2017-06-10");
		AtomicInteger cnt = new AtomicInteger();
		for (int i = 0; i < 100; i++) {
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
				}
			});
		}
		Thread.sleep(5000);
		System.out.println(cnt);
		client.close();
		
	}

}
