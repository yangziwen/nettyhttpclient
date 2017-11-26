package io.github.yangziwen.nettyhttpclient;

import java.net.URI;

import io.github.yangziwen.nettyhttpclient.NettyHttpPoolHandler.Response;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

public class Test {
	
	public static void main(String[] args) throws Exception {
		NettyPooledClient<Response> client = new NettyPooledClient<>(5, NettyHttpPoolHandler::new);
		URI uri = new URI("http://localhost:8070/stats/errors1.json?startDate=2017-06-02&endDate=2017-06-10");
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
				}
			});
		}
		Thread.sleep(10000);
		client.close();
		
	}

}
