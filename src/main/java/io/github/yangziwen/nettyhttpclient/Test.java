package io.github.yangziwen.nettyhttpclient;

import java.net.URI;
import java.util.Collections;

import io.github.yangziwen.nettyhttpclient.NettyHttpPoolHandler.Response;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.util.concurrent.Promise;

public class Test {
	
	public static void main(String[] args) throws Exception {
		NettyPooledClient<Response> client = new NettyPooledClient<>(10, new ChannelPoolHandlerFactory<Response>() {
			@Override
			public ChannelPoolHandler createHandler(NettyPooledClient<Response> client) {
				return new NettyHttpPoolHandler(client);
			}
		});
		Promise<Response> promise1 = client.sendGet(new URI("http://localhost:8045/job/codeline/metrics"), Collections.<String, Object>emptyMap());
		Promise<Response> promise2 = client.sendGet(new URI("http://localhost:8045/job/codeline/metrics1"), Collections.<String, Object>emptyMap());
		Thread.sleep(2000);
		System.out.println(promise1.get());
		System.out.println(promise2.get());
		client.close();
		
	}

}
