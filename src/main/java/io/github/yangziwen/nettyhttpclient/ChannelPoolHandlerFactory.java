package io.github.yangziwen.nettyhttpclient;

import io.netty.channel.pool.ChannelPoolHandler;

public interface ChannelPoolHandlerFactory<R> {
	
	public ChannelPoolHandler createHandler(NettyPooledClient<R> client);

}
