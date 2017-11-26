package io.github.yangziwen.nettyhttpclient;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

public class NettyPooledClient<R> implements AutoCloseable {
	
	protected static final Logger logger = LoggerFactory.getLogger(NettyPooledClient.class);
	
	protected Bootstrap bootstrap = new Bootstrap().group(new NioEventLoopGroup());
	
	protected AbstractChannelPoolMap<InetSocketAddress, FixedChannelPool> channelPoolMap;
	
	protected ConcurrentHashMap<Channel, InetSocketAddress> channelAddressMapping = new ConcurrentHashMap<>();
	
	public NettyPooledClient(int poolSizePerAddress, ChannelPoolHandlerFactory<R> handlerFactory) {
		bootstrap.channel(NioSocketChannel.class)
			.option(ChannelOption.TCP_NODELAY, true)
			.option(ChannelOption.SO_KEEPALIVE, true);
		channelPoolMap = new AbstractChannelPoolMap<InetSocketAddress, FixedChannelPool>() {
			@Override
			protected FixedChannelPool newPool(InetSocketAddress key) {
				return new FixedChannelPool(bootstrap.remoteAddress(key), 
						handlerFactory.createHandler(NettyPooledClient.this), 
						poolSizePerAddress);
			}
		};
	}
	
	public Future<Channel> acquireChannel(InetSocketAddress address) {
		Promise<Channel> promise = bootstrap.config().group().next().newPromise();
		channelPoolMap.get(address).acquire().addListener(new FutureListener<Channel>() {
			@Override
			public void operationComplete(Future<Channel> future) throws Exception {
				if (future.isSuccess()) {
					Channel channel = future.get();
					channelAddressMapping.putIfAbsent(channel, address);
					promise.trySuccess(channel);
				} else {
					promise.tryFailure(future.cause());
				}
			}
		});
		return promise;
	}
	
	public Future<Void> releaseChannel(Channel channel) {
		InetSocketAddress address = channelAddressMapping.remove(channel);
		return channelPoolMap.get(address).release(channel);
	}
	

	@Override
	public void close() throws Exception {
		bootstrap.config().group().shutdownGracefully().sync();
	}
	
	public int getTotalPoolCnt() {
		return channelPoolMap.size();
	}
	
}
