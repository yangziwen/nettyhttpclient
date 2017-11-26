package io.github.yangziwen.nettyhttpclient;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.AbstractChannelPoolMap;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

public class NettyPooledClient<R> implements AutoCloseable {
	
	private static final Logger logger = LoggerFactory.getLogger(NettyPooledClient.class);
	
	AttributeKey<Promise<R>> RESPONSE_PROMISE_KEY = AttributeKey.valueOf("response_promise");
	
	private EventLoopGroup group = new NioEventLoopGroup();
	
	private Bootstrap bootstrap = new Bootstrap();
	
	private AbstractChannelPoolMap<InetSocketAddress, FixedChannelPool> channelPoolMap;
	
	private ConcurrentHashMap<Channel, InetSocketAddress> channelAddressMapping = new ConcurrentHashMap<>();
	
	public NettyPooledClient(int poolSizePerAddress, ChannelPoolHandlerFactory<R> handlerFactory) {
		bootstrap.group(group).channel(NioSocketChannel.class)
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
		return channelPoolMap.get(address).acquire();
	}
	
	public Future<Void> releaseChannel(Channel channel) {
		InetSocketAddress address = channelAddressMapping.remove(channel);
		return channelPoolMap.get(address).release(channel);
	}
	
	public Promise<R> sendGet(URI uri, Map<String, Object> params) {
		InetSocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
		Promise<R> promise = group.next().newPromise();
		Future<Channel> future = acquireChannel(address);
		future.addListener(new FutureListener<Channel>() {
			@Override
			public void operationComplete(Future<Channel> future) throws Exception {
				if (!future.isSuccess()) {
					logger.error("failed to acquire client for uri[{}]", uri);
					promise.tryFailure(future.cause());
					return;
				}
				Channel channel = future.get();
				channelAddressMapping.putIfAbsent(channel, address);
				QueryStringEncoder encoder = new QueryStringEncoder(String.valueOf(uri));
				for (Entry<String, Object> entry : params.entrySet()) {
					encoder.addParam(entry.getKey(), String.valueOf(entry.getValue()));
				}
				HttpRequest request = createHttpRequest(new URI(encoder.toString()), HttpMethod.GET);
				channel.writeAndFlush(request);
				setResponsePromise(channel, promise);
			}
		});
		return promise;
	}
	
	public Promise<R> sendPost(URI uri, Map<String, Object> params) {
		InetSocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
		Promise<R> promise = group.next().newPromise();
		Future<Channel> future = acquireChannel(address);
		future.addListener(new FutureListener<Channel>() {
			@Override
			public void operationComplete(Future<Channel> future) throws Exception {
				if (!future.isSuccess()) {
					logger.error("failed to acquire client for uri[{}]", uri);
					promise.tryFailure(future.cause());
					return;
				}
				Channel channel = future.get();
				channelAddressMapping.putIfAbsent(channel, address);
				HttpRequest request = createHttpRequest(uri, HttpMethod.POST);
				HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(request, false);
				for (Entry<String, Object> entry : params.entrySet()) {
					encoder.addBodyAttribute(entry.getKey(), String.valueOf(entry.getValue()));
				}
				channel.writeAndFlush(encoder.finalizeRequest());
				setResponsePromise(channel, promise);
			}
		});
		return promise;
	}
	
	private HttpRequest createHttpRequest(URI uri, HttpMethod method) {
		HttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, method, uri.getRawPath());
		request.headers()
			.set(HttpHeaderNames.HOST, uri.getHost())
			.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE)
			.set(HttpHeaderNames.ACCEPT_ENCODING, HttpHeaderValues.GZIP);
		return request;
	}
	
	private Channel setResponsePromise(Channel channel, Promise<R> promise) {
		Attribute<Promise<R>> attr = channel.attr(RESPONSE_PROMISE_KEY);
		attr.set(promise);
		return channel;
	}

	@Override
	public void close() throws Exception {
		for (Entry<InetSocketAddress, FixedChannelPool> entry : channelPoolMap) {
			entry.getValue().close();
		}
		group.shutdownGracefully().sync();
	}
	
	public int getTotalPoolCnt() {
		return channelPoolMap.size();
	}
	
}
