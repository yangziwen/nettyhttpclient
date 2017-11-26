package io.github.yangziwen.nettyhttpclient;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import io.github.yangziwen.nettyhttpclient.NettyPooledHttpClient.Response;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.codec.http.QueryStringEncoder;
import io.netty.handler.codec.http.multipart.HttpPostRequestEncoder;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

public class NettyPooledHttpClient extends NettyPooledClient<Response> {
	
	public static final AttributeKey<Promise<Response>> RESPONSE_PROMISE_KEY = AttributeKey.valueOf("response_promise");
	
	private static final AttributeKey<Response> HTTP_RESPONSE_KEY = AttributeKey.valueOf("http_response");

	public NettyPooledHttpClient(int poolSizePerAddress) {
		super(poolSizePerAddress, NettyHttpPoolHandler::new);
	}
	
	public Promise<Response> sendGet(URI uri) {
		return sendGet(uri, Collections.emptyMap());
	}

	public Promise<Response> sendGet(URI uri, Map<String, Object> params) {
		InetSocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
		Promise<Response> promise = bootstrap.config().group().next().newPromise();
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
	
	public Promise<Response> sendPost(URI uri, Map<String, Object> params) {
		InetSocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
		Promise<Response> promise = bootstrap.config().group().next().newPromise();
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
		request.headers().set(HttpHeaderNames.HOST, uri.getHost());
		return request;
	}
	
	private Channel setResponsePromise(Channel channel, Promise<Response> promise) {
		channel.attr(RESPONSE_PROMISE_KEY).set(promise);
		return channel;
	}
	
	public static class NettyHttpPoolHandler extends AbstractChannelPoolHandler {
		
		private NettyPooledClient<Response> client;
		
		public NettyHttpPoolHandler(NettyPooledClient<Response> client) {
			this.client = client;
		}

		@Override
		public void channelCreated(Channel channel) throws Exception {
			channel.pipeline()
				.addLast(new HttpClientCodec())
				.addLast(new SimpleChannelInboundHandler<HttpObject>() {
					@Override
					protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) throws Exception {
						if (msg instanceof HttpResponse) {
							HttpResponse resp = (HttpResponse) msg;
							Response response = new Response(resp.status().code());
							channel.attr(HTTP_RESPONSE_KEY).set(response);
							return;
						}
						if (msg instanceof HttpContent) {
							HttpContent content = (HttpContent) msg;
							Response response = channel.attr(HTTP_RESPONSE_KEY).get();
							response.appendContent(content.content().toString(CharsetUtil.UTF_8));
						}
						if (msg instanceof LastHttpContent) {
							Response response = channel.attr(HTTP_RESPONSE_KEY).get();
							client.releaseChannel(channel);
							Promise<Response> promise = channel.attr(RESPONSE_PROMISE_KEY).get();
							promise.trySuccess(response);
						}
					}
					public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
				            throws Exception {
						Promise<Response> promise = channel.attr(RESPONSE_PROMISE_KEY).get();
						client.releaseChannel(channel);
						promise.tryFailure(cause);
				    }
				});
		}
		

	}

	
	public static class Response {
		
		private int code;
		
		private StringBuilder contentBuffer = new StringBuilder();
		
		public Response(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}

		public void setCode(int code) {
			this.code = code;
		}

		public String getContent() {
			return contentBuffer.toString();
		}

		public Response appendContent(String content) {
			contentBuffer.append(content);
			return this;
		}
		
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
		}
		
	}
}
