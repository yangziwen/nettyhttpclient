package io.github.yangziwen.nettyhttpclient;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import io.github.yangziwen.nettyhttpclient.NettyPooledHttpClient.Response;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
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
import io.netty.util.concurrent.Promise;
import io.netty.util.concurrent.ScheduledFuture;

public class NettyPooledHttpClient extends NettyPooledClient<Response> {
	
	private static final AttributeKey<Promise<Response>> RESPONSE_PROMISE_KEY = AttributeKey.valueOf("response_promise");
	
	private static final AttributeKey<Response> HTTP_RESPONSE_KEY = AttributeKey.valueOf("http_response");
	
	public NettyPooledHttpClient(int poolSizePerAddress) {
		this(poolSizePerAddress, 0, 0, TimeUnit.SECONDS);
	}

	public NettyPooledHttpClient(int poolSizePerAddress, int nThreads, long timeout, TimeUnit timeoutUnit) {
		super(poolSizePerAddress, NettyHttpPoolHandler::new, nThreads, timeout, timeoutUnit);
	}
	
	@Override
	public Future<Channel> acquireChannel(InetSocketAddress address) {
		return super.acquireChannel(address).addListener(future -> {
			if (future.isSuccess()) {
				Channel channel = (Channel) future.get();
				channel.pipeline().get(NettyHttpTimeoutHandler.class).enable();
			}
		});
	}
	
	@Override
	public Future<Void> releaseChannel(Channel channel) {
		channel.pipeline().get(NettyHttpTimeoutHandler.class).disable();
		return super.releaseChannel(channel);
	}
	
	public Promise<Response> sendGet(URI uri) {
		return sendGet(uri, Collections.emptyMap());
	}

	public Promise<Response> sendGet(URI uri, Map<String, Object> params) {
		InetSocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
		Promise<Response> promise = newPromise();
		acquireChannel(address).addListener(future -> {
			if (!future.isSuccess()) {
				logger.error("failed to acquire client for uri[{}]", uri);
				promise.tryFailure(future.cause());
				return;
			}
			Channel channel = (Channel) future.get();
			channel.attr(RESPONSE_PROMISE_KEY).set(promise);
			QueryStringEncoder encoder = new QueryStringEncoder(String.valueOf(uri));
			for (Entry<String, Object> entry : params.entrySet()) {
				encoder.addParam(entry.getKey(), String.valueOf(entry.getValue()));
			}
			HttpRequest request = createHttpRequest(new URI(encoder.toString()), HttpMethod.GET);
			channel.writeAndFlush(request);
		});
		return promise;
	}
	
	public Promise<Response> sendPost(URI uri, Map<String, Object> params) {
		InetSocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort());
		Promise<Response> promise = newPromise();
		acquireChannel(address).addListener(future -> {
			if (!future.isSuccess()) {
				logger.error("failed to acquire client for uri[{}]", uri);
				promise.tryFailure(future.cause());
				return;
			}
			Channel channel = (Channel) future.get();
			channel.attr(RESPONSE_PROMISE_KEY).set(promise);;
			HttpRequest request = createHttpRequest(uri, HttpMethod.POST);
			HttpPostRequestEncoder encoder = new HttpPostRequestEncoder(request, false);
			for (Entry<String, Object> entry : params.entrySet()) {
				encoder.addBodyAttribute(entry.getKey(), String.valueOf(entry.getValue()));
			}
			channel.writeAndFlush(encoder.finalizeRequest());
		});
		return promise;
	}
	
	private HttpRequest createHttpRequest(URI uri, HttpMethod method) {
		HttpRequest request = new DefaultFullHttpRequest(
                HttpVersion.HTTP_1_1, method, uri.getRawPath());
		request.headers()
				.set(HttpHeaderNames.HOST, uri.getHost())
				.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
		return request;
	}
	
	public static class NettyHttpPoolHandler extends AbstractChannelPoolHandler {
		
		private NettyPooledClient<Response> client;
		
		public NettyHttpPoolHandler(NettyPooledClient<Response> client) {
			this.client = client;
		}
		
		@Override
		public void channelCreated(Channel channel) throws Exception {
			channel.pipeline()
				.addLast(new NettyHttpTimeoutHandler(client))
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
							Promise<Response> promise = channel.attr(RESPONSE_PROMISE_KEY).getAndSet(null);
							client.releaseChannel(channel);
							promise.trySuccess(response);
						}
					}
					public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
				            throws Exception {
						Promise<Response> promise = channel.attr(RESPONSE_PROMISE_KEY).getAndSet(null);
		    			ctx.close().addListener(f -> {
		    				client.releaseChannel(ctx.channel());
		    			});
						promise.tryFailure(cause);
				    }
				});
		}
		

	}
	
	public static class NettyHttpTimeoutHandler extends ChannelDuplexHandler {
		
		private NettyPooledClient<Response> client;
		
		private ScheduledFuture<?> future;
		
		private ChannelHandlerContext context;
		
		public NettyHttpTimeoutHandler(NettyPooledClient<Response> client) {
			this.client = client;
		}
		
	    @Override
	    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
	    	context = ctx;
	    }
	    
	    public void enable() {
	    	if (client.timeout <= 0) {
	    		return;
	    	}
    		future = context.executor().schedule(() -> {
    			String message = String.format("channel%s is timeout and will be closed", context.channel());
    			context.fireExceptionCaught(new TimeoutException(message));
    		}, client.timeout, client.timeoutUnit);
	    }
	    
	    public void disable() {
	    	if (future == null) {
	    		return;
	    	}
	    	future.cancel(true);
	    	future = null;
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
