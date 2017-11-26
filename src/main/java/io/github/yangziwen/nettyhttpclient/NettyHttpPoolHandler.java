package io.github.yangziwen.nettyhttpclient;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.pool.AbstractChannelPoolHandler;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.AttributeKey;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Promise;

public class NettyHttpPoolHandler extends AbstractChannelPoolHandler {
	
	private AttributeKey<Response> HTTP_RESPONSE_KEY = AttributeKey.valueOf("http_response");
	
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
						Promise<Response> promise = channel.attr(client.RESPONSE_PROMISE_KEY).get();
						promise.trySuccess(response);
					}
				}
				public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
			            throws Exception {
					client.releaseChannel(channel);
					Promise<Response> promise = channel.attr(client.RESPONSE_PROMISE_KEY).get();
					promise.tryFailure(cause);
			    }
			});
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
