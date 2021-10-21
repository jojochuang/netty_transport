package com.weichiu;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

@ChannelHandler.Sharable
public class SimpleTcpServerHandler extends ChannelInboundHandlerAdapter {
  public static final Logger LOG =
      LoggerFactory.getLogger(SimpleTcpServerHandler.class);

  private AtomicInteger receivedMessages = new AtomicInteger();

  public int getProcessedMessages() {
    return receivedMessages.get();
  }

  @Override
  public void channelActive(final ChannelHandlerContext ctx) {
    LOG.info("server channel is active");
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    ByteBuf inBuffer = (ByteBuf) msg;

    String received = inBuffer.toString(CharsetUtil.UTF_8);
    /*LOG.info("Server received: " + received);

    ctx.writeAndFlush("Hello " + received);*/
    receivedMessages.addAndGet(received.length());

    inBuffer.release();
  }

  /*@Override
  public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
        .addListener(ChannelFutureListener.CLOSE);
    ctx.flush();
  }*/

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    cause.printStackTrace();
    ctx.close();
  }
}
