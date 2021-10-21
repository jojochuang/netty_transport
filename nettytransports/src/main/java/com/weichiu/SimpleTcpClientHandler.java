package com.weichiu;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleTcpClientHandler extends ChannelInboundHandlerAdapter {
  public static final Logger LOG =
      LoggerFactory.getLogger(SimpleTcpClientHandler.class);



  public SimpleTcpClientHandler() {
  }

  @Override
  public void channelActive(ChannelHandlerContext ctx) throws Exception {
    // Send the request
    LOG.info("connected");
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) {
    ByteBuf m = (ByteBuf) msg;
    try {
      //String str = m.toString();
      //LOG.info("received response '{}'", str);
    } finally {
      m.release();
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    LOG.warn("Unexpected exception from downstream: ", cause.getCause());
    LOG.info(cause.getLocalizedMessage());
    ctx.fireExceptionCaught(cause);
    //ctx.channel().close();
  }
}