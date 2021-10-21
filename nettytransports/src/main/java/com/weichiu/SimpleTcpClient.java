package com.weichiu;

import java.net.InetSocketAddress;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple TCP based RPC client which just sends a request to a server.
 */
public class SimpleTcpClient {
  public static final Logger LOG =
      LoggerFactory.getLogger(SimpleTcpClient.class);

  protected final String host;
  protected final int port;
  private ChannelFuture future;

  public SimpleTcpClient(String host, int port) {
    this.host = host;
    this.port = port;
  }

  public static class EventGroupFactory {
    private static String transport = System.getProperty("com.weichiu.netty.transport");

    public static EventLoopGroup createWorkerGroup() {
      ThreadFactory namedThreadFactory =
          new ThreadFactoryBuilder()
              .setNameFormat("client-worker-%d")
              .setDaemon(true)
              .build();

      if ((transport != null && transport.equals("nio"))) {
        return new NioEventLoopGroup(namedThreadFactory);
      }

      // use native transport whenever possible:
      // epoll for Linux and kqueue for Mac
      if (Epoll.isAvailable()) {
        return new EpollEventLoopGroup(namedThreadFactory);
      }

      if (KQueue.isAvailable()) {
        return new KQueueEventLoopGroup(namedThreadFactory);
      }

      return new NioEventLoopGroup(namedThreadFactory);
    }

    static Class<? extends SocketChannel> createSocketChannel() {
      if ((transport != null && transport.equals("nio"))) {
        return NioSocketChannel.class;
      }

      if (Epoll.isAvailable()) {
        return EpollSocketChannel.class;
      }

      if (KQueue.isAvailable()) {
        return KQueueSocketChannel.class;
      }

      return NioSocketChannel.class;
    }
  }

  protected ChannelInitializer<SocketChannel> setChannelHandler() {
    return new ChannelInitializer<SocketChannel>() {
      @Override
      protected void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline p = ch.pipeline();
        // FlushConsolidationHandler consolidates flushes, speeding up by 5x.
        //p.addLast(new FlushConsolidationHandler(256, true));
        p.addLast(new StringEncoder());
        p.addLast(new SimpleTcpClientHandler());
      }
    };
  }

  public void run(EventLoopGroup workerGroup) {
    // Configure the client.
    Bootstrap bootstrap = new Bootstrap()
        .group(workerGroup)
        .channel(EventGroupFactory.createSocketChannel());

    try {
      future = bootstrap.handler(setChannelHandler())
          .option(ChannelOption.TCP_NODELAY, true)
          .option(ChannelOption.SO_KEEPALIVE, true)
          // enlarge send/recv buffer size, increase throughput
          .option(ChannelOption.SO_RCVBUF, 1048576)
          .option(ChannelOption.SO_SNDBUF, 1048576)
          .option(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
          .option(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
          // use pooled allocator -- this is the default
          .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
          .connect(new InetSocketAddress(host, port)).sync();

      //LOG.info("isSuccess = {}", future.isSuccess());
      //LOG.info("isDone = {}", future.isDone());
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
    }
  }

  public Channel getChannel() {
    return future.channel();
  }

  public void stop() {
    LOG.info("stopping client");
    try {
      if (future != null) {
        // Wait until the connection is closed or the connection attempt fails.
        future.channel().close().sync();
        future.cancel(true);
      }
      // Shut down thread pools to exit.
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      LOG.info("client stopped");
    }
  }
}
