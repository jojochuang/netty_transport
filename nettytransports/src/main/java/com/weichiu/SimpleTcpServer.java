package com.weichiu;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SimpleTcpServer {
  public static final Logger LOG =
      LoggerFactory.getLogger(SimpleTcpServer.class);
  protected final int port;
  protected int boundPort = -1; // Will be set after server starts
  private ServerBootstrap server;
  private Channel ch;
  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private ChannelFuture future;
  private SimpleTcpServerHandler handler;

  /** The maximum number of I/O worker threads */
  protected final int workerCount;

  /**
   * @param port TCP port where to start the server at
   * @param workercount Number of worker threads
   */
  public SimpleTcpServer(int port, int workercount) {
    this.port = port;
    this.workerCount = workercount;
  }

  public static class EventGroupFactory {
    private static String transport = System.getProperty("com.weichiu.netty.transport");

    static EventLoopGroup createBossGroup() {
      ThreadFactory namedThreadFactory =
          new ThreadFactoryBuilder().setNameFormat("server-boss-%d").build();

      if ((transport != null && transport.equals("nio"))) {
        LOG.debug("server uses nio transport");
        return new NioEventLoopGroup(namedThreadFactory);
      }

      if (Epoll.isAvailable()) {
        LOG.debug("server uses epoll transport");
        return new EpollEventLoopGroup(namedThreadFactory);
      }

      if (KQueue.isAvailable()) {
        LOG.debug("server uses kqueue transport");
        return new KQueueEventLoopGroup(namedThreadFactory);
      }

      LOG.debug("server uses nio transport");
      return new NioEventLoopGroup(namedThreadFactory);
    }

    static EventLoopGroup createWorkerGroup(int workerCount) {
      ThreadFactory namedThreadFactory =
          new ThreadFactoryBuilder().setNameFormat("server-worker-%d").setDaemon(true).build();

      ExecutorService
          service = new ThreadPoolExecutor(workerCount, workerCount, 0L,
          TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
          namedThreadFactory);

      if ((transport != null && transport.equals("nio"))) {
        return new NioEventLoopGroup(workerCount, service);
      }

      if (Epoll.isAvailable()) {
        return new EpollEventLoopGroup(workerCount, service);
      }

      if (KQueue.isAvailable()) {
        return new KQueueEventLoopGroup(workerCount, service);
      }

      return new NioEventLoopGroup(workerCount, service);
    }

    static Class<? extends ServerChannel> createServerSocketChannel() {
      if ((transport != null && transport.equals("nio"))) {
        return NioServerSocketChannel.class;
      }

      if (Epoll.isAvailable()) {
        return EpollServerSocketChannel.class;
      }

      if (KQueue.isAvailable()) {
        return KQueueServerSocketChannel.class;
      }

      return NioServerSocketChannel.class;
    }
  }

  public int getProcessedMessages() {
    return handler.getProcessedMessages();
  }

  public void run() throws InterruptedException {
    // Configure the Server.
    bossGroup = EventGroupFactory.createBossGroup();
    workerGroup = EventGroupFactory.createWorkerGroup(workerCount);

    handler = new SimpleTcpServerHandler();

    server = new ServerBootstrap();

    server.group(bossGroup, workerGroup)
        .channel(EventGroupFactory.createServerSocketChannel())
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            p.addLast(handler);
          }})
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childOption(ChannelOption.SO_KEEPALIVE, true)
        .childOption(ChannelOption.SO_REUSEADDR, true)
        .childOption(ChannelOption.SO_RCVBUF, 1048576)
        .childOption(ChannelOption.SO_SNDBUF, 1048576)
        .childOption(ChannelOption.WRITE_BUFFER_HIGH_WATER_MARK, 32 * 1024)
        .childOption(ChannelOption.WRITE_BUFFER_LOW_WATER_MARK, 8 * 1024)
        // use pooled allocator -- this is the default
        .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        .option(ChannelOption.SO_REUSEADDR, true);


    // Listen to TCP port
    future = server.bind(new InetSocketAddress(port)).sync();
    ch = future.channel();
    InetSocketAddress socketAddr = (InetSocketAddress) ch.localAddress();
    boundPort = socketAddr.getPort();

    LOG.info("Started listening to TCP requests at port " + boundPort + " for "
        + " with workerCount " + workerCount);
  }

  // boundPort will be set only after server starts
  public int getBoundPort() {
    return this.boundPort;
  }

  public void shutdown() throws InterruptedException {
    LOG.info("stopping server");
    if (ch != null) {
      ch.close().sync();
      ch = null;
    }

    future.cancel(true);

    if (workerGroup != null) {
      workerGroup.shutdownGracefully().sync();
      workerGroup = null;
    }

    if (bossGroup != null) {
      bossGroup.shutdownGracefully().sync();
      bossGroup = null;
    }
    LOG.info("server stopped");
    LOG.info("server processed messages {}", getProcessedMessages());
  }
}