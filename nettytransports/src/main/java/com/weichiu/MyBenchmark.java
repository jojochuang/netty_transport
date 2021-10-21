/*
 * Copyright (c) 2005, 2013, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package com.weichiu;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.GenerateMicroBenchmark;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.channel.ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE;

/*@Warmup(iterations = 1, time = 1, timeUnit = SECONDS)
@Measurement(iterations = 1, time = 1, timeUnit = SECONDS)*/
@OutputTimeUnit(TimeUnit.MILLISECONDS)
//@Fork(value=1, jvmArgs = {"-Xms2G", "-Xmx2G"})
@Fork(value=1)
public class MyBenchmark {
    public static final Logger LOG =
        LoggerFactory.getLogger(MyBenchmark.class);

    @State(Scope.Benchmark)
    public static class BenchmarkServerState {
        private List<SimpleTcpClient> clients;
        private SimpleTcpServer server;
        private AtomicInteger sentMessages;
        private EventLoopGroup workerGroup;

        @Setup(Level.Trial)
        public void setup() throws InterruptedException {
            LOG.info("setting up server");
            sentMessages = new AtomicInteger();
            server = new SimpleTcpServer(10240, 4);
            server.run();

            LOG.info("setting up client");
            // share thread pool between clients
            workerGroup = SimpleTcpClient.EventGroupFactory.createWorkerGroup();

            // create multiple clients; one channel/socket per client.
            clients = new ArrayList<>();
            for (int i = 0; i< 32; i++) {
                SimpleTcpClient client = new SimpleTcpClient("localhost", 10240);
                client.run(workerGroup);
                clients.add(client);
            }
        }

        @TearDown(Level.Trial)
        public void cleanup() {
            for (SimpleTcpClient client: clients) {
                client.stop();
            }
            try {
                server.shutdown();
                workerGroup.shutdownGracefully().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @GenerateMicroBenchmark
    @GroupThreads(32)
    public void testMethod(BenchmarkServerState state) {
        int int_random = ThreadLocalRandom.current().nextInt(state.clients.size());

        final Channel channel = state.clients.get(int_random).getChannel();
        /*try {
            while (state.sentMessages.get() - state.server.getProcessedMessages() > 4096) {
                synchronized (this) {
                    wait(1);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        try {
            while (channel.bytesBeforeWritable() > 40960) {
                synchronized (this) {
                    wait(1);
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String msg = "abc";
        channel.writeAndFlush(msg)
            .addListener(FIRE_EXCEPTION_ON_FAILURE);
        state.sentMessages.addAndGet(msg.length());
            //.awaitUninterruptibly();
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
