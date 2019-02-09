/*
 * Copyright (c) 2013 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.protocol.framework;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public abstract class AbstractProtocolSession<M> extends SimpleChannelInboundHandler<Object> implements ProtocolSession<M> {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractProtocolSession.class);

    /**
     * Handles incoming message (parsing, reacting if necessary).
     *
     * @param msg incoming message
     */
    protected abstract void handleMessage(final M msg);

    /**
     * Called when reached the end of input stream while reading.
     */
    protected abstract void endOfInput();

    /**
     * Called when the session is added to the pipeline.
     */
    protected abstract void sessionUp();

    // channel inactive最终触发上层对象感知
    @Override
    public final void channelInactive(final ChannelHandlerContext ctx) {
        LOG.debug("Channel {} inactive.", ctx.channel());
        endOfInput();
        try {
            // Forward channel inactive event, all handlers in pipeline might be interested in the event e.g. close channel handler of reconnect promise
            super.channelInactive(ctx);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to delegate channel inactive event on channel " + ctx.channel(), e);
        }
    }

    // 收到底层设备发送消息到channel，最终触发上层对象感知
    @Override
    @SuppressWarnings("unchecked")
    protected final void channelRead0(final ChannelHandlerContext ctx, final Object msg) {
        LOG.debug("Message was received: {}", msg);
        handleMessage((M) msg);
    }

    // 在NetconfClientSessionNegotiator中协商完成，就会将当前对象加入到pipeline中，然后触发sessionUp，让上层对象感知，
    @Override
    public final void handlerAdded(final ChannelHandlerContext ctx) {
        sessionUp();
    }
}
