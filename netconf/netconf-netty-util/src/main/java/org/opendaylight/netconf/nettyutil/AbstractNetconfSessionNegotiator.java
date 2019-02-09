/*
 * Copyright (c) 2013 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package org.opendaylight.netconf.nettyutil;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.Promise;
import java.util.concurrent.TimeUnit;
import org.opendaylight.netconf.api.NetconfDocumentedException;
import org.opendaylight.netconf.api.NetconfMessage;
import org.opendaylight.netconf.api.NetconfSessionListener;
import org.opendaylight.netconf.api.NetconfSessionPreferences;
import org.opendaylight.netconf.api.messages.NetconfHelloMessage;
import org.opendaylight.netconf.api.xml.XmlNetconfConstants;
import org.opendaylight.netconf.nettyutil.handler.FramingMechanismHandlerFactory;
import org.opendaylight.netconf.nettyutil.handler.NetconfChunkAggregator;
import org.opendaylight.netconf.nettyutil.handler.NetconfMessageToXMLEncoder;
import org.opendaylight.netconf.nettyutil.handler.NetconfXMLToHelloMessageDecoder;
import org.opendaylight.netconf.nettyutil.handler.NetconfXMLToMessageDecoder;
import org.opendaylight.netconf.util.messages.FramingMechanism;
import org.opendaylight.protocol.framework.AbstractSessionNegotiator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public abstract class AbstractNetconfSessionNegotiator<P extends NetconfSessionPreferences,
        S extends AbstractNetconfSession<S, L>, L extends NetconfSessionListener<S>>
    extends AbstractSessionNegotiator<NetconfHelloMessage, S> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractNetconfSessionNegotiator.class);

    public static final String NAME_OF_EXCEPTION_HANDLER = "lastExceptionHandler";

    protected final P sessionPreferences;

    private final L sessionListener;
    private Timeout timeout;

    /**
     * Possible states for Finite State Machine.
     */
    protected enum State {
        IDLE, OPEN_WAIT, FAILED, ESTABLISHED
    }

    private State state = State.IDLE;
    private final Promise<S> promise;
    private final Timer timer;
    private final long connectionTimeoutMillis;

    protected AbstractNetconfSessionNegotiator(final P sessionPreferences, final Promise<S> promise,
                                               final Channel channel, final Timer timer,
                                               final L sessionListener, final long connectionTimeoutMillis) {
        super(promise, channel);
        this.sessionPreferences = sessionPreferences;
        this.promise = promise;
        this.timer = timer;
        this.sessionListener = sessionListener;
        this.connectionTimeoutMillis = connectionTimeoutMillis;
    }

    /**
     * 由AbstarctSessionNegotiator.channelActive触发调用当前方法，当netty channel ok了开始协商.
     */
    @Override
    protected final void startNegotiation() {
        final Optional<SslHandler> sslHandler = getSslHandler(channel);
        // 首先判断当前netconf连接底层是否使用ssh,还是直接的tcp/ssl
        if (sslHandler.isPresent()) {
            // 使用tcp/ssl情况下，等ssl握手完成
            Future<Channel> future = sslHandler.get().handshakeFuture();
            future.addListener(new GenericFutureListener<Future<? super Channel>>() {
                @Override
                public void operationComplete(final Future<? super Channel> future) {
                    Preconditions.checkState(future.isSuccess(), "Ssl handshake was not successful");
                    LOG.debug("Ssl handshake complete");
                    // ssl已经ok了，开始netconf协商
                    start();
                }
            });
        } else {
            // 当是使用ssh情况下，AsyncSshHandler已经建立连接下，才会触发chanelActive，所以这里直接开启netconf协商即可
            start();
        }
    }

    private static Optional<SslHandler> getSslHandler(final Channel channel) {
        final SslHandler sslHandler = channel.pipeline().get(SslHandler.class);
        return sslHandler == null ? Optional.<SslHandler>absent() : Optional.of(sslHandler);
    }

    public P getSessionPreferences() {
        return sessionPreferences;
    }

    /**
     * Netconf协议协商的过程：发送hello message.
     */
    private void start() {
        final NetconfHelloMessage helloMessage = this.sessionPreferences.getHelloMessage();
        LOG.debug("Session negotiation started with hello message {} on channel {}", helloMessage, channel);

        channel.pipeline().addLast(NAME_OF_EXCEPTION_HANDLER, new ExceptionHandlingInboundChannelHandler());

        // 发送hello消息
        sendMessage(helloMessage);

        // 发送完hello消息后，pipeline中NETCONF_MESSAGE_ENCODER将NetconfHelloMessageToXMLEncoder替换为NetconfMessageToXMLEncoder
        replaceHelloMessageOutboundHandler();
        // 修改netconf session状态为OPEN_WAIT
        changeState(State.OPEN_WAIT);

        // 等待一段时间，检查session状态是否为ESTABLISHED. 状态的修改由另外的异步(hello message交互)实现
        timeout = this.timer.newTimeout(new TimerTask() {
            @Override
            @SuppressWarnings("checkstyle:hiddenField")
            public void run(final Timeout timeout) {
                synchronized (this) {
                    if (state != State.ESTABLISHED) {

                        LOG.debug("Connection timeout after {}, session is in state {}", timeout, state);

                        // Do not fail negotiation if promise is done or canceled
                        // It would result in setting result of the promise second time and that throws exception
                        if (!isPromiseFinished()) {
                            LOG.warn("Netconf session was not established after {}", connectionTimeoutMillis);
                            // 修改状态为FAILED
                            changeState(State.FAILED);

                            channel.close().addListener(new GenericFutureListener<ChannelFuture>() {
                                @Override
                                public void operationComplete(final ChannelFuture future) throws Exception {
                                    if (future.isSuccess()) {
                                        LOG.debug("Channel {} closed: success", future.channel());
                                    } else {
                                        LOG.warn("Channel {} closed: fail", future.channel());
                                    }
                                }
                            });
                        }
                    } else if (channel.isOpen()) {
                        channel.pipeline().remove(NAME_OF_EXCEPTION_HANDLER);
                    }
                }
            }

            private boolean isPromiseFinished() {
                return promise.isDone() || promise.isCancelled();
            }

        }, connectionTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void cancelTimeout() {
        if (timeout != null) {
            timeout.cancel();
        }
    }

    protected final S getSessionForHelloMessage(final NetconfHelloMessage netconfMessage)
            throws NetconfDocumentedException {
        Preconditions.checkNotNull(netconfMessage, "netconfMessage");

        final Document doc = netconfMessage.getDocument();

        if (shouldUseChunkFraming(doc)) {
            insertChunkFramingToPipeline();
        }

        // 修改状态
        changeState(State.ESTABLISHED);
        return getSession(sessionListener, channel, netconfMessage);
    }

    /**
     * Insert chunk framing handlers into the pipeline.
     */
    private void insertChunkFramingToPipeline() {
        replaceChannelHandler(channel, AbstractChannelInitializer.NETCONF_MESSAGE_FRAME_ENCODER,
                FramingMechanismHandlerFactory.createHandler(FramingMechanism.CHUNK));
        replaceChannelHandler(channel, AbstractChannelInitializer.NETCONF_MESSAGE_AGGREGATOR,
                new NetconfChunkAggregator());
    }

    private boolean shouldUseChunkFraming(final Document doc) {
        return containsBase11Capability(doc)
                && containsBase11Capability(sessionPreferences.getHelloMessage().getDocument());
    }

    /**
     * Remove special inbound handler for hello message. Insert regular netconf xml message (en|de)coders.
     *
     * <p>
     * Inbound hello message handler should be kept until negotiation is successful
     * It caches any non-hello messages while negotiation is still in progress
     */
    protected final void replaceHelloMessageInboundHandler(final S session) {
        // 接收完NetconfHelloMessage后，将pipeline中NetconfXMLToHelloMessageDecoder替换为NetconfXMLToMessageDecoder
        ChannelHandler helloMessageHandler = replaceChannelHandler(channel,
                AbstractChannelInitializer.NETCONF_MESSAGE_DECODER, new NetconfXMLToMessageDecoder());

        Preconditions.checkState(helloMessageHandler instanceof NetconfXMLToHelloMessageDecoder,
                "Pipeline handlers misplaced on session: %s, pipeline: %s", session, channel.pipeline());
        // 取出在协商过程中收到的非NetconfHelloMessage，并在下面交给上层处理
        Iterable<NetconfMessage> netconfMessagesFromNegotiation =
                ((NetconfXMLToHelloMessageDecoder) helloMessageHandler).getPostHelloNetconfMessages();

        // Process messages received during negotiation
        // The hello message handler does not have to be synchronized,
        // since it is always call from the same thread by netty.
        // It means, we are now using the thread now
        for (NetconfMessage message : netconfMessagesFromNegotiation) {
            session.handleMessage(message);
        }
    }

    /**
     * Remove special outbound handler for hello message. Insert regular netconf xml message (en|de)coders.
     */
    private void replaceHelloMessageOutboundHandler() {
        replaceChannelHandler(channel, AbstractChannelInitializer.NETCONF_MESSAGE_ENCODER,
                new NetconfMessageToXMLEncoder());
    }

    private static ChannelHandler replaceChannelHandler(final Channel channel, final String handlerKey,
                                                        final ChannelHandler decoder) {
        return channel.pipeline().replace(handlerKey, handlerKey, decoder);
    }

    @SuppressWarnings("checkstyle:hiddenField")
    protected abstract S getSession(L sessionListener, Channel channel, NetconfHelloMessage message)
            throws NetconfDocumentedException;

    private synchronized void changeState(final State newState) {
        LOG.debug("Changing state from : {} to : {} for channel: {}", state, newState, channel);
        Preconditions.checkState(isStateChangePermitted(state, newState),
                "Cannot change state from %s to %s for chanel %s", state, newState, channel);
        this.state = newState;
    }

    private static boolean containsBase11Capability(final Document doc) {
        final NodeList nList = doc.getElementsByTagNameNS(
            XmlNetconfConstants.URN_IETF_PARAMS_XML_NS_NETCONF_BASE_1_0,
            XmlNetconfConstants.CAPABILITY);
        for (int i = 0; i < nList.getLength(); i++) {
            if (nList.item(i).getTextContent().contains(XmlNetconfConstants.URN_IETF_PARAMS_NETCONF_BASE_1_1)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isStateChangePermitted(final State state, final State newState) {
        if (state == State.IDLE && newState == State.OPEN_WAIT) {
            return true;
        }
        if (state == State.OPEN_WAIT && newState == State.ESTABLISHED) {
            return true;
        }
        if (state == State.OPEN_WAIT && newState == State.FAILED) {
            return true;
        }
        LOG.debug("Transition from {} to {} is not allowed", state, newState);
        return false;
    }

    /**
     * Handler to catch exceptions in pipeline during negotiation.
     */
    private final class ExceptionHandlingInboundChannelHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            LOG.warn("An exception occurred during negotiation with {}", channel.remoteAddress(), cause);
            cancelTimeout();
            negotiationFailed(cause);
            changeState(State.FAILED);
        }
    }
}
