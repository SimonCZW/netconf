/*
 * Copyright (c) 2014 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.netconf.sal.connect.netconf.sal;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opendaylight.controller.md.sal.dom.api.DOMRpcAvailabilityListener;
import org.opendaylight.controller.md.sal.dom.api.DOMRpcException;
import org.opendaylight.controller.md.sal.dom.api.DOMRpcIdentifier;
import org.opendaylight.controller.md.sal.dom.api.DOMRpcImplementationNotAvailableException;
import org.opendaylight.controller.md.sal.dom.api.DOMRpcResult;
import org.opendaylight.controller.md.sal.dom.api.DOMRpcService;
import org.opendaylight.controller.md.sal.dom.spi.DefaultDOMRpcResult;
import org.opendaylight.netconf.api.NetconfMessage;
import org.opendaylight.netconf.sal.connect.api.MessageTransformer;
import org.opendaylight.netconf.sal.connect.api.RemoteDeviceCommunicator;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.yang.common.RpcResult;
import org.opendaylight.yangtools.yang.data.api.schema.NormalizedNode;
import org.opendaylight.yangtools.yang.model.api.SchemaContext;
import org.opendaylight.yangtools.yang.model.api.SchemaPath;

/**
 * Invokes RPC by sending netconf message via listener. Also transforms result from NetconfMessage to CompositeNode.
 */
public final class NetconfDeviceRpc implements DOMRpcService {

    private final RemoteDeviceCommunicator<NetconfMessage> listener;
    private final MessageTransformer<NetconfMessage> transformer;
    private final Collection<DOMRpcIdentifier> availableRpcs;

    /**
     * 有两个地方实例化NetconfDeviceRpc，netconfDevice.onRemoteSessionUp
     * 	1.首先会传入BaseSchema创建NetconfDeviceRpc
     * 		netconf标准的几个RPC
     * 	2.其次会在netconf协商后（学习到device自身的额外支出的Rpc），在NetconfDevice.SchemaSetup中创建
     * 		device特有的RPC:
     *          在NetconfDevice.setUpSchema过程中创建device对应的RPC类，后续会在一路调用链:
     *              NetconfDeviceSalFacade.onDeviceConnected -> NetconfDeviceSalProvider.mountInstance.onTopologyDeviceConnected
     *
     *      最终在mountInstance.onTopologyDeviceConnected中将此NetconfDeviceRpc对象注册到DOMMountPointService！
     *      上层应用调用时从DOMMountPointService中拿到的DOMRPCService，其实是当前NetconfDeviceRpc对象！
     *
     * @param schemaContext
     * @param listener 封装底层，请求listener就是请求底层device设备
     * @param transformer transformer是在NetconfDevice中已经根据协议交互的yang rpc(SchemaContext)构建好的对象
     *  个人理解是已经有对应netconf rpc及数据结构map映射，用于后续跟进input node yang对象翻译为NetconfMessage
     */
    public NetconfDeviceRpc(final SchemaContext schemaContext, final RemoteDeviceCommunicator<NetconfMessage> listener,
                            final MessageTransformer<NetconfMessage> transformer) {
        this.listener = listener;
        this.transformer = transformer;

        availableRpcs = Collections2.transform(schemaContext.getOperations(),
            input -> DOMRpcIdentifier.create(input.getPath()));
    }

    @Nonnull
    @Override
    public CheckedFuture<DOMRpcResult, DOMRpcException> invokeRpc(@Nonnull final SchemaPath type,
                                                                  @Nullable final NormalizedNode<?, ?> input) {
        /*
            MessageTransformer是用于将控制层传入的yang normalized node对象 翻译为 对应netconf RPC数据结构的NetconfMessage对象。
            用于将控制层传入的yang NormalizedNode对象 翻译为 对应netconf RPC数据结构的NetconfMessage
         */
        final NetconfMessage message = transformer.toRpcRequest(type, input);
        // 已经换好为对应的NetconfMessage
        final ListenableFuture<RpcResult<NetconfMessage>> delegateFutureWithPureResult =
                listener.sendRequest(message, type.getLastComponent());

        final ListenableFuture<DOMRpcResult> transformed =
            Futures.transform(delegateFutureWithPureResult, input1 -> {
                if (input1.isSuccessful()) {
                    return transformer.toRpcResult(input1.getResult(), type);
                } else {
                    return new DefaultDOMRpcResult(input1.getErrors());
                }
            }, MoreExecutors.directExecutor());

        return Futures.makeChecked(transformed, new Function<Exception, DOMRpcException>() {
            @Nullable
            @Override
            public DOMRpcException apply(@Nullable final Exception exception) {
                return new DOMRpcImplementationNotAvailableException(exception, "Unable to invoke rpc %s", type);
            }
        });
    }

    @Nonnull
    @Override
    public <T extends DOMRpcAvailabilityListener> ListenerRegistration<T> registerRpcListener(
            @Nonnull final T listener) {

        listener.onRpcAvailable(availableRpcs);

        return new ListenerRegistration<T>() {
            @Override
            public void close() {
                // NOOP, no rpcs appear and disappear in this implementation
            }

            @Override
            public T getInstance() {
                return listener;
            }
        };
    }
}
