/*
 * Copyright (c) 2014 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */
package org.opendaylight.netconf.sal.connect.netconf.sal;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import java.util.List;
import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.dom.api.DOMDataBroker;
import org.opendaylight.controller.md.sal.dom.api.DOMMountPointService;
import org.opendaylight.controller.md.sal.dom.api.DOMNotification;
import org.opendaylight.controller.md.sal.dom.api.DOMRpcService;
import org.opendaylight.netconf.sal.connect.api.RemoteDeviceHandler;
import org.opendaylight.netconf.sal.connect.netconf.listener.NetconfDeviceCapabilities;
import org.opendaylight.netconf.sal.connect.netconf.listener.NetconfSessionPreferences;
import org.opendaylight.netconf.sal.connect.util.RemoteDeviceId;
import org.opendaylight.yangtools.yang.model.api.SchemaContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 个人理解：整个Sal包中的入口核心类！
 *
 * SalFacade，如其名，作为Facade封装操作！
 */
public final class NetconfDeviceSalFacade implements AutoCloseable, RemoteDeviceHandler<NetconfSessionPreferences> {

    private static final Logger LOG = LoggerFactory.getLogger(NetconfDeviceSalFacade.class);

    private final RemoteDeviceId id;
    private final NetconfDeviceSalProvider salProvider;
    private final List<AutoCloseable> salRegistrations = Lists.newArrayList();

    public NetconfDeviceSalFacade(final RemoteDeviceId id, final DOMMountPointService mountPointService,
                                  final DataBroker dataBroker) {
        this.id = id;
        // 传入了mountPointService
        this.salProvider = new NetconfDeviceSalProvider(id, mountPointService, dataBroker);
    }

    @VisibleForTesting
    NetconfDeviceSalFacade(final RemoteDeviceId id, final NetconfDeviceSalProvider salProvider) {
        this.id = id;
        this.salProvider = salProvider;
    }

    @Override
    public synchronized void onNotification(final DOMNotification domNotification) {
        salProvider.getMountInstance().publish(domNotification);
    }

    /**
     * 当与底层建立连接后(NetconfDevice.java中)，最终回调此方法
     */
    @Override
    public synchronized void onDeviceConnected(final SchemaContext schemaContext,
                                               final NetconfSessionPreferences netconfSessionPreferences,
                                               final DOMRpcService deviceRpc) {

        final DOMDataBroker domBroker =
                new NetconfDeviceDataBroker(id, schemaContext, deviceRpc, netconfSessionPreferences);

        final NetconfDeviceNotificationService notificationService = new NetconfDeviceNotificationService();

        // 最终是调用NetconfDeviceSalProvider的内部类MountInstace的方法
        // 效果是：更新operational topology-netconf yang中node的连接状态以及capabilities
        salProvider.getMountInstance()
                .onTopologyDeviceConnected(schemaContext, domBroker, deviceRpc, notificationService);
        // 最终是调用NetconfDeviceTopologyAdapter的updateDeviceData方法
        // 效果是：在DOMMountPointService注册当前device节点的DOMMountPoint，DOMMountPoint中传入了DOMDataBroker/DOMRpcService/DOMNotifactionService
        salProvider.getTopologyDatastoreAdapter()
                .updateDeviceData(true, netconfSessionPreferences.getNetconfDeviceCapabilities());
    }

    @Override
    public synchronized void onDeviceDisconnected() {
        salProvider.getTopologyDatastoreAdapter().updateDeviceData(false, new NetconfDeviceCapabilities());
        salProvider.getMountInstance().onTopologyDeviceDisconnected();
    }

    @Override
    public synchronized void onDeviceFailed(final Throwable throwable) {
        salProvider.getTopologyDatastoreAdapter().setDeviceAsFailed(throwable);
        salProvider.getMountInstance().onTopologyDeviceDisconnected();
    }

    @Override
    public synchronized void close() {
        for (final AutoCloseable reg : Lists.reverse(salRegistrations)) {
            closeGracefully(reg);
        }
        closeGracefully(salProvider);
    }

    @SuppressWarnings("checkstyle:IllegalCatch")
    private void closeGracefully(final AutoCloseable resource) {
        if (resource != null) {
            try {
                resource.close();
            } catch (final Exception e) {
                LOG.warn("{}: Ignoring exception while closing {}", id, resource, e);
            }
        }
    }

}
