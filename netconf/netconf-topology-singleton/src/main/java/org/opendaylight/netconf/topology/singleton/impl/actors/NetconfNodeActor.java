/*
 * Copyright (c) 2016 Cisco Systems, Inc. and others. All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package org.opendaylight.netconf.topology.singleton.impl.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Status.Failure;
import akka.actor.Status.Success;
import akka.pattern.AskTimeoutException;
import akka.util.Timeout;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.eclipse.jdt.annotation.NonNull;
import org.opendaylight.controller.cluster.common.actor.AbstractUntypedActor;
import org.opendaylight.controller.cluster.schema.provider.RemoteYangTextSourceProvider;
import org.opendaylight.controller.cluster.schema.provider.impl.RemoteSchemaProvider;
import org.opendaylight.controller.cluster.schema.provider.impl.YangTextSchemaSourceSerializationProxy;
import org.opendaylight.controller.md.sal.dom.api.DOMDataBroker;
import org.opendaylight.controller.md.sal.dom.api.DOMDataReadOnlyTransaction;
import org.opendaylight.controller.md.sal.dom.api.DOMDataReadWriteTransaction;
import org.opendaylight.controller.md.sal.dom.api.DOMDataWriteTransaction;
import org.opendaylight.controller.md.sal.dom.api.DOMMountPointService;
import org.opendaylight.controller.md.sal.dom.api.DOMRpcException;
import org.opendaylight.controller.md.sal.dom.api.DOMRpcResult;
import org.opendaylight.controller.md.sal.dom.api.DOMRpcService;
import org.opendaylight.netconf.sal.connect.util.RemoteDeviceId;
import org.opendaylight.netconf.topology.singleton.impl.ProxyDOMRpcService;
import org.opendaylight.netconf.topology.singleton.impl.ProxyYangTextSourceProvider;
import org.opendaylight.netconf.topology.singleton.impl.SlaveSalFacade;
import org.opendaylight.netconf.topology.singleton.impl.utils.NetconfTopologySetup;
import org.opendaylight.netconf.topology.singleton.messages.AskForMasterMountPoint;
import org.opendaylight.netconf.topology.singleton.messages.CreateInitialMasterActorData;
import org.opendaylight.netconf.topology.singleton.messages.MasterActorDataInitialized;
import org.opendaylight.netconf.topology.singleton.messages.NormalizedNodeMessage;
import org.opendaylight.netconf.topology.singleton.messages.NotMasterException;
import org.opendaylight.netconf.topology.singleton.messages.RefreshSetupMasterActorData;
import org.opendaylight.netconf.topology.singleton.messages.RefreshSlaveActor;
import org.opendaylight.netconf.topology.singleton.messages.RegisterMountPoint;
import org.opendaylight.netconf.topology.singleton.messages.UnregisterSlaveMountPoint;
import org.opendaylight.netconf.topology.singleton.messages.YangTextSchemaSourceRequest;
import org.opendaylight.netconf.topology.singleton.messages.rpc.InvokeRpcMessage;
import org.opendaylight.netconf.topology.singleton.messages.rpc.InvokeRpcMessageReply;
import org.opendaylight.netconf.topology.singleton.messages.transactions.EmptyResultResponse;
import org.opendaylight.netconf.topology.singleton.messages.transactions.NewReadTransactionReply;
import org.opendaylight.netconf.topology.singleton.messages.transactions.NewReadTransactionRequest;
import org.opendaylight.netconf.topology.singleton.messages.transactions.NewReadWriteTransactionReply;
import org.opendaylight.netconf.topology.singleton.messages.transactions.NewReadWriteTransactionRequest;
import org.opendaylight.netconf.topology.singleton.messages.transactions.NewWriteTransactionReply;
import org.opendaylight.netconf.topology.singleton.messages.transactions.NewWriteTransactionRequest;
import org.opendaylight.yangtools.yang.data.api.YangInstanceIdentifier;
import org.opendaylight.yangtools.yang.model.api.SchemaContext;
import org.opendaylight.yangtools.yang.model.api.SchemaPath;
import org.opendaylight.yangtools.yang.model.repo.api.SchemaContextFactory;
import org.opendaylight.yangtools.yang.model.repo.api.SchemaRepository;
import org.opendaylight.yangtools.yang.model.repo.api.SchemaSourceFilter;
import org.opendaylight.yangtools.yang.model.repo.api.SourceIdentifier;
import org.opendaylight.yangtools.yang.model.repo.api.YangTextSchemaSource;
import org.opendaylight.yangtools.yang.model.repo.spi.PotentialSchemaSource;
import org.opendaylight.yangtools.yang.model.repo.spi.SchemaSourceRegistration;
import org.opendaylight.yangtools.yang.model.repo.spi.SchemaSourceRegistry;
import scala.concurrent.duration.Duration;

public class NetconfNodeActor extends AbstractUntypedActor {

    private final Duration writeTxIdleTimeout;
    private final DOMMountPointService mountPointService;

    private SchemaSourceRegistry schemaRegistry;
    private SchemaRepository schemaRepository;
    private Timeout actorResponseWaitTime;
    private RemoteDeviceId id;
    private NetconfTopologySetup setup;
    private List<SourceIdentifier> sourceIdentifiers;
    private DOMRpcService deviceRpc;
    private SlaveSalFacade slaveSalManager;
    private DOMDataBroker deviceDataBroker;
    //readTxActor can be shared
    private ActorRef readTxActor;
    private List<SchemaSourceRegistration<YangTextSchemaSource>> registeredSchemas;

    public static Props props(final NetconfTopologySetup setup,
                              final RemoteDeviceId id, final SchemaSourceRegistry schemaRegistry,
                              final SchemaRepository schemaRepository, final Timeout actorResponseWaitTime,
                              final DOMMountPointService mountPointService) {
        return Props.create(NetconfNodeActor.class, () ->
                new NetconfNodeActor(setup, id, schemaRegistry, schemaRepository, actorResponseWaitTime,
                        mountPointService));
    }

    protected NetconfNodeActor(final NetconfTopologySetup setup,
                               final RemoteDeviceId id, final SchemaSourceRegistry schemaRegistry,
                               final SchemaRepository schemaRepository, final Timeout actorResponseWaitTime,
                               final DOMMountPointService mountPointService) {
        this.setup = setup;
        this.id = id;
        this.schemaRegistry = schemaRegistry;
        this.schemaRepository = schemaRepository;
        this.actorResponseWaitTime = actorResponseWaitTime;
        this.writeTxIdleTimeout = setup.getIdleTimeout();
        this.mountPointService = mountPointService;
    }

    @SuppressWarnings("checkstyle:IllegalCatch")
    @Override
    public void handleReceive(final Object message) throws Exception {
        LOG.debug("{}:  received message {}", id, message);

        if (message instanceof CreateInitialMasterActorData) { // master

            final CreateInitialMasterActorData masterActorData = (CreateInitialMasterActorData) message;
            sourceIdentifiers = masterActorData.getSourceIndentifiers();
            this.deviceDataBroker = masterActorData.getDeviceDataBroker();
            final DOMDataReadOnlyTransaction tx = deviceDataBroker.newReadOnlyTransaction();
            readTxActor = context().actorOf(ReadTransactionActor.props(tx));
            this.deviceRpc = masterActorData.getDeviceRpc();

            sender().tell(new MasterActorDataInitialized(), self());

            LOG.debug("{}: Master is ready.", id);

        } else if (message instanceof  RefreshSetupMasterActorData) {
            setup = ((RefreshSetupMasterActorData) message).getNetconfTopologyDeviceSetup();
            id = ((RefreshSetupMasterActorData) message).getRemoteDeviceId();
            sender().tell(new MasterActorDataInitialized(), self());
        } else if (message instanceof AskForMasterMountPoint) { // master
            /*
                在NetconfNodeManager.handleSlaveMountPoint中，当设备连上控制器时，发出消息给master节点，只有master节点会收到此消息并处理.
             */

            AskForMasterMountPoint askForMasterMountPoint = (AskForMasterMountPoint)message;

            // only master contains reference to deviceDataBroker
            if (deviceDataBroker != null) {
                LOG.debug("{}: Sending RegisterMountPoint reply to {}", id, askForMasterMountPoint.getSlaveActorRef());
                // master节点获取slaveActorRef并回复RegisterMountPoint消息，告诉其当前device支持的netconf rpc等信息（sourceIdentifiers是从SchemaContext转换过来的）
                askForMasterMountPoint.getSlaveActorRef().tell(new RegisterMountPoint(sourceIdentifiers, self()),
                        sender());
            } else {
                LOG.warn("{}: Received {} but we don't appear to be the master", id, askForMasterMountPoint);
                sender().tell(new Failure(new NotMasterException(self())), self());
            }

        } else if (message instanceof YangTextSchemaSourceRequest) { // master

            final YangTextSchemaSourceRequest yangTextSchemaSourceRequest = (YangTextSchemaSourceRequest) message;
            sendYangTextSchemaSourceProxy(yangTextSchemaSourceRequest.getSourceIdentifier(), sender());

        } else if (message instanceof NewReadTransactionRequest) { // master

            sender().tell(new NewReadTransactionReply(readTxActor), self());

        } else if (message instanceof NewWriteTransactionRequest) { // master
            try {
                final DOMDataWriteTransaction tx = deviceDataBroker.newWriteOnlyTransaction();
                final ActorRef txActor = context().actorOf(WriteTransactionActor.props(tx, writeTxIdleTimeout));
                sender().tell(new NewWriteTransactionReply(txActor), self());
            } catch (final Exception t) {
                sender().tell(t, self());
            }

        } else if (message instanceof NewReadWriteTransactionRequest) {
            try {
                final DOMDataReadWriteTransaction tx = deviceDataBroker.newReadWriteTransaction();
                final ActorRef txActor = context().actorOf(ReadWriteTransactionActor.props(tx, writeTxIdleTimeout));
                sender().tell(new NewReadWriteTransactionReply(txActor), self());
            } catch (final Exception t) {
                sender().tell(t, self());
            }
        } else if (message instanceof InvokeRpcMessage) { // master
            /*
             * 在ProxyDOMRpcService中发送该消息。
             * 在slave控制器上如果对device请求rpc，获取的是 ProxyDOMRpcService对象，其效果是构造InvokeRpcMessage发送给master处理
             */

            final InvokeRpcMessage invokeRpcMessage = (InvokeRpcMessage) message;
            // 处理slave控制器调用device的rpc
            invokeSlaveRpc(invokeRpcMessage.getSchemaPath(), invokeRpcMessage.getNormalizedNodeMessage(), sender());

        } else if (message instanceof RegisterMountPoint) { //slaves
            /*
             * 在NetconfNodeManager.handleSlaveMountPoint中，当设备连上控制器时，slave发出AskForMasterMountPoint消息给master节点，
             * master节点回复RegisterMountPoint消息（告知device支持的netconf rpc）。 可参考上面AskForMasterMountPoint的处理过程。
             */

            RegisterMountPoint registerMountPoint = (RegisterMountPoint)message;
            // device支持的netconf rpc（sourceIdentifiers是从SchemaContext转换过来的）
            sourceIdentifiers = registerMountPoint.getSourceIndentifiers();
            // 创建SlaveSalFacade并注册SlaveMountPoint等（proxy类）
            registerSlaveMountPoint(registerMountPoint.getMasterActorRef());
            sender().tell(new Success(null), self());
        } else if (message instanceof UnregisterSlaveMountPoint) { //slaves
            unregisterSlaveMountPoint();
        } else if (message instanceof RefreshSlaveActor) { //slave
            actorResponseWaitTime = ((RefreshSlaveActor) message).getActorResponseWaitTime();
            id = ((RefreshSlaveActor) message).getId();
            schemaRegistry = ((RefreshSlaveActor) message).getSchemaRegistry();
            setup = ((RefreshSlaveActor) message).getSetup();
            schemaRepository = ((RefreshSlaveActor) message).getSchemaRepository();
        }

    }

    @Override
    public void postStop() throws Exception {
        try {
            super.postStop();
        } finally {
            unregisterSlaveMountPoint();
        }
    }

    private void unregisterSlaveMountPoint() {
        if (slaveSalManager != null) {
            slaveSalManager.close();
            slaveSalManager = null;
        }

        closeSchemaSourceRegistrations();
    }

    private void sendYangTextSchemaSourceProxy(final SourceIdentifier sourceIdentifier, final ActorRef sender) {
        final ListenableFuture<@NonNull YangTextSchemaSource> schemaSourceFuture =
                schemaRepository.getSchemaSource(sourceIdentifier, YangTextSchemaSource.class);

        Futures.addCallback(schemaSourceFuture, new FutureCallback<YangTextSchemaSource>() {
            @Override
            public void onSuccess(final YangTextSchemaSource yangTextSchemaSource) {
                try {
                    sender.tell(new YangTextSchemaSourceSerializationProxy(yangTextSchemaSource), getSelf());
                } catch (IOException e) {
                    sender.tell(new Failure(e), getSelf());
                }
            }

            @Override
            public void onFailure(@Nonnull final Throwable throwable) {
                sender.tell(new Failure(throwable), getSelf());
            }
        }, MoreExecutors.directExecutor());
    }

    private void invokeSlaveRpc(final SchemaPath schemaPath, final NormalizedNodeMessage normalizedNodeMessage,
                                final ActorRef recipient) {

        LOG.debug("{}: invokeSlaveRpc for {}, input: {} on rpc service {}", id, schemaPath, normalizedNodeMessage,
                deviceRpc);

        final CheckedFuture<DOMRpcResult, DOMRpcException> rpcResult = deviceRpc.invokeRpc(schemaPath,
                normalizedNodeMessage != null ? normalizedNodeMessage.getNode() : null);

        Futures.addCallback(rpcResult, new FutureCallback<DOMRpcResult>() {
            @Override
            public void onSuccess(@Nullable final DOMRpcResult domRpcResult) {
                LOG.debug("{}: invokeSlaveRpc for {}, domRpcResult: {}", id, schemaPath, domRpcResult);

                if (domRpcResult == null) {
                    // 回复slave控制器节点
                    recipient.tell(new EmptyResultResponse(), getSender());
                    return;
                }
                NormalizedNodeMessage nodeMessageReply = null;
                if (domRpcResult.getResult() != null) {
                    nodeMessageReply = new NormalizedNodeMessage(YangInstanceIdentifier.EMPTY,
                            domRpcResult.getResult());
                }
                // 回复slave控制器节点
                recipient.tell(new InvokeRpcMessageReply(nodeMessageReply, domRpcResult.getErrors()), getSelf());
            }

            @Override
            public void onFailure(@Nonnull final Throwable throwable) {
                recipient.tell(new Failure(throwable), getSelf());
            }
        }, MoreExecutors.directExecutor());
    }

    private void registerSlaveMountPoint(final ActorRef masterReference) {
        unregisterSlaveMountPoint();

        // 创建SlaveSalFacade
        slaveSalManager = new SlaveSalFacade(id, setup.getActorSystem(), actorResponseWaitTime, mountPointService);

        /*
         * 干了几个事：
         * 1.创建SchemaContextFactory
         * 2.将device的sourceIdentifiers反解析为SchemaContext(netconf rpc能力)
         * 3.创建ProxyDOMRpcService对象（代理master节点的DOMRpcService，其效果是走akka让master请求底层device）
         * 4.调用SlaveSalFacade.registerSlaveMountPoint注册device的本地MountPoint（各种Proxy类，与master节点有区别）到本地DOMMountPointService
         */
        resolveSchemaContext(createSchemaContextFactory(masterReference), slaveSalManager, masterReference, 1);
    }

    private DOMRpcService getDOMRpcService(final ActorRef masterReference) {
        return new ProxyDOMRpcService(setup.getActorSystem(), masterReference, id, actorResponseWaitTime);
    }

    private SchemaContextFactory createSchemaContextFactory(final ActorRef masterReference) {
        final RemoteYangTextSourceProvider remoteYangTextSourceProvider =
                new ProxyYangTextSourceProvider(masterReference, getContext().dispatcher(), actorResponseWaitTime);
        final RemoteSchemaProvider remoteProvider = new RemoteSchemaProvider(remoteYangTextSourceProvider,
                getContext().dispatcher());

        registeredSchemas = sourceIdentifiers.stream()
                .map(sourceId ->
                        schemaRegistry.registerSchemaSource(remoteProvider, PotentialSchemaSource.create(sourceId,
                                YangTextSchemaSource.class, PotentialSchemaSource.Costs.REMOTE_IO.getValue())))
                .collect(Collectors.toList());

        return schemaRepository.createSchemaContextFactory(SchemaSourceFilter.ALWAYS_ACCEPT);
    }

    private void resolveSchemaContext(final SchemaContextFactory schemaContextFactory,
            final SlaveSalFacade localSlaveSalManager, final ActorRef masterReference, int tries) {
        // 反解析SchemaContext
        final ListenableFuture<SchemaContext> schemaContextFuture =
                schemaContextFactory.createSchemaContext(sourceIdentifiers);
        Futures.addCallback(schemaContextFuture, new FutureCallback<SchemaContext>() {
            @Override
            public void onSuccess(@Nonnull final SchemaContext result) {
                executeInSelf(() -> {
                    // Make sure the slaveSalManager instance hasn't changed since we initiated the schema context
                    // resolution.
                    if (slaveSalManager == localSlaveSalManager) {
                        LOG.info("{}: Schema context resolved: {} - registering slave mount point",
                                id, result.getModules());
                        /*
                            先创建ProxyDOMRpcService对象（代理master节点的DOMRpcService，其效果是走akka让master请求底层device）

                            注册MountPoint到本地的DOMMountPointService，只不过mountPoint包含的对象是ProxyDOMRpcService，ProxyDOMDataBroker等
                         */
                        slaveSalManager.registerSlaveMountPoint(result, getDOMRpcService(masterReference),
                                masterReference);
                    }
                });
            }

            @Override
            public void onFailure(@Nonnull final Throwable throwable) {
                executeInSelf(() -> {
                    if (slaveSalManager == localSlaveSalManager) {
                        final Throwable cause = Throwables.getRootCause(throwable);
                        if (cause instanceof AskTimeoutException) {
                            if (tries <= 5 || tries % 10 == 0) {
                                LOG.warn("{}: Failed to resolve schema context - retrying...", id, throwable);
                            }

                            resolveSchemaContext(schemaContextFactory, localSlaveSalManager,
                                    masterReference, tries + 1);
                        } else {
                            LOG.error("{}: Failed to resolve schema context - unable to register slave mount point",
                                    id, throwable);
                            closeSchemaSourceRegistrations();
                        }
                    }
                });
            }
        }, MoreExecutors.directExecutor());
    }

    private void closeSchemaSourceRegistrations() {
        if (registeredSchemas != null) {
            registeredSchemas.forEach(SchemaSourceRegistration::close);
            registeredSchemas = null;
        }
    }

}
