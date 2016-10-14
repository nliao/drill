package org.apache.drill.exec.server;

import io.netty.channel.socket.SocketChannel;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.DrillbitStartupException;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.proto.beans.QueryType;
import org.apache.drill.exec.proto.beans.RunQuery;
import org.apache.drill.exec.rpc.user.UserServer;
import org.apache.drill.exec.work.WorkManager;
import org.apache.drill.exec.work.foreman.Foreman;
import org.apache.drill.exec.work.user.UserWorker;

import java.util.Properties;

import static org.apache.drill.exec.proto.UserProtos.QueryResultsMode.STREAM_FULL;
import static org.apache.drill.exec.proto.UserProtos.RunQuery.newBuilder;

/**
 * @author ningliao
 * Singleton class for query processing. A wrapper of Drillbit.
 * It will create a foreman for each query request.
 */
public class SlimDrillbit implements AutoCloseable{

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Drillbit.class);
    private Drillbit drillbit;

    private static final Properties TEST_CONFIGURATIONS = new Properties() {
        {
            put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
            put(ExecConstants.HTTP_ENABLE, "false");
        }
    };

    public SlimDrillbit() throws DrillbitStartupException {
        drillbit = start(DrillConfig.create(TEST_CONFIGURATIONS), RemoteServiceSet.getLocalServiceSet());
    }

    public static Drillbit start(final DrillConfig config, final RemoteServiceSet remoteServiceSet)
            throws DrillbitStartupException {
        logger.debug("Starting new Drillbit.");
        // TODO: allow passing as a parameter
        ScanResult classpathScan = ClassPathScanner.fromPrescan(config);
        Drillbit bit;
        try {
            bit = new Drillbit(config, remoteServiceSet, classpathScan);
        } catch (final Exception ex) {
            throw new DrillbitStartupException("Failure while initializing values in Drillbit.", ex);
        }

        try {
            bit.run();
        } catch (final Exception e) {
            logger.error("Failure during initial startup of Drillbit.", e);
            bit.close();
            throw new DrillbitStartupException("Failure during initial startup of Drillbit.", e);
        }
        logger.debug("Started new Drillbit.");
        return bit;
    }

    public UserBitShared.QueryId runQuery(SocketChannel channel, String sql) {
        UserProtos.RunQuery query = newBuilder().setType(UserBitShared.QueryType.SQL)
                .setPlan(sql)
                .setResultsMode(STREAM_FULL).build();

        UserServer.UserClientConnection connection = this.drillbit.getServiceEngine().getUserServer().initRemoteConnection(channel);

        return this.drillbit.runQuery(connection, query);
    }

    @Override public void close() throws Exception {
        drillbit.close();
    }
}
