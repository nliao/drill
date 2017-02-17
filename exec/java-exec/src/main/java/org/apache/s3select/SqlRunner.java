package org.apache.s3select;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.config.LogicalPlanPersistence;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.scanner.ClassPathScanner;
import org.apache.drill.common.scanner.persistence.ScanResult;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.QueryContext;
import org.apache.drill.exec.ops.UdfUtilities;
import org.apache.drill.exec.physical.PhysicalPlan;
import org.apache.drill.exec.physical.base.FragmentRoot;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.planner.PhysicalPlanReader;
import org.apache.drill.exec.planner.fragment.Fragment;
import org.apache.drill.exec.planner.fragment.MakeFragmentsVisitor;
import org.apache.drill.exec.planner.fragment.SimpleParallelizer;
import org.apache.drill.exec.planner.sql.QueryInputException;
import org.apache.drill.exec.planner.sql.SqlConverter;
import org.apache.drill.exec.planner.sql.handlers.AbstractSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.DefaultSqlHandler;
import org.apache.drill.exec.planner.sql.handlers.ExplainHandler;
import org.apache.drill.exec.planner.sql.handlers.SqlHandlerConfig;
import org.apache.drill.exec.proto.BitControl;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.rpc.control.ControlTunnel;
import org.apache.drill.exec.rpc.user.UserSession;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.work.QueryWorkUnit;
import org.apache.drill.exec.work.batch.IncomingBuffers;
import org.apache.drill.exec.work.foreman.ForemanSetupException;
import org.apache.drill.exec.work.foreman.SqlUnsupportedException;
import org.apache.drill.exec.work.fragment.FragmentExecutor;
import org.apache.drill.exec.work.fragment.FragmentStatusReporter;
import org.apache.drill.exec.work.fragment.RootFragmentManager;
import org.apache.hadoop.security.AccessControlException;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author ningliao
 */
public class SqlRunner {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SqlRunner.class);
    private final QueryContext queryContext;
    private final SqlConverter parser;
    private final static Executor executor = Executors.newCachedThreadPool();
    private final UserBitShared.QueryId queryId;
    private final String sql;
    private final UserSession userSession;
    private final PhysicalPlanReader physicalPlanReader;

    private static final Properties TEST_CONFIGURATIONS = new Properties() {
        {
            put(ExecConstants.SYS_STORE_PROVIDER_LOCAL_ENABLE_WRITE, "false");
            put(ExecConstants.HTTP_ENABLE, "false");
        }
    };

    public SqlRunner(String sql) {
        userSession = UserSession.Builder.newBuilder()
                .withCredentials(UserBitShared.UserCredentials.newBuilder().setUserName("foo").build())
                .build();
        queryId = queryIdGenerator();
        queryContext = new QueryContext(userSession, null, queryId);

        parser = new SqlConverter(
                queryContext.getPlannerSettings(),
                queryContext.getNewDefaultSchema(),
                queryContext.getDrillOperatorTable(),
                (UdfUtilities) queryContext,
                queryContext.getFunctionRegistry());

        this.sql = sql;

        DrillConfig config = DrillConfig.create(TEST_CONFIGURATIONS);
        ScanResult classpathScan = ClassPathScanner.fromPrescan(config);

        LogicalPlanPersistence lpPersistence = new LogicalPlanPersistence(config, classpathScan);

        StoragePluginRegistry storagePlugins = config.getInstance(StoragePluginRegistry.STORAGE_PLUGIN_REGISTRY_IMPL, StoragePluginRegistry.class, this);

        this.physicalPlanReader = new PhysicalPlanReader(config, classpathScan, lpPersistence,
                CoordinationProtos.DrillbitEndpoint.getDefaultInstance(), storagePlugins);
    }

    public void runSql() throws ExecutionSetupException {

        SqlNode sqlNode = parse(sql);

        PhysicalPlan physicalPlan = getPhysicalPlan(sqlNode);

        runPhysicalPlan(physicalPlan);
    }

    private void runPhysicalPlan(PhysicalPlan physicalPlan) throws ExecutionSetupException {
        final QueryWorkUnit work = getQueryWorkUnit(physicalPlan);
        // TODO: Does a S3Select query only have 1 root fragment?
        // final List<BitControl.PlanFragment> planFragments = work.getFragments();
        final BitControl.PlanFragment rootPlanFragment = work.getRootFragment();
        assert queryId == rootPlanFragment.getHandle().getQueryId();

        runRootFragment(work.getRootFragment(), work.getRootOperator());
    }

    private QueryWorkUnit getQueryWorkUnit(PhysicalPlan physicalPlan) throws ExecutionSetupException {
        final PhysicalOperator rootOperator = physicalPlan.getSortedOperators(false).iterator().next();
        final Fragment rootFragment = rootOperator.accept(MakeFragmentsVisitor.INSTANCE, null);
        final SimpleParallelizer parallelizer = new SimpleParallelizer(queryContext);
        final QueryWorkUnit queryWorkUnit = parallelizer.getFragments(
                queryContext.getOptions().getOptionList(), queryContext.getCurrentEndpoint(),
                queryId, queryContext.getActiveEndpoints(), this.physicalPlanReader, rootFragment,
                userSession, queryContext.getQueryContextInfo());

        return queryWorkUnit;
    }

    /**
     * Set up the root fragment (which will run locally), and submit it for execution.
     *
     * @param rootFragment
     * @param rootOperator
     * @throws ExecutionSetupException
     */
    private void runRootFragment(final BitControl.PlanFragment rootFragment, final FragmentRoot rootOperator)
            throws ExecutionSetupException {
//        @SuppressWarnings("resource")
//        final FragmentContext rootContext = new FragmentContext(drillbitContext, rootFragment, queryContext,
//                initiatingClient, drillbitContext.getFunctionImplementationRegistry());
//        @SuppressWarnings("resource")
//        final IncomingBuffers buffers = new IncomingBuffers(rootFragment, rootContext);
//        rootContext.setBuffers(buffers);
//
//        final ControlTunnel tunnel = drillbitContext.getController().getTunnel(queryContext.getCurrentEndpoint());
//        final FragmentExecutor rootRunner = new FragmentExecutor(rootContext, rootFragment,
//                new FragmentStatusReporter(rootContext, tunnel),
//                rootOperator);
//        final RootFragmentManager fragmentManager = new RootFragmentManager(rootFragment.getHandle(), buffers, rootRunner);
//
//        if (buffers.isDone()) {
//            // if we don't have to wait for any incoming data, start the fragment runner.
//            bee.addFragmentRunner(fragmentManager.getRunnable());
//        } else {
//            // if we do, record the fragment manager in the workBus.
//            drillbitContext.getWorkBus().addFragmentManager(fragmentManager);
//        }
    }

    private SqlNode parse(String sql) {

        SqlNode sqlNode = parser.parse(sql);
        return sqlNode;
    }

    private PhysicalPlan getPhysicalPlan(SqlNode sqlNode) throws QueryInputException {

        final SqlHandlerConfig config = new SqlHandlerConfig(queryContext, parser);

        AbstractSqlHandler handler;
        switch(sqlNode.getKind()){
        case EXPLAIN:
            handler = new ExplainHandler(config, null);
            break;
        default:
            handler = new DefaultSqlHandler(config, null);
        }

        try {
            return handler.getPlan(sqlNode);
        } catch(ValidationException e) {
            String errorMessage = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            throw UserException.validationError(e)
                    .message(errorMessage)
                    .build(logger);
        } catch (AccessControlException e) {
            throw UserException.permissionError(e)
                    .build(logger);
        } catch(SqlUnsupportedException e) {
            throw UserException.unsupportedError(e)
                    .build(logger);
        } catch (IOException | RelConversionException e) {
            throw new QueryInputException("Failure handling SQL.", e);
        } catch (ForemanSetupException e) {
            throw new RuntimeException("foreman setup exception.");
        }
    }

    /**
     * Helper method to generate QueryId
     * @return generated QueryId
     */
    private static UserBitShared.QueryId queryIdGenerator() {
        ThreadLocalRandom r = ThreadLocalRandom.current();

        // create a new queryid where the first four bytes are a growing time (each new value comes earlier in sequence).  Last 12 bytes are random.
        final long time = (int) (System.currentTimeMillis()/1000);
        final long p1 = ((Integer.MAX_VALUE - time) << 32) + r.nextInt();
        final long p2 = r.nextLong();
        final UserBitShared.QueryId id = UserBitShared.QueryId.newBuilder().setPart1(p1).setPart2(p2).build();
        return id;
    }
}
