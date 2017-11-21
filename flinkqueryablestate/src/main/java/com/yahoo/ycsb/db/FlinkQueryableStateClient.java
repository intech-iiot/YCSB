package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.ByteArrayByteIterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Executors;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils.AddressResolution;
import org.apache.flink.runtime.query.QueryableStateClient;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class FlinkQueryableStateClient extends DB {
  public static final String JOBMANAGER_RPC_ADDRESS = "flink.jobmanager.rpc.address";
  public static final String JOBMANAGER_RPC_PORT = "flink.jobmanager.rpc.port";
  public static final String JOB_ID = "flink.job-id";
  public static final String REQUEST_WAIT_MILLIS = "flink.queryablestate.request.max.wait.ms";

  private static final TypeSerializer<String> KEY_SER =
      TypeInformation.of(new TypeHint<String>() {}).createSerializer(new ExecutionConfig());

  private QueryableStateClient flinkQueryableStateCl;
  private JobID jobId;
  private String requestMaxMs;

  public void init() throws DBException {
    Properties props = getProperties();
    Configuration flinkConfig = new Configuration();
    flinkConfig.setString(JobManagerOptions.ADDRESS, props.getProperty(JOBMANAGER_RPC_ADDRESS));
    String portString = props.getProperty(JOBMANAGER_RPC_PORT);
    int port;
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = JobManagerOptions.PORT.defaultValue();
    }
    requestMaxMs = props.getProperty(REQUEST_WAIT_MILLIS) + " millis";
    flinkConfig.setInteger(JobManagerOptions.PORT, port);
    jobId = JobID.fromHexString(props.getProperty(JOB_ID));

    try {
      HighAvailabilityServices haServices =
          HighAvailabilityServicesUtils.createHighAvailabilityServices(
              flinkConfig,
              Executors.newSingleThreadExecutor(),
              AddressResolution.TRY_ADDRESS_RESOLUTION);
      flinkQueryableStateCl = new QueryableStateClient(flinkConfig, haServices);
    } catch (Exception e) {
      throw new DBException("Failed to create Flink HA services", e);
    }
  }

  public void cleanup() throws DBException {
    flinkQueryableStateCl.shutDown();
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    int keyHashCode = key.hashCode();
    try {
      byte[] serializedKey =
          KvStateRequestSerializer.serializeKeyAndNamespace(
              key.toString(),
              KEY_SER,
              VoidNamespace.INSTANCE,
              VoidNamespaceSerializer.INSTANCE);
      byte[] bytes = Await.result(
          flinkQueryableStateCl.getKvState(jobId, table, keyHashCode, serializedKey),
          Duration.apply(requestMaxMs));
      result.put(key, new ByteArrayByteIterator(bytes));
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    throw new UnsupportedOperationException("Flink Queryable state does not support scanning.");
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    throw new UnsupportedOperationException("Flink Queryable state does not support updating.");
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    throw new UnsupportedOperationException("Flink Queryable state does not support inserting.");
  }

  @Override
  public Status delete(String table, String key) {
    throw new UnsupportedOperationException("Flink Queryable state does not support deleting.");
  }
}
