## Quick Start

This section describes how to run YCSB on a Flink cluster with a job using quyeryable state. 

### 1. Start a Flink job with queryable state.

### 2. Install Java and Maven

### 3. Set Up YCSB

Git clone YCSB and compile:

    git clone http://github.com/brianfrankcooper/YCSB.git
    cd YCSB
    mvn -pl com.yahoo.ycsb:flinkqueryablestate-binding -am clean package -Dcheckstyle.skip

### 4. Run read only workload:
    
    ./bin/ycsb run flinkqueryablestate -P flinkqueryablestate.properties -P workload_queryable_state
    
where contents of the *workload_queryable_state* file could be:

    workload=com.yahoo.ycsb.workloads.PreLoadedKeysWorkload
    preloadedkeys=key1,key2,key3
    table=wrap
    recordcount=100
    operationcount=100000
    readallfields=true
    readproportion=1
    updateproportion=0
    scanproportion=0
    insertproportion=0
    requestdistribution=uniform
    
and the contents of the *flinkqueryablestate.properties* file could be:

    flink.jobmanager.rpc.address=localhost
    flink.job-id=8e343929097b7a0e7620c68dfb0e1baf
    flink.queryablestate.request.max.wait.ms=200

Note: only read-only workloads are supported at the moment because we cannot write to Flink 1.3.2's
queryable state cache directly.
    