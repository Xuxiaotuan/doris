// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.PartitionType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.RoutineLoadJob;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.LoadTaskInfo;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TPipelineInstanceParams;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TScanRangeParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

// Used to generate a plan fragment for a streaming load.
// we only support OlapTable now.
// TODO(zc): support other type table
public class StreamLoadPlanner {
    private static final Logger LOG = LogManager.getLogger(StreamLoadPlanner.class);

    // destination Db and table get from request
    // Data will load to this table
    private Database db;
    private OlapTable destTable;
    private LoadTaskInfo taskInfo;

    private Analyzer analyzer;
    private DescriptorTable descTable;

    private ScanNode scanNode;
    private TupleDescriptor tupleDesc;

    public StreamLoadPlanner(Database db, OlapTable destTable, LoadTaskInfo taskInfo) {
        this.db = db;
        this.destTable = destTable;
        this.taskInfo = taskInfo;
    }

    private void resetAnalyzer() {
        analyzer = new Analyzer(Env.getCurrentEnv(), null);
        // TODO(cmy): currently we do not support UDF in stream load command.
        // Because there is no way to check the privilege of accessing UDF..
        analyzer.setUDFAllowed(Config.enable_udf_in_load);
        descTable = analyzer.getDescTbl();
    }

    // can only be called after "plan()", or it will return null
    public OlapTable getDestTable() {
        return destTable;
    }

    // the caller should get table read lock when call this method
    public TPipelineFragmentParams plan(TUniqueId loadId) throws UserException {
        return this.plan(loadId, 1);
    }

    // the caller should get table read lock when call this method
    // create the plan. the plan's query id and load id are same, using the parameter 'loadId'
    public TPipelineFragmentParams plan(TUniqueId loadId, int fragmentInstanceIdIndex) throws UserException {
        if (destTable.getKeysType() != KeysType.UNIQUE_KEYS
                && taskInfo.getMergeType() != LoadTask.MergeType.APPEND) {
            throw new AnalysisException("load by MERGE or DELETE is only supported in unique tables.");
        }
        if (taskInfo.getMergeType() != LoadTask.MergeType.APPEND
                && !destTable.hasDeleteSign()) {
            throw new AnalysisException("load by MERGE or DELETE need to upgrade table to support batch delete.");
        }
        TUniqueKeyUpdateMode uniquekeyUpdateMode = taskInfo.getUniqueKeyUpdateMode();
        if (uniquekeyUpdateMode != TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS
                && destTable.hasSequenceCol() && !taskInfo.hasSequenceCol() && destTable.getSequenceMapCol() == null) {
            throw new UserException("Table " + destTable.getName()
                    + " has sequence column, need to specify the sequence column");
        }
        if (!destTable.hasSequenceCol() && taskInfo.hasSequenceCol()) {
            throw new UserException("There is no sequence column in the table " + destTable.getName());
        }
        resetAnalyzer();
        // construct tuple descriptor, used for dataSink
        tupleDesc = descTable.createTupleDescriptor("DstTableTuple");
        TupleDescriptor scanTupleDesc = tupleDesc;
        // note: we use two tuples separately for Scan and Sink here to avoid wrong nullable info.
        // construct tuple descriptor, used for scanNode
        scanTupleDesc = descTable.createTupleDescriptor("ScanTuple");
        boolean negative = taskInfo.getNegative();
        // get partial update related info
        if (uniquekeyUpdateMode != TUniqueKeyUpdateMode.UPSERT && !destTable.getEnableUniqueKeyMergeOnWrite()) {
            throw new UserException("Only unique key merge on write support partial update");
        }

        // try to convert to upsert if only has missing auto-increment key column
        boolean hasMissingColExceptAutoIncKey = false;
        if (taskInfo.getColumnExprDescs().descs.isEmpty()
                && uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS) {
            uniquekeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;
        }

        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS
                || uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS) {
            boolean hasSyncMaterializedView = destTable.getFullSchema().stream()
                    .anyMatch(col -> col.isMaterializedViewColumn());
            if (hasSyncMaterializedView) {
                throw new DdlException("Can't do partial update on merge-on-write Unique table"
                        + " with sync materialized view.");
            }
            if (destTable.isUniqKeyMergeOnWriteWithClusterKeys()) {
                throw new UserException("Can't do partial update on merge-on-write Unique table with cluster keys");
            }
        }

        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS && !destTable.hasSkipBitmapColumn()) {
            String tblName = destTable.getName();
            throw new UserException("Flexible partial update can only support table with skip bitmap hidden column."
                    + " But table " + tblName + " doesn't have it. You can use `ALTER TABLE " + tblName
                            + " ENABLE FEATURE \"UPDATE_FLEXIBLE_COLUMNS\";` to add it to the table.");
        }
        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS
                && !destTable.getEnableLightSchemaChange()) {
            throw new UserException("Flexible partial update can only support table with light_schema_change enabled."
                    + " But table " + destTable.getName() + "'s property light_schema_change is false");
        }
        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FLEXIBLE_COLUMNS
                && destTable.hasVariantColumns()) {
            throw new UserException("Flexible partial update can only support table without variant columns.");
        }
        HashSet<String> partialUpdateInputColumns = new HashSet<>();
        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS) {
            for (Column col : destTable.getFullSchema()) {
                boolean existInExpr = false;
                if (col.hasOnUpdateDefaultValue()) {
                    partialUpdateInputColumns.add(col.getName());
                }
                for (ImportColumnDesc importColumnDesc : taskInfo.getColumnExprDescs().descs) {
                    if (importColumnDesc.getColumnName() != null
                            && importColumnDesc.getColumnName().equals(col.getName())) {
                        if (!col.isVisible() && !Column.DELETE_SIGN.equals(col.getName())) {
                            throw new UserException("Partial update should not include invisible column except"
                                    + " delete sign column: " + col.getName());
                        }
                        partialUpdateInputColumns.add(col.getName());
                        if (destTable.hasSequenceCol() && (taskInfo.hasSequenceCol() || (
                                destTable.getSequenceMapCol() != null
                                        && destTable.getSequenceMapCol().equalsIgnoreCase(col.getName())))) {
                            partialUpdateInputColumns.add(Column.SEQUENCE_COL);
                        }
                        existInExpr = true;
                        break;
                    }
                }
                if (!existInExpr) {
                    if (col.isKey() && !col.isAutoInc()) {
                        throw new UserException("Partial update should include all key columns, missing: "
                                + col.getName());
                    }
                    if (!(col.isKey() && col.isAutoInc()) && col.isVisible()) {
                        hasMissingColExceptAutoIncKey = true;
                    }
                }

                if (!col.getGeneratedColumnsThatReferToThis().isEmpty()
                        && col.getGeneratedColumnInfo() == null && !existInExpr) {
                    throw new UserException("Partial update should include"
                            + " all ordinary columns referenced"
                            + " by generated columns, missing: " + col.getName());
                }
            }
            if (taskInfo.getMergeType() == LoadTask.MergeType.DELETE
                    || taskInfo.getMergeType() == LoadTask.MergeType.MERGE) {
                partialUpdateInputColumns.add(Column.DELETE_SIGN);
            }
        }
        if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS && !hasMissingColExceptAutoIncKey) {
            uniquekeyUpdateMode = TUniqueKeyUpdateMode.UPSERT;
        }
        // here we should be full schema to fill the descriptor table
        for (Column col : destTable.getFullSchema()) {
            if (uniquekeyUpdateMode == TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS
                    && !partialUpdateInputColumns.contains(col.getName())) {
                continue;
            }
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
            slotDesc.setAutoInc(col.isAutoInc());
            SlotDescriptor scanSlotDesc = descTable.addSlotDescriptor(scanTupleDesc);
            scanSlotDesc.setIsMaterialized(true);
            scanSlotDesc.setColumn(col);
            scanSlotDesc.setIsNullable(col.isAllowNull());
            scanSlotDesc.setAutoInc(col.isAutoInc());
            if (col.isAutoInc()) {
                // auto-increment column should be non-nullable
                // however, here we use `NullLiteral` to indicate that a cell should
                // be filled with generated value in `VOlapTableSink::_fill_auto_inc_cols()`
                scanSlotDesc.setIsNullable(true);
            }
            for (ImportColumnDesc importColumnDesc : taskInfo.getColumnExprDescs().descs) {
                try {
                    if (!importColumnDesc.isColumn() && importColumnDesc.getColumnName() != null
                            && importColumnDesc.getColumnName().equals(col.getName())) {
                        scanSlotDesc.setIsNullable(importColumnDesc.getExpr().isNullable());
                        break;
                    }
                } catch (Exception e) {
                    // An exception may be thrown here because the `importColumnDesc.getExpr()` is not analyzed now.
                    // We just skip this case here.
                }
            }
            if (negative && !col.isKey() && col.getAggregationType() != AggregateType.SUM) {
                throw new DdlException("Column is not SUM AggregateType. column:" + col.getName());
            }
        }

        scanTupleDesc.setTable(destTable);
        analyzer.registerTupleDescriptor(scanTupleDesc);
        Expr whereExpr = null;
        if (null != taskInfo.getWhereExpr()) {
            whereExpr = taskInfo.getWhereExpr().clone();
            whereExpr.analyze(analyzer);
        }
        // create scan node
        FileLoadScanNode fileScanNode = new FileLoadScanNode(new PlanNodeId(0), scanTupleDesc);
        // 1. create file group
        DataDescription dataDescription = new DataDescription(destTable.getName(), taskInfo);
        dataDescription.analyzeWithoutCheckPriv(db.getFullName());
        BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription);
        fileGroup.setWhereExpr(whereExpr);
        fileGroup.parse(db, dataDescription);
        // 2. create dummy file status
        TBrokerFileStatus fileStatus = new TBrokerFileStatus();
        if (taskInfo.getFileType() == TFileType.FILE_LOCAL) {
            fileStatus.setPath(taskInfo.getPath());
            fileStatus.setIsDir(false);
            fileStatus.setSize(taskInfo.getFileSize()); // must set to -1, means stream.
        } else {
            fileStatus.setPath("");
            fileStatus.setIsDir(false);
            fileStatus.setSize(-1); // must set to -1, means stream.
        }
        // The load id will pass to csv reader to find the stream load context from new load stream manager
        fileScanNode.setLoadInfo(loadId, taskInfo.getTxnId(), destTable, BrokerDesc.createForStreamLoad(),
                fileGroup, fileStatus, taskInfo.isStrictMode(), taskInfo.getFileType(), taskInfo.getHiddenColumns(),
                uniquekeyUpdateMode, destTable.getSequenceMapCol());
        scanNode = fileScanNode;

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        descTable.computeStatAndMemLayout();

        int timeout = taskInfo.getTimeout();
        final boolean enableMemtableOnSinkNode =
                destTable.getTableProperty().getUseSchemaLightChange()
                ? taskInfo.isMemtableOnSinkNode() : false;
        final boolean enableSingleReplicaLoad = enableMemtableOnSinkNode
                ? false : Config.enable_single_replica_load;
        // create dest sink
        List<Long> partitionIds = getAllPartitionIds();
        OlapTableSink olapTableSink;
        if (taskInfo instanceof StreamLoadTask && ((StreamLoadTask) taskInfo).getGroupCommit() != null) {
            olapTableSink = new GroupCommitBlockSink(destTable, tupleDesc, partitionIds,
                    enableSingleReplicaLoad, ((StreamLoadTask) taskInfo).getGroupCommit(),
                    taskInfo.getMaxFilterRatio());
        } else {
            olapTableSink = new OlapTableSink(destTable, tupleDesc, partitionIds, enableSingleReplicaLoad);
        }
        int txnTimeout = timeout == 0 ? ConnectContext.get().getExecTimeout() : timeout;
        olapTableSink.init(loadId, taskInfo.getTxnId(), db.getId(), timeout, taskInfo.getSendBatchParallelism(),
                taskInfo.isLoadToSingleTablet(), taskInfo.isStrictMode(), txnTimeout);
        olapTableSink.setPartialUpdateInfo(uniquekeyUpdateMode, partialUpdateInputColumns);
        olapTableSink.complete(analyzer);

        // for stream load, we only need one fragment, ScanNode -> DataSink.
        // OlapTableSink can dispatch data to corresponding node.
        PlanFragment fragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);
        fragment.setSink(olapTableSink);

        fragment.finalize(null);

        TPipelineFragmentParams params = new TPipelineFragmentParams();
        params.setProtocolVersion(PaloInternalServiceVersion.V1);
        params.setFragment(fragment.toThrift());

        params.setDescTbl(analyzer.getDescTbl().toThrift());
        params.setCoord(new TNetworkAddress(FrontendOptions.getLocalHostAddress(), Config.rpc_port));
        params.setCurrentConnectFe(new TNetworkAddress(FrontendOptions.getLocalHostAddress(), Config.rpc_port));

        TPipelineInstanceParams execParams = new TPipelineInstanceParams();
        // user load id (streamLoadTask.id) as query id
        params.setQueryId(loadId);
        execParams.setFragmentInstanceId(new TUniqueId(loadId.hi, loadId.lo + fragmentInstanceIdIndex));
        params.per_exch_num_senders = Maps.newHashMap();
        params.destinations = Lists.newArrayList();
        Map<Integer, List<TScanRangeParams>> perNodeScanRange = Maps.newHashMap();
        List<TScanRangeParams> scanRangeParams = Lists.newArrayList();
        for (TScanRangeLocations locations : scanNode.getScanRangeLocations(0)) {
            scanRangeParams.add(new TScanRangeParams(locations.getScanRange()));
        }
        // For stream load, only one sender
        execParams.setSenderId(0);
        params.setNumSenders(1);
        perNodeScanRange.put(scanNode.getId().asInt(), scanRangeParams);
        execParams.setPerNodeScanRanges(perNodeScanRange);
        params.addToLocalParams(execParams);
        params.setLoadStreamPerNode(taskInfo.getStreamPerNode());
        params.setTotalLoadStreams(taskInfo.getStreamPerNode());
        params.setNumLocalSink(1);
        TQueryOptions queryOptions = new TQueryOptions();
        queryOptions.setQueryType(TQueryType.LOAD);
        queryOptions.setQueryTimeout(timeout);
        queryOptions.setExecutionTimeout(timeout);
        if (timeout < 1) {
            LOG.info("try set timeout less than 1", new RuntimeException(""));
        }
        queryOptions.setMemLimit(taskInfo.getMemLimit());
        // for stream load, we use exec_mem_limit to limit the memory usage of load channel.
        queryOptions.setLoadMemLimit(taskInfo.getMemLimit());
        // load
        queryOptions.setBeExecVersion(Config.be_exec_version);
        queryOptions.setIsReportSuccess(taskInfo.getEnableProfile());
        queryOptions.setEnableProfile(taskInfo.getEnableProfile());
        queryOptions.setEnableMemtableOnSinkNode(enableMemtableOnSinkNode);
        params.setQueryOptions(queryOptions);
        TQueryGlobals queryGlobals = new TQueryGlobals();
        queryGlobals.setNowString(TimeUtils.getDatetimeFormatWithTimeZone().format(LocalDateTime.now()));
        queryGlobals.setTimestampMs(System.currentTimeMillis());
        queryGlobals.setTimeZone(taskInfo.getTimezone());
        if (taskInfo instanceof RoutineLoadJob) {
            queryGlobals.setLoadZeroTolerance(false);
        } else {
            queryGlobals.setLoadZeroTolerance(taskInfo.getMaxFilterRatio() <= 0.0);
        }
        queryGlobals.setNanoSeconds(LocalDateTime.now().getNano());

        params.setQueryGlobals(queryGlobals);
        params.setTableName(destTable.getName());
        params.setIsMowTable(destTable.getEnableUniqueKeyMergeOnWrite());
        // LOG.debug("stream load txn id: {}, plan: {}", streamLoadTask.getTxnId(), params);
        return params;
    }

    // get all specified partition ids.
    // if no partition specified, return null
    private List<Long> getAllPartitionIds() throws DdlException, AnalysisException {
        List<Long> partitionIds = Lists.newArrayList();

        PartitionNames partitionNames = taskInfo.getPartitions();
        if (partitionNames != null) {
            for (String partName : partitionNames.getPartitionNames()) {
                Partition part = destTable.getPartition(partName, partitionNames.isTemp());
                if (part == null) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_PARTITION, partName, destTable.getName());
                }
                partitionIds.add(part.getId());
            }
            return partitionIds;
        }
        List<Expr> conjuncts = scanNode.getConjuncts();
        if (destTable.getPartitionInfo().getType() != PartitionType.UNPARTITIONED && !conjuncts.isEmpty()) {
            PartitionInfo partitionInfo = destTable.getPartitionInfo();
            Map<Long, PartitionItem> itemById = partitionInfo.getIdToItem(false);
            Map<String, ColumnRange> columnNameToRange = Maps.newHashMap();
            for (Column column : partitionInfo.getPartitionColumns()) {
                SlotDescriptor slotDesc = tupleDesc.getColumnSlot(column.getName());
                if (null == slotDesc) {
                    continue;
                }
                ColumnRange columnRange = ScanNode.createColumnRange(slotDesc, conjuncts, partitionInfo);
                if (columnRange != null) {
                    columnNameToRange.put(column.getName(), columnRange);
                }
            }
            if (columnNameToRange.isEmpty()) {
                return null;
            }

            PartitionPruner partitionPruner = null;
            if (destTable.getPartitionInfo().getType() == PartitionType.RANGE) {
                partitionPruner = new RangePartitionPrunerV2(itemById,
                        partitionInfo.getPartitionColumns(), columnNameToRange);
            } else if (destTable.getPartitionInfo().getType() == PartitionType.LIST) {
                partitionPruner = new ListPartitionPrunerV2(itemById,
                        partitionInfo.getPartitionColumns(), columnNameToRange);
            }
            partitionIds.addAll(partitionPruner.prune());
            return partitionIds;
        }
        return null;
    }

    public DescriptorTable getDescTable() {
        return descTable;
    }
}
