-- For compression
set hive.exec.compress.output=true;
set io.seqfile.compression.type=BLOCK;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set mapred.job.queue.name=dev;
-- Set for dynamic partition
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.sortmerge.join.bigtable.selection.policy=org.apache.hadoop.hive.ql.optimizer.TableSizeBasedBigTableSelectorForAutoSMJ;
use ${hivevar:db};
