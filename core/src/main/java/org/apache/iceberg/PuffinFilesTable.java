/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import java.util.List;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

/**
 * A {@link Table} implementation that exposes a table's registered Puffin statistics files as rows.
 *
 * <p>This table reads only Iceberg table metadata, specifically {@link Table#statisticsFiles()}. It
 * does not open Puffin files and does not read Puffin footer or blob payload bytes.
 */
public class PuffinFilesTable extends BaseMetadataTable {

  private static final Schema PUFFIN_FILES_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "snapshot_id", Types.LongType.get()),
          Types.NestedField.required(2, "statistics_path", Types.StringType.get()),
          Types.NestedField.required(3, "file_size_in_bytes", Types.LongType.get()),
          Types.NestedField.required(4, "file_footer_size_in_bytes", Types.LongType.get()),
          Types.NestedField.required(5, "blob_count", Types.IntegerType.get()),
          Types.NestedField.optional(
              6, "blob_types", Types.ListType.ofRequired(7, Types.StringType.get())),
          Types.NestedField.optional(
              8, "field_ids", Types.ListType.ofRequired(9, Types.IntegerType.get())));

  PuffinFilesTable(Table table) {
    this(table, table.name() + ".puffin_files");
  }

  PuffinFilesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new PuffinFilesTableScan(table());
  }

  @Override
  public Schema schema() {
    return PUFFIN_FILES_SCHEMA;
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.PUFFIN_FILES;
  }

  private DataTask task(BaseTableScan scan) {
    String location = table().operations().current().metadataFileLocation();
    return StaticDataTask.of(
        table()
            .io()
            .newInputFile(location != null ? location : scan.snapshot().manifestListLocation()),
        schema(),
        scan.schema(),
        table().statisticsFiles(),
        PuffinFilesTable::statisticsFileToRow);
  }

  private class PuffinFilesTableScan extends StaticTableScan {

    PuffinFilesTableScan(Table table) {
      super(
          table, PUFFIN_FILES_SCHEMA, MetadataTableType.PUFFIN_FILES, PuffinFilesTable.this::task);
    }

    PuffinFilesTableScan(Table table, TableScanContext context) {
      super(
          table,
          PUFFIN_FILES_SCHEMA,
          MetadataTableType.PUFFIN_FILES,
          PuffinFilesTable.this::task,
          context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new PuffinFilesTableScan(table, context);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      return CloseableIterable.withNoopClose(PuffinFilesTable.this.task(this));
    }
  }

  private static StaticDataTask.Row statisticsFileToRow(StatisticsFile statisticsFile) {
    List<BlobMetadata> blobs = statisticsFile.blobMetadata();

    List<String> blobTypes =
        blobs.stream().map(BlobMetadata::type).distinct().collect(ImmutableList.toImmutableList());

    List<Integer> fieldIds =
        blobs.stream()
            .flatMap(blob -> blob.fields().stream())
            .distinct()
            .collect(ImmutableList.toImmutableList());

    return StaticDataTask.Row.of(
        statisticsFile.snapshotId(),
        statisticsFile.path(),
        statisticsFile.fileSizeInBytes(),
        statisticsFile.fileFooterSizeInBytes(),
        blobs.size(),
        blobTypes,
        fieldIds);
  }
}
