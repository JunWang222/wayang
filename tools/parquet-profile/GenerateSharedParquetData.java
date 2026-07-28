/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class GenerateSharedParquetData {

    private static final Schema ORDERS_SCHEMA = new Schema.Parser().parse(
            "{"
                    + "\"type\":\"record\","
                    + "\"name\":\"orders\","
                    + "\"fields\":["
                    + "{\"name\":\"order_id\",\"type\":\"int\"},"
                    + "{\"name\":\"customer_id\",\"type\":\"int\"},"
                    + "{\"name\":\"region\",\"type\":\"string\"},"
                    + "{\"name\":\"amount\",\"type\":\"double\"},"
                    + "{\"name\":\"bucket\",\"type\":\"int\"}"
                    + "]"
                    + "}"
    );

    private static final Schema CUSTOMERS_SCHEMA = new Schema.Parser().parse(
            "{"
                    + "\"type\":\"record\","
                    + "\"name\":\"customers\","
                    + "\"fields\":["
                    + "{\"name\":\"cust_id\",\"type\":\"int\"},"
                    + "{\"name\":\"tier\",\"type\":\"string\"}"
                    + "]"
                    + "}"
    );

    private GenerateSharedParquetData() {
    }

    public static void main(String[] args) throws Exception {
        String outputDir = args.length == 0 ? "target/shared-parquet-profile" : args[0];
        Files.createDirectories(Paths.get(outputDir));

        String ordersPath = outputDir + "/orders.parquet";
        String customersPath = outputDir + "/customers.parquet";
        writeOrders(ordersPath);
        writeCustomers(customersPath);

        System.out.println("orders_uri=" + toFileUri(Paths.get(ordersPath)));
        System.out.println("customers_uri=" + toFileUri(Paths.get(customersPath)));
        System.out.println("orders_rows=12");
        System.out.println("customers_rows=5");
        System.out.println("expected_join_rows=12");
    }

    private static void writeOrders(String outputPath) throws Exception {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new LocalOutputFile(Paths.get(outputPath)))
                .withSchema(ORDERS_SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            writeOrder(writer, 1001, 1, "north", 42.5, 0);
            writeOrder(writer, 1002, 2, "south", 18.0, 0);
            writeOrder(writer, 1003, 3, "west", 23.5, 1);
            writeOrder(writer, 1004, 1, "north", 99.0, 1);
            writeOrder(writer, 1005, 4, "east", 14.0, 2);
            writeOrder(writer, 1006, 5, "south", 67.2, 2);
            writeOrder(writer, 1007, 2, "south", 31.8, 3);
            writeOrder(writer, 1008, 3, "west", 44.4, 3);
            writeOrder(writer, 1009, 4, "east", 52.1, 4);
            writeOrder(writer, 1010, 5, "north", 73.3, 4);
            writeOrder(writer, 1011, 1, "north", 11.2, 5);
            writeOrder(writer, 1012, 2, "south", 87.6, 5);
        }
    }

    private static void writeCustomers(String outputPath) throws Exception {
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new LocalOutputFile(Paths.get(outputPath)))
                .withSchema(CUSTOMERS_SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build()) {
            writeCustomer(writer, 1, "gold");
            writeCustomer(writer, 2, "silver");
            writeCustomer(writer, 3, "bronze");
            writeCustomer(writer, 4, "gold");
            writeCustomer(writer, 5, "silver");
        }
    }

    private static void writeOrder(ParquetWriter<GenericRecord> writer,
                                   int orderId,
                                   int customerId,
                                   String region,
                                   double amount,
                                   int bucket) throws Exception {
        GenericRecord record = new GenericData.Record(ORDERS_SCHEMA);
        record.put("order_id", orderId);
        record.put("customer_id", customerId);
        record.put("region", region);
        record.put("amount", amount);
        record.put("bucket", bucket);
        writer.write(record);
    }

    private static void writeCustomer(ParquetWriter<GenericRecord> writer,
                                      int customerId,
                                      String tier) throws Exception {
        GenericRecord record = new GenericData.Record(CUSTOMERS_SCHEMA);
        record.put("cust_id", customerId);
        record.put("tier", tier);
        writer.write(record);
    }

    private static String toFileUri(Path path) {
        return "file:///" + path.toAbsolutePath().toString().replace('\\', '/');
    }
}
