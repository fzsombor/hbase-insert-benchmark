package com.cloudera;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;


public class HBaseInsertBenchmark {

    private static final byte[] CF_NAME = Bytes.toBytes("test_cf");

    public static void createTable(final Admin admin, TableName table, boolean compression) throws IOException {
        System.out.println(admin.tableExists(table));
        if (!admin.tableExists(table)) {

            HTableDescriptor tableDescriptor = new HTableDescriptor(table);
            HColumnDescriptor cd = new HColumnDescriptor(CF_NAME)
                    .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
                    .setMaxVersions(1);
            if (compression) {
                System.out.println("Using compression in the columns");
                cd.setCompressionType(Compression.Algorithm.GZ);
            }
            tableDescriptor.addFamily(cd);
            admin.createTable(tableDescriptor);
            System.out.println("Table created");
        } else {
            System.out.println("Table already exists. Exiting...");
            System.exit(1);
        }

    }

    public static void deleteTable(final Admin admin, TableName table) throws IOException {
        admin.disableTable(table);
        admin.deleteTable(table);
        System.out.println("Table deleted");

    }

    public static void putBatch(final Table table, byte[][] row, int batch) throws IOException {

        String[] columns = {"F.DATE", "F.INSTRUMENTID", "F.EXCHANGEORDERID", "F.MESSAGETIMESTAMP", "F.SEQUENCENUMBER", "F.MESSAGEINDEX", "F.ACTIVITYTYPE", "F.OWNINGBUID", "F.OFIVER", "F.ITS", "F.DATA"};
        List<Put> putList = new ArrayList<>();

        for (int i = 0; i < batch; i++) {
            Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
            for (int j = 0; j < 11; j++) {
                put.addColumn(CF_NAME, Bytes.toBytes(columns[j]), row[j]).setDurability(Durability.SKIP_WAL);
            }

            putList.add(put);


        }
        long startTime = System.currentTimeMillis();
        table.put(putList);
        long endTime = System.currentTimeMillis();
        System.out.println("Raw HBase insertion time is " + (endTime - startTime) + " ms. The rest is time spent in Java transforms/methods.");
    }

    public static byte[] generateByteField(int size) {

        byte[] b = new byte[size];
        new Random().nextBytes(b);
        return b;
    }

    public static byte[][] generateByteRow(int size) {
//        PKKEY VARBINARY PRIMARY KEY,
//        F.DATE UNSIGNED_LONG,
//        F.INSTRUMENTID UNSIGNED_LONG,
//        F.EXCHANGEORDERID UNSIGNED_LONG,
//        F.MESSAGETIMESTAMP UNSIGNED_LONG,
//        F.SEQUENCENUMBER UNSIGNED_LONG,
//        F.MESSAGEINDEX UNSIGNED_INT,
//        F.ACTIVITYTYPE UNSIGNED_INT,
//        F.OWNINGBUID UNSIGNED_INT,
//        F.OFIVER VARCHAR,
//        F.DATA VARBINARY,
//        F.ITS UNSIGNED_LONG
        byte[][] row = new byte[11][];
        for (int i = 0; i < 9; i++) {
            row[i] = generateByteField(255);
        }
        row[10] = generateByteField(size * 1024);
        return row;
    }

    public static CommandLine initArgs(String[] args) {
        Options options = new Options();

        Option table = new Option("t", "table", true, "HBase table name");
        table.setRequired(true);
        options.addOption(table);

        Option batch = new Option("b", "batch", true, "Number of rows to insert together");
        batch.setRequired(true);
        options.addOption(batch);

        Option size = new Option("s", "size", true, "Size of binary field in KB");
        size.setRequired(true);
        options.addOption(size);

        Option number = new Option("n", "number", true, "Number of batches to insert");
        number.setRequired(true);
        options.addOption(number);

        Option keytab = new Option("kt", "keytab", true, "Location of kerberos keytab");
        keytab.setRequired(true);
        options.addOption(keytab);

        Option principal = new Option("p", "principal", true, "Principal in kerberos keytab");
        principal.setRequired(true);
        options.addOption(principal);

        Option delete = new Option("d", "delete", true, "Delete table after the test. Just pass any value. CAUTION!");
        delete.setRequired(false);
        options.addOption(delete);

        Option compression = new Option("c", "compression", true, "Set any value to enable compression");
        compression.setRequired(false);
        options.addOption(compression);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;   //not a good practice, it serves it purpose

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("HBase Insert Benchmark", "Set the HBASE_CONF_DIR environment variable to the location of hbase-site.xml and core-site.xml.", options, "Try again");
            System.exit(1);
        }
        return cmd;
    }

    public static void main(String[] args) throws IOException {
        CommandLine cmd = initArgs(args);

        TableName tableName = TableName.valueOf(cmd.getOptionValue("table"));

        int size = Integer.parseInt(cmd.getOptionValue("size"));
        int batch = Integer.parseInt(cmd.getOptionValue("batch"));
        int number = Integer.parseInt(cmd.getOptionValue("number"));


        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "core-site.xml"));
        System.out.println("Using config DIR for HBase: " + System.getenv("HBASE_CONF_DIR"));
        config.set("hadoop.security.authentication", "Kerberos");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        UserGroupInformation.setConfiguration(config);
        System.out.println("Kiniting with " + cmd.getOptionValue("principal") + " in " + cmd.getOptionValue("keytab"));
        UserGroupInformation.loginUserFromKeytab(cmd.getOptionValue("principal"), cmd.getOptionValue("keytab"));


        System.out.println("Generating row data");
        byte[][] row = generateByteRow(size);
        System.out.println("Connecting to HBase");
        try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {
            System.out.println("Creating table: " + tableName.getNameAsString());
            HBaseAdmin.available(config);
            createTable(admin, tableName, cmd.hasOption("compression"));
            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            System.out.println("Inserting " + batch * number + " rows with " + size + "KB data");
            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            long totalDuration = 0;
            try (Table table = connection.getTable(tableName)) {
                for (int i = 0; i < number; i++) {
                    long startTime = System.currentTimeMillis();
                    putBatch(table, row, batch);
                    long endTime = System.currentTimeMillis();
                    long duration = (endTime - startTime);
                    System.out.println("Inserted " + batch + " rows in " + duration + " ms.");
                    totalDuration += duration;
                }
            }
            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            System.out.println("Inserted " + batch * number + " rows in " + totalDuration + " ms in total");
            System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
            if (cmd.hasOption("delete")) {
                deleteTable(admin, tableName);
            }
        }
    }
}