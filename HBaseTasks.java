import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import  org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.FilterList.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.nio.ByteBuffer;
import java.io.IOException;

public class HBaseTasks {

    /**
     * The private IP address of HBase master node.
     */
    private static String zkAddr = "172.31.21.122";
    /**
     * The name of your HBase table.
     */
    private static TableName tableName = TableName.valueOf("songdata");
    /**
     * HTable handler.
     */
    private static Table songsTable;
    /**
     * HBase connection.
     */
    private static Connection conn;
    /**
     * Byte representation of column family.
     */
    private static byte[] bColFamily = Bytes.toBytes("data");
    /**
     * Logger.
     */
    private static final Logger LOGGER = Logger.getRootLogger();

    /**
     * Initialize HBase connection.
     * @throws IOException
     */
    private static void initializeConnection() throws IOException {
        // Remember to set correct log level to avoid unnecessary output.
        LOGGER.setLevel(Level.ERROR);
        if (!zkAddr.matches("\\d+.\\d+.\\d+.\\d+")) {
            System.out.print("Malformed HBase IP address");
            System.exit(-1);
        }
       
				Configuration conf = HBaseConfiguration.create();
			//	System.out.println("hello");
        conf.set("hbase.master", zkAddr + ":16000");
        conf.set("hbase.zookeeper.quorum", zkAddr);
        conf.set("hbase.zookeeper.property.clientport", "2181");
        conn = ConnectionFactory.createConnection(conf);
      //  System.out.println("lol");
				songsTable = conn.getTable(tableName);
    }

    /**
     * Clean up resources.
     * @throws IOException
     */
    private static void cleanup() throws IOException {
        if (songsTable != null) {
            songsTable.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    /**
     * You should complete the missing parts in the following method. Feel free to add helper functions if necessary.
     *
     * For all questions, output your answer in ONE single line, i.e. use System.out.print().
     *
     * @param args The arguments for main method.
     */
    public static void main(String[] args) throws IOException {
        initializeConnection();
        switch (args[0]) {
        case "demo":
            demo();
            break;
        case "q12":
            demo();
            break;
        case "q13":
            demo1();
            break;
        case "q14":
            demo2();
            break;
        case "q15":
            demo3();
            break;
        case "q16":
            demo4();
        }
        cleanup();
    }

    /**
     * This is a demo of how to use HBase Java API. It will print all the artist_names starting with "The Beatles".
     * @throws IOException
     */
    private static void demo() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol);
        RegexStringComparator comp = new RegexStringComparator("^Total.*");
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        comp = new RegexStringComparator("Water");
				SingleColumnValueFilter filter2 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
				FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
				list.addFilter(filter1);
				list.addFilter(filter2);
				scan.setFilter(list);
        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol)));
        }
        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();
    }
    private static void demo1() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol);
        RegexStringComparator comp = new RegexStringComparator("^Apologies.*|^Confessions.*");
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        comp = new RegexStringComparator("Kanye West");
        bCol = Bytes.toBytes("artist_name");
        scan.addColumn(bColFamily, bCol);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filter1);
        list.addFilter(filter2);
        scan.setFilter(list);
        ResultScanner rs = songsTable.getScanner(scan);
        bCol = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol)));
        }
        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();
    }
    private static void demo2() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("artist_name");
        scan.addColumn(bColFamily, bCol);
        RegexStringComparator comp = new RegexStringComparator("Bob Marley");
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        
        BinaryPrefixComparator comp1 = new BinaryPrefixComparator("400".getBytes());
        bCol = Bytes.toBytes("duration");
        scan.addColumn(bColFamily, bCol);
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.GREATER, comp1);
        
        
        comp1 = new BinaryPrefixComparator("1999".getBytes());
        bCol = Bytes.toBytes("year");
        scan.addColumn(bColFamily, bCol);
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.GREATER, comp1);
        
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filter1);
        list.addFilter(filter2);
        list.addFilter(filter3);
        scan.setFilter(list);
        bCol = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol);

        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol)));
        }
        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();
    }
    private static void demo3() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("artist_name");
        scan.addColumn(bColFamily, bCol);
        RegexStringComparator comp = new RegexStringComparator("Consequence");
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        
        bCol = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol);
        comp = new RegexStringComparator("Family");
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        
        BinaryPrefixComparator comp1 = new BinaryPrefixComparator("0".getBytes());
        bCol = Bytes.toBytes("artist_hotttnesss");
        scan.addColumn(bColFamily, bCol);
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.GREATER, comp1);
        
        
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filter1);
        list.addFilter(filter2);
        list.addFilter(filter3);
        scan.setFilter(list);
        bCol = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol);

        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        for (Result r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol)));
        }
        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();
    }
    private static void demo4() throws IOException {
        Scan scan = new Scan();
        byte[] bCol = Bytes.toBytes("artist_name");
        scan.addColumn(bColFamily, bCol);
        RegexStringComparator comp = new RegexStringComparator("Gwen Guthrie");
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        
        bCol = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol);
        comp = new RegexStringComparator("Love");
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp);
        
        BinaryPrefixComparator comp1 = new BinaryPrefixComparator("1990".getBytes());
        bCol = Bytes.toBytes("year");
        scan.addColumn(bColFamily, bCol);
        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(bColFamily, bCol, CompareFilter.CompareOp.EQUAL, comp1);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(filter1);
        list.addFilter(filter2);
        list.addFilter(filter3);
        scan.setFilter(list);
        bCol = Bytes.toBytes("title");
        scan.addColumn(bColFamily, bCol);

        ResultScanner rs = songsTable.getScanner(scan);
        int count = 0;
        Result r=rs.next();
        for (r = rs.next(); r != null; r = rs.next()) {
            count++;
            System.out.println(Bytes.toString(r.getValue(bColFamily, bCol)));
            break;
        }
        System.out.println("Scan finished. " + count + " match(es) found.");
        rs.close();
    }
  

    /**
     * Question 12.
     *
     * What was that song whose name started with "Total" and ended with "Water"?
     * Write an HBase query that finds the track that the person is looking for.
     * The title starts with "Total" and ends with "Water", both are case sensitive.
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q12() {

    }

    /**
     * Question 13.
     *
     * I don't remember the exact title, it was that song by "Kanye West", and the
     * title started with either "Apologies" or "Confessions". Not sure which...
     * Write an HBase query that finds the track that the person is looking for.
     * The artist_name contains "Kanye West", and the title starts with either
     * "Apologies" or "Confessions" (Case sensitive).
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q13() {

    }

    /**
     * Question 14.
     *
     * There was that new track by "Bob Marley" that was really long. Do you know?
     * Write an HBase query that finds the track the person is looking for.
     * The artist_name has a prefix of "Bob Marley", duration no less than 400,
     * and year 2000 and onwards (Case sensitive).
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q14() {

    }

    /**
     * Question 15.
     *
     * I heard a really great song about "Family" by this really cute singer,
     * I think his name was "Consequence" or something...
     * Write an HBase query that finds the track the person is looking for.
     * The track has an artist_hotttnesss of at least 1, and the artist_name
     * contains "Consequence". Also, the title contains "Family" (Case sensitive).
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q15() {

    }

    /**
     * Question 16.
     *
     * Hey what was that "Love" song that "Gwen Guthrie" came out with in 1990?
     * No, no, it wasn't the sad one, nothing "Bitter" or "Never"...
     * Write an HBase query that finds the track the person is looking for.
     * The track has an artist_name prefix of "Gwen Guthrie", the title contains "Love"
     * but does NOT contain "Bitter" or "Never", the year equals to 1990.
     * Print the track title(s) in a single line.
     *
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q16() {
    }

}

