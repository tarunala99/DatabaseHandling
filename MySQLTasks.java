import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.*;
public class MySQLTasks {

    private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DB_NAME = "song_db";
    private static final String URL = "jdbc:mysql://localhost/" + DB_NAME + "?useSSL=false";
    private static final String DB_USER = "default";
    private static final String DB_PWD = "db15319root";

    private static Connection conn;

    /**
     * You should complete the missing parts in the following method. Feel free to add helper functions if necessary.
     *
     * For all questions, output your answer in one single line, i.e. use System.out.print().
     *
     * @param args The arguments for main method.
     */
    public static void main(String[] args) {
        try {
            initializeConnection();
            // This argument should be used to determine the piece(s) of your code to run.
            String runOption = args[0];
						String asd;
            switch (runOption) {
            // Run the demo function.
            case "demo":
            //    demo();
                break;
            // Load data from the csv files into corresponding tables.
            case "load_data":
                loadData();
                break;
            // Answer question 7.
            case "q7":
                asd="SELECT track_id AS cnt FROM songs WHERE duration=(SELECT max(duration) from songs)";
                demo(asd);
                break;
            // Answer question 8.
            case "q8":
                // For q8, there should be an args[1] which is the name (NOT field) of your intended database index.
                q8(args[1]);
                break;
            // Answer question 9.
            case "q9":
                asd="SELECT track_id AS cnt FROM songs WHERE duration=(SELECT max(duration) from songs)";
                demo(asd);
                break;
            // Answer question 10.
            case "q10":
                asd="SELECT count(*) AS cnt FROM songs WHERE title like binary '%The Beatles%' or song_id like binary '%The Beatles%' or songs.release like binary '%The Beatles%' or artist_id like binary '%The Beatles%' or artist_mbid like binary '%The Beatles%' or artist_name like binary '%The Beatles%'";
                demo(asd);
                break;
            // Answer question 11.
            case "q11":
                asd="select min(artist_name) AS cnt from songs where artist_id=(select artist_id from (select artist_id,count(*) from songs group by artist_id order by count(*) desc LIMIT 1 OFFSET 2) AS T)";
                demo(asd);
                break;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Initializes database connection.
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private static void initializeConnection() throws ClassNotFoundException, SQLException {
    try
		{
			BufferedReader a=new BufferedReader(new FileReader("out.txt"));
			String s = a.readLine();
			//System.out.println(s);
			a.close();
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(URL, s, DB_PWD);
		}
		catch(Exception e){System.out.println("error");}
		/* System.out.println(s);
				Class.forName(JDBC_DRIVER);
        conn = DriverManager.getConnection(URL, s, DB_PWD); */
    }

    /**
     * JDBC usage demo. The following function will print the row count of the "songs" table.
     * Table must exists before this function is called.
     */
    private static void demo(String qwe) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            String tableName = "songs";
            
           // System.out.println(e);
						ResultSet rs = stmt.executeQuery(qwe);
						//System.out.println(rs);
             if (rs.next()) {
                String rowCount = rs.getString("cnt");
                System.out.println(rowCount); 
}            
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Load data.
     * 
     * This method should load data from csv files into corresponding tables.
     * Complete this method with your own implementation.
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void loadData() {
    }

    /**
     * Question 7.
     * 
     * This method should execute a SQL query and print the trackid of the song with the maximum duration.
     * If there are multiple answers, simply print any one of them. Do NOT hardcode your answer.
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q7() {
    }

    /**
     * Question 8.
     * 
     * A database index is a data structure that improves the speed of data retrieval.
     * Identify the field that will improve the performance of your query in question 7
     * and create a database index on that field. A custom index name is needed to create an index.
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     *
     * @param indexName The name of your index (this is NOT the field on which your index will be created).
     */
    private static void q8(String indexName) {
    }

    /**
     * Question 9.
     * 
     * This method should execute a SQL query and return the trackid of the song with the maximum duration.
     * If there are multiple answers, simply print any one of them. Do NOT hardcode your answer.
     * 
     * This is the same query as Question 7. Do you see any difference in performance?
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     * 
     * Hint: Please create a private function and reuse it in q7 and q9.
     */
    private static void q9() {
    }

    /**
     * Question 10.
     * 
     * Write the SQL query that returns all matches (across any column), similar to the command grep -P 'The Beatles' | wc -l:
     * Do NOT hardcode your answer.
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q10() {
    }

    /**
     * Question 11.
     * 
     * Which artist has the third-most number of rows in table songs? The output should be the name of the artist.
     * Please use artist_id as the unique identifier of the artist.
     * If there are multiple answers, simply print any one of them. Do NOT hardcode your answer.
     * 
     * You are allowed to make changes such as modifying method name, parameter list and/or return type.
     */
    private static void q11() {
    }
}

