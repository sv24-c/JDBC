package BatchProcessing;

import org.gjt.mm.mysql.Driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by smit on 7/3/22.
 */
public class BatchUsingStatement
{
    public static void main(String[] args) throws ClassNotFoundException, SQLException
    {

        Class.forName("com.mysql.jdbc.Driver");

        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/mysqldb?characterEncoding=latin1","root","Mind@123");

        con.setAutoCommit(false);

        Statement statement = con.createStatement();

        statement.addBatch("insert into emp values(111, 'abc',12,'jnd')");

        statement.executeBatch();

        con.commit();

        con.close();

    }
}
