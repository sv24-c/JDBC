package BatchProcessing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.logging.Level;

/**
 * Created by smit on 7/3/22.
 */
public class BatchUsingPreparedStatement
{
    static
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
    }

    static Connection makeConnection()
    {
        Connection con = null;
        try
        {
            if (con == null )
            {
                con = DriverManager.getConnection("jdbc:mysql://localhost:3306/mysqldb?characterEncoding=latin1", "root", "Mind@123");
            }

            System.out.println("Connection Established...");
        }

        catch (SQLException e)
        {
            e.printStackTrace();
        }

        return con;
    }

    static void closeConnection(PreparedStatement preparedStatement, Connection connection)
    {

        try
        {

            if (preparedStatement !=null || !preparedStatement.isClosed())
            {
                preparedStatement.close();
            }

            if (connection != null || !preparedStatement.isClosed())
            {
                connection.close();

                System.out.println("Connection Closed..");
            }
        }

        catch (SQLException e)
        {
            e.printStackTrace();
        }

    }

    private static void insertinTable()
    {
        Connection con = null;

        PreparedStatement preparedStatement = null;

        try
        {
            con = makeConnection();

            con.setAutoCommit(false);

            preparedStatement = con.prepareStatement("insert into emp values(?,?,?,?)");

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));


            while (true)
            {
                System.out.println("Enter id");
                String string =reader.readLine();
                int id =Integer.parseInt(string);

                System.out.println("Enter name");
                String name = reader.readLine();

                System.out.println("Enter age");
                String string1 =reader.readLine();
                int age =Integer.parseInt(string1);

                System.out.println("Enter address");
                String address = reader.readLine();

                preparedStatement.setInt(1, id);
                preparedStatement.setString(2, name);
                preparedStatement.setInt(3, age);
                preparedStatement.setString(4, address);

                preparedStatement.addBatch();

                System.out.println(id+" "+name+" "+age+" "+address);

                System.out.println("You want to add more record: y/n");

                String ans = reader.readLine();

                if (ans.equals("n"))
                {
                    break;
                }

            }

            preparedStatement.executeBatch();

            System.out.println("Record saved Successfully.");

            con.commit();
        }

        catch (Exception e)
        {
            System.out.println(e.getStackTrace());

            System.out.println(e.getMessage());

                try
                {
                    System.out.println("Trying to get rollback.");

                    con.rollback();

                    System.out.println("You are rolled back.");
                }

                catch (Exception exc)
                {
                    exc.printStackTrace();
                }

        }

        finally
        {
            closeConnection(preparedStatement, con);
        }
    }

    private static void updateinTalbe()
    {
        Connection con = null;

        PreparedStatement preparedStatement = null;
    }

    public static void main(String[] args)
    {

        try
        {
            Scanner scanner = new Scanner(System.in);

            System.out.println("Enter any number");

            System.out.println("1.Insert Query ");

            int number = scanner.nextInt();

            switch (number)
            {
                case 1:

                    BatchUsingPreparedStatement.insertinTable();

                    break;

                default:
                    System.out.println("Enter valid Number");
            }


        }

        catch (Exception e)
        {
            System.out.println(e);
        }


    }
}
