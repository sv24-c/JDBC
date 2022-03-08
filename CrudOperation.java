package crudopration;

import org.omg.CORBA.CODESET_INCOMPATIBLE;
import org.omg.IOP.TAG_ALTERNATE_IIOP_ADDRESS;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.*;
import java.util.*;

/**
 * Created by smit on 3/3/22.
 */
public class CrudOperation
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
            if (con == null)
            {

                con = DriverManager.getConnection("jdbc:mysql://localhost:3306/mysqldb?characterEncoding=latin1", "root", "Mind@123");
            }

            System.out.println("Connection Established..");

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

            if (preparedStatement != null || !preparedStatement.isClosed())
            {
                preparedStatement.close();
            }

            if (connection != null || !connection.isClosed())
            {
                connection.close();

                System.out.println("Connection Closed...");
            }

        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    private static void viewTable()
    {

        Connection con = null;

        PreparedStatement preparedStatement = null;

        try
        {

            con = makeConnection();

            preparedStatement = con.prepareStatement("SELECT * FROM emp");

            System.out.println("Prepared statement created successfully");

            //preparedStatement.setString(1, "pavan");

            //preparedStatement.setString(2,"smit");

            //preparedStatement.setString(3, "age");

            //preparedStatement.setString(4, "address");

            ResultSet resultSet = preparedStatement.executeQuery();

            List<Map<String, Object>> list =  new ArrayList<Map<String, Object>>();

            System.out.println("New ArrayList created");

            while (resultSet.next())
            {
                Map<String, Object> map = new LinkedHashMap<String, Object>();

                System.out.println("New LinkedHashMap has created ");

                map.put("id", resultSet.getInt(1));

                map.put("name", resultSet.getString(2));

                map.put("age", resultSet.getInt(3));

                map.put("address", resultSet.getString(4));

                list.add(map);

                System.out.println("Map added to list");

            }

            for (int i = 0; i <list.size() ; i++)
            {
                System.out.println(list.get(i));
            }



        }

        catch (SQLException e)
        {
            //e.printStackTrace();

            System.out.println("SQL State: "+ e.getSQLState());

            System.out.println("Error Code "+ e.getErrorCode());

            System.out.println(e.getMessage());

        }

        finally
        {
            closeConnection(preparedStatement, con);
        }



    }

    private static void updateTable()
    {
        Connection con = null;

        PreparedStatement preparedStatementOfUpdate = null;

        try
        {
            con = makeConnection();

            preparedStatementOfUpdate = con.prepareStatement("UPDATE emp SET name=? where id=?");

            System.out.println("Prepared statement created successfully");

            preparedStatementOfUpdate.setString(1, "smitvachhani");

            preparedStatementOfUpdate.setInt(2, 12);

            int result = preparedStatementOfUpdate.executeUpdate();

            System.out.println(result+" record updated successfully");


        }

        catch (Exception e)
        {
            System.out.println(e.getMessage());

            System.out.println(e.getStackTrace());

        }

        finally
        {
            closeConnection(preparedStatementOfUpdate, con);
        }


    }

    private static void deleteentryTable() throws SQLException {

        Connection con = null;

        PreparedStatement preparedStatementOfDeleteRow = null;

        try
        {
            con = makeConnection();

            preparedStatementOfDeleteRow = con.prepareStatement("DELETE from emp where id=?");

            preparedStatementOfDeleteRow.setInt(1, 333);

            int result = preparedStatementOfDeleteRow.executeUpdate();

            System.out.println(result+" record deleted successfully");

        }

        catch (Exception e)
        {
            System.out.println(e.getMessage());

            System.out.println(e.getStackTrace());

        }

        finally
        {
            closeConnection(preparedStatementOfDeleteRow, con);
        }
    }

    private static void insertRowTable()
    {
        Connection con = null;

        PreparedStatement preparedStatementOfInsert = null;

        try
        {
            con = makeConnection();

            preparedStatementOfInsert = con.prepareStatement("INSERT INTO emp VALUES(?,?,?,?)");

            preparedStatementOfInsert.setInt(1, 333);

            preparedStatementOfInsert.setString(2, "xyzxyz");

            preparedStatementOfInsert.setInt(3, 33);

            preparedStatementOfInsert.setString(4, "ahm");

            int result = preparedStatementOfInsert.executeUpdate();

            System.out.println(result+" record inserted successfully");

        }

        catch (Exception e)
        {
            System.out.println(e.getMessage());

            System.out.println(e.getStackTrace());

        }

        finally
        {
            closeConnection(preparedStatementOfInsert, con);
        }
    }

    private static void createTable()
    {
        Connection con = null;

        PreparedStatement preparedStatementOfCreateTable = null;

        try
        {
            con = makeConnection();

            preparedStatementOfCreateTable = con.prepareStatement("CREATE TABLE copyofcaragain (Number INT (10), ModalName VARCHAR (30), Price int(10))");

            int result = preparedStatementOfCreateTable.executeUpdate();

            System.out.println(result+" Table Created Successfully");

        }

        catch (Exception e)
        {
            System.out.println(e.getMessage());

            System.out.println(e.getStackTrace());

        }

        finally
        {
            closeConnection(preparedStatementOfCreateTable, con);
        }

    }

    private static void showTableList() throws SQLException
    {

        Connection con = null;

        PreparedStatement preparedStatement = null;

        //Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/mysqldb?characterEncoding=latin1", "root", "Mind@123");

        try
        {
            con = makeConnection();

            DatabaseMetaData databaseMetaData = con.getMetaData();

            String table[] = {"TABLE"};

            ResultSet resultSet = databaseMetaData.getTables(null,null,null, table);

            while (resultSet.next())
            {
                System.out.println(resultSet.getString(3));
            }

        }

        catch (Exception e)
        {
            System.out.println(e.getMessage());

            System.out.println(e.getStackTrace());

        }

        finally
        {
            closeConnection(preparedStatement, con);
        }
    }

    public static void main(String[] args)
    {

        try
        {

            System.out.println("Enter Number You Want to perform your Query Execution: ");

            System.out.println("1.Select query.    2.Update Query.    3.Delete Query.    4. Insert Query.    5.Create Table.    6.List of Tables in Database");

            Scanner scanner = new Scanner(System.in);

            int inputNumber = scanner.nextInt();

            switch (inputNumber)
            {
                case 1:

                    System.out.println("Select Query Result is: ");

                    System.out.println();

                    CrudOperation.viewTable();

                    break;

                case 2:

                    System.out.println("Update Query Result is: ");

                    System.out.println();

                    CrudOperation.updateTable();

                    break;

                case 3:

                    System.out.println("Delete Query Result is:");

                    System.out.println();

                    CrudOperation.deleteentryTable();

                    break;

                case 4:

                    System.out.println("Insert Query Result is:");

                    System.out.println();

                    CrudOperation.insertRowTable();

                    break;

                case 5:

                    System.out.println("Create Table Result is:");

                    System.out.println();

                    CrudOperation.createTable();

                    break;

                case 6:

                    System.out.println("Table List is: ");

                    CrudOperation.showTableList();

                    break;

                default:

                    System.out.println("Sorry Input number provided above");
            }
        }
        catch (Exception e)
        {
            System.out.println(e);
        }
    }

}
