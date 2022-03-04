package crudopration;

import org.omg.CORBA.CODESET_INCOMPATIBLE;

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

    private static void viewTable(Connection con)
    {

        /*try(PreparedStatement preparedStatement = con.prepareStatement("SELECT id, name, age FROM emp"))
        {
            ResultSet resultSet = preparedStatement.executeQuery();

            con.setAutoCommit(false);

            while (resultSet.next())
            {
                int i = resultSet.getInt("id");

                String n = resultSet.getString("name");

                int a = resultSet.getInt("age");

                System.out.println(i+" "+n+" "+a);
            }

            for (Map.Entry<String, Integer> e : hashMap.entrySet())
            {
                int id = resultSet.getInt(e.getKey());

                String name = resultSet.getString(e.getValue());

                int age = resultSet.getInt(e.getKey());

                System.out.println(id+" "+name+" "+age);

                con.commit();
            }

        }*/

        try
        {
            PreparedStatement preparedStatement = con.prepareStatement("SELECT * FROM emp");

            //preparedStatement.setString(1, "pavan");

            //preparedStatement.setString(2,"smit");

            //preparedStatement.setString(3, "age");

            //preparedStatement.setString(4, "address");

            con.setAutoCommit(false);

            ResultSet resultSet = preparedStatement.executeQuery();

            ResultSetMetaData metaData = resultSet.getMetaData();

            Map<String, List<Object>> resultMap = new LinkedHashMap<String, List<Object>>();

            while (resultSet.next())
            {

                for (int i=1;i<=metaData.getColumnCount();i++)
                {
                    String strcolumnName = metaData.getColumnName(i);

                    //Object columnValue = resultSet.getObject(i);

                    Object columnValue = resultSet.getObject(i);

                    if (resultMap.containsKey(strcolumnName))
                    {
                        resultMap.get(strcolumnName).add(columnValue);
                    }

                    else
                    {
                        List<Object> resultList = new ArrayList<>();

                        resultList.add(columnValue);

                        resultMap.put(strcolumnName, resultList);
                    }
                }

            }

            for (Iterator<Map.Entry<String, List<Object>>> iterator = resultMap.entrySet().iterator(); iterator.hasNext();)
            {
                Map.Entry<String, List<Object>> entry = iterator.next();

                System.out.println("Column Name: "+entry.getKey());

                System.out.println("Column Value: "+entry.getValue());

            }

            con.commit();

        }

        catch (SQLException e)
        {
            //e.printStackTrace();

            System.out.println("SQL State: "+ e.getSQLState());

            System.out.println("Error Code "+ e.getErrorCode());

            System.out.println(e.getMessage());

            if (con != null)
            {
                try
                {
                    System.out.println("Trying to get rollback.");

                    con.rollback();
                }

                catch (Exception exc)
                {
                    exc.printStackTrace();
                }

            }

        }

    }

    private static void updateTable(Connection con)
    {
        try
        {
            PreparedStatement preparedStatementOfUpdate = con.prepareStatement("UPDATE emp SET name=? where id=?");

            //preparedStatementOfUpdate.setString(1, "emp");

            preparedStatementOfUpdate.setString(1, "smitvachhani");

            preparedStatementOfUpdate.setInt(2, 10);

            con.setAutoCommit(false);

            System.out.println("Enter number of Entries you want to enter");

            Scanner scanner = new Scanner(System.in);

            Scanner scannertwo = new Scanner(System.in);

            int number = scanner.nextInt();

            for (int i=1; i<=number; i++)
            {
                System.out.println("Enter the "+ i +"th entry id to update into emp table");

                preparedStatementOfUpdate.setInt(1, scanner.nextInt());

                System.out.println("Enter the"+ i +"th entry name to update into emp table");

                preparedStatementOfUpdate.setString(2, scannertwo.nextLine());

                /*System.out.println("Enter the "+ i +"th entry age to update into emp table");

                preparedStatementOfUpdate.setInt(3, scanner.nextInt());

                System.out.println("Enter the"+ i +"th entry address to update into emp table");

                preparedStatementOfUpdate.setString(4, scannertwo.nextLine());*/

                int resultSetOfUpdate = preparedStatementOfUpdate.executeUpdate();

                System.out.println(resultSetOfUpdate+" Updated Successfully");

            }

            con.commit();

        }

        catch (Exception e)
        {
            System.out.println(e.getMessage());

            System.out.println(e.getStackTrace());

            if (con != null)
            {
                try
                {
                    System.out.println("Trying to rollback.");

                    con.rollback();
                }

                catch (Exception exc)
                {
                    exc.printStackTrace();
                }

            }
        }
    }

    private static void deleteentryTable(Connection con) throws SQLException {
        try
        {

            con.setAutoCommit(false);

            String queryforDelete = " ";

            PreparedStatement preparedStatementOfDeleteRow = con.prepareStatement("DELETE from emp where id=?");

            //preparedStatementOfDeleteRow.setInt(1, 10);

            System.out.println("Enter number of Entries you want to enter");

            Scanner scanner = new Scanner(System.in);

            Scanner scannertwo = new Scanner(System.in);

            int number = scanner.nextInt();

            for (int i=1; i<=number; i++)
            {

                System.out.println("Enter the "+ i +"th entry id to delete into emp table");

                preparedStatementOfDeleteRow.setInt(1, scanner.nextInt());

                /*System.out.println("Enter the "+ i +"th entry name to delete into emp table");

                preparedStatementOfDeleteRow.setString(2, scannertwo.nextLine().trim());*/

                int resultSetofDeleteRow = preparedStatementOfDeleteRow.executeUpdate();

                System.out.println(resultSetofDeleteRow+" Delete Operation Successfully");
            }

            con.commit();
        }

        catch (Exception e)
        {
            System.out.println(e.getMessage());

            System.out.println(e.getStackTrace());

            if (con != null)
            {

                try
                {
                    System.out.println("Trying to Rollback");

                    con.rollback();
                }

                catch (SQLException e1)
                {
                    e1.printStackTrace();
                }
            }

        }
    }

    private static void insertRowTable(Connection con)
    {
        try
        {
            con.setAutoCommit(false);

            //String inserrtQuery = "";

            /*System.out.println("\nEnter the Query for Delete Operation");

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            inserrtQuery = reader.readLine();*/

            PreparedStatement preparedStatementOfInsert = con.prepareStatement("INSERT INTO emp VALUES(?,?,?,?)");

            //Map<String, List<Object>> map = new LinkedHashMap<String, List<Object>>();

            System.out.println("Enter number of Entries you want to enter");

            Scanner scanner = new Scanner(System.in);

            Scanner scannertwo = new Scanner(System.in);

            int number = scanner.nextInt();

            for (int i=1; i<=number; i++)
            {
                System.out.println("Enter the "+ i +"th entry id to insert into emp table");

                preparedStatementOfInsert.setInt(1, scanner.nextInt());

                System.out.println("Enter the"+ i +"th entry name to insert into emp table");

                preparedStatementOfInsert.setString(2, scannertwo.nextLine());

                System.out.println("Enter the "+ i +"th entry age to insert into emp table");

                preparedStatementOfInsert.setInt(3, scanner.nextInt());

                System.out.println("Enter the"+ i +"th entry address to insert into emp table");

                preparedStatementOfInsert.setString(4, scannertwo.nextLine());

                int insertedData = preparedStatementOfInsert.executeUpdate();

                System.out.println(insertedData+" Data inserted Successfully");

            }

            con.commit();
        }

        catch (Exception e)
        {
            System.out.println(e.getMessage());

            System.out.println(e.getStackTrace());

            if (con != null)
            {
                try
                {
                    con.rollback();
                }

                catch (SQLException e1)
                {
                    e1.printStackTrace();
                }
            }
        }
    }

    private static void createTable(Connection con)
    {
        try
        {
            //CREATE TABLE car (Number INT (10), ModalName VARCHAR (30), Year int(10))

            PreparedStatement preparedStatementOfCreateTable = con.prepareStatement("CREATE TABLE car (Number INT (10), ModalName VARCHAR (30), Year int(10))");

            //preparedStatementOfCreateTable.setString(1, "Number");

            //preparedStatementOfCreateTable.setInt(2, 10);

            //preparedStatementOfCreateTable.setString(2, "ModalName");

            //preparedStatementOfCreateTable.setInt(4, 30);

            //preparedStatementOfCreateTable.setString(3, "Year");

            //preparedStatementOfCreateTable.setInt(6, 10);

            int creatTable = preparedStatementOfCreateTable.executeUpdate();

            System.out.println(creatTable+" Table Created Successfully");
        }

        catch (Exception e)
        {
            System.out.println(e.getMessage());

            System.out.println(e.getStackTrace());

            if (con != null)
            {
                try
                {
                    con.rollback();
                }

                catch (SQLException e1)
                {
                    e1.printStackTrace();
                }
            }
        }

    }

    private static void showTableList(Connection con) throws SQLException
    {
        //ResultSetMetaData resultSetMetaData = (ResultSetMetaData) con.getMetaData();

        ResultSet resultSet = null;

        ResultSetMetaData metaData = resultSet.getMetaData();

        for (int i = 1; i <=metaData.getColumnCount() ; i++)
        {
            String nameofTable = metaData.getTableName(i);

            System.out.println("Name of Table is: "+nameofTable);
        }
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException
    {
        Class.forName("com.mysql.jdbc.Driver");

        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/mysqldb?characterEncoding=latin1", "root", "Mind@123");

        System.out.println("Enter Number You Want to perform your Query Execution: ");

        System.out.println("1.Select query.    2.Update Query.    3.Delete Query.    4. Insert Query.    5.Create Table.    6.List of Tables in Database");

        Scanner scanner = new Scanner(System.in);

        int inputNumber = scanner.nextInt();

        switch (inputNumber)
        {
            case 1:

                System.out.println("Select Query Result is: ");

                System.out.println();

                CrudOperation.viewTable(connection);

                break;

            case 2:

                System.out.println("Update Query Result is: ");

                System.out.println();

                CrudOperation.updateTable(connection);

                break;

            case 3:

                System.out.println("Delete Query Result is:");

                System.out.println();

                CrudOperation.deleteentryTable(connection);

                break;

            case 4:

                System.out.println("Insert Query Result is:");

                System.out.println();

                CrudOperation.insertRowTable(connection);

                break;

            case 5:

                System.out.println("Create Table Result is:");

                System.out.println();

                CrudOperation.createTable(connection);

                break;

            case 6:

                System.out.println("Table List is: ");

                CrudOperation.showTableList(connection);

                break;

            default:

                System.out.println("Sorry Input number provided above");
        }
    }

}
