package org.example;

import java.sql.*;

public class EmployeeService {
    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        String user = "user";
        String password = "1234";

        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            System.out.println("Connection ok");

            //Creating and filling table
            dropTable(connection);
            createTable(connection);
            initTable(connection);

            //Find employee by id
            ResultSet result = findById(8, connection);
            showResult(result);

            result = groupByName(connection);
            showGroupResult(result, "count", "name");

            //Find employees by birthdates in some interval
            result = findBetween("1990-01-01", "1992-12-31", connection);
            showResult(result);



        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    static void showGroupResult(ResultSet result, String newColumnName, String groupName) throws SQLException {

        if(result.next()){
            System.out.println(groupName + "\t" + newColumnName);
            do{

                String firstColumn = result.getString(groupName);
                String secondColumn= result.getString(newColumnName);

                System.out.println(firstColumn + "\t" + secondColumn);
            }while(result.next());
        }else{
            System.out.println("Result is empty");
        }
    }
    static void showResult(ResultSet result) throws SQLException {

        if(result.next()){
            System.out.println("Id \t Name \t Surname \t Birth date \t Department \t Salary");
            do{

                int id = result.getInt("id");
                String name = result.getString("name");
                String surname = result.getString("surname");
                String birthDate = result.getString("birth_date");
                String department = result.getString("department");
                double salary = result.getDouble("salary");


                System.out.println(id + "\t" + name + "\t" + surname + "\t\t" + birthDate + "\t\t" + department + "\t\t\t\t" + salary);
            }while(result.next());
        }else{
            System.out.println("Result is empty");
        }
    }
    static void createTable(Connection connection) throws SQLException {

        Statement createStatement = connection.createStatement();
        createStatement.executeUpdate("CREATE TABLE IF NOT EXISTS Employee (" +
                "id SERIAL PRIMARY KEY, " +
                "name VARCHAR(25) NOT NULL, " +
                "surname VARCHAR(25) NOT NULL, " +
                "birth_date  DATE NOT NULL, " +
                "department VARCHAR(100), " +
                "salary DECIMAL NOT NULL);");

    }
    static void  initTable(Connection connection) throws SQLException {

        PreparedStatement insertStatement = connection.prepareStatement(
                "INSERT INTO Employee (id, name, surname, birth_date, department, salary) VALUES" +
                        "(1, 'Иван', 'Иванов', '1985-06-15', 'IT', 60000.00)," +
                        "(2, 'Мария', 'Петрова', '1990-03-22', 'HR', 55000.00)," +
                        "(3, 'Сергей', 'Сидоров', '1982-11-30', 'Finance', 70000.00)," +
                        "(4, 'Анна', 'Кузнецова', '1995-01-10', 'Marketing', 50000.00)," +
                        "(5, 'Дмитрий', 'Смирнов', '1988-09-05', 'IT', 62000.00);");
        insertStatement.executeUpdate();
    }
    static  void dropTable(Connection connection) throws SQLException{

        PreparedStatement dropStatement = connection.prepareStatement("DROP TABLE IF EXISTS Employee ");
        dropStatement.executeUpdate();
    }
    static ResultSet findById(int id, Connection connection) throws SQLException {

        System.out.println("\n Finding by id: " + id);
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM Employee WHERE id = ?");
        statement.setInt(1, id);
        return statement.executeQuery();
    }
    static ResultSet groupByName(Connection connection) throws SQLException {

        System.out.println("\n Group by names: ");
        PreparedStatement statement = connection.prepareStatement("SELECT name, COUNT(*) FROM Employee GROUP BY name");
        return statement.executeQuery();
    }
    static ResultSet findBetween(String from, String to, Connection connection) throws SQLException {

        System.out.println("\n Finding between: " + from + " and " + to);
        PreparedStatement statement = connection.prepareStatement("SELECT * FROM Employee WHERE birth_date BETWEEN ? AND ?");
        statement.setDate(1, Date.valueOf(from));
        statement.setDate(2, Date.valueOf(to));
        return statement.executeQuery();
    }
}