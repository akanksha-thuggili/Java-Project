
import java.io.*;
import java.sql.*;
import java.util.Scanner;

public class FileDBExecutorAnyTable {

    public static void main(String[] args) {

        // Default menu options
        String[] menuOptions = {
            "1. Create Tables (dept.sql)",
            "2. Insert into DEPT (dept1.txt)",
            "3. Insert into EMP (emp.txt)",
            "4. Insert into SALGRADE (salgrade.txt)",
            "5. Drop Tables (drop_tables.sql)",
            "6. Execute SQL from a Custom File"
        };

        // Default file paths for options 1–5
        String[] filePaths = {
            "D:\\Akanksha\\dept.sql",
            "D:\\Akanksha\\dept1.txt",
            "D:\\Akanksha\\emp.txt",
            "D:\\Akanksha\\salgrade.txt",
            "D:\\Akanksha\\drop_tables.sql"
        };

        // Database connection details
        String url = "jdbc:mysql://localhost:3306/sys";
        String user = "root";
        String password = "akanksha@123";

        Scanner scanner = new Scanner(System.in);
        System.out.println("Select the SQL file to execute:");
        for (String option : menuOptions) {
            System.out.println(option);
        }

        System.out.print("Enter your choice (1-6): ");
        int choice = scanner.nextInt();
        scanner.nextLine(); // consume newline

        String selectedFile;

        if (choice >= 1 && choice <= 5) {
            selectedFile = filePaths[choice - 1];
        } else if (choice == 6) {
            System.out.print("Enter full path to your SQL file: ");
            selectedFile = scanner.nextLine();
        } else {
            System.out.println("Invalid choice. Exiting...");
            return;
        }

        File sqlFile = new File(selectedFile);
        if (!sqlFile.exists()) {
            System.out.println("File not found: " + selectedFile);
            return;
        }

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            try (
                Connection conn = DriverManager.getConnection(url, user, password);
                Statement stmt = conn.createStatement();
                BufferedReader reader = new BufferedReader(new FileReader(sqlFile))
            ) {
                System.out.println("Connected to database.");
                System.out.println("Executing SQL from: " + selectedFile + "\n");

                StringBuilder queryBuilder = new StringBuilder();
                String line;

                while ((line = reader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty() || line.startsWith("--")) continue;

                    queryBuilder.append(line).append(" ");

                    if (line.endsWith(";")) {
                        String query = queryBuilder.toString().trim();
                        query = query.substring(0, query.length() - 1); // remove trailing semicolon

                        System.out.println("Executing Query: " + query);

                        try {
                            boolean isResultSet = stmt.execute(query);

                            if (isResultSet) {
                                ResultSet rs = stmt.getResultSet();
                                ResultSetMetaData rsmd = rs.getMetaData();
                                int columnCount = rsmd.getColumnCount();

                                while (rs.next()) {
                                    for (int i = 1; i <= columnCount; i++) {
                                        System.out.print(rs.getString(i) + "\t");
                                    }
                                    System.out.println();
                                }
                                rs.close();
                            } else {
                                int rows = stmt.getUpdateCount();
                                System.out.println("Query OK, " + rows + " row(s) affected.");
                            }

                        } catch (SQLException e) {
                            System.err.println("SQL Error: " + e.getMessage());
                        }

                        System.out.println("--------------------------------------------------");
                        queryBuilder.setLength(0);
                    }
                }

            }

        } catch (ClassNotFoundException e) {
            System.err.println("MySQL JDBC Driver not found.");
        } catch (SQLException e) {
            System.err.println("Database error: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("File read error: " + e.getMessage());
        }
    }
}
