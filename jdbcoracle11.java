import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

public class jdbcoracle11{
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", "system", "sqlmy")) {
            while (true) {
                System.out.println("\nMenu:");
                System.out.println("1. Create Table");
                System.out.println("2. Insert Data (Manual)");
                System.out.println("3. Insert Data (From File)");
                System.out.println("4. Update Data (Manual)");
                System.out.println("5. Update Data (From File)");
                System.out.println("6. Delete Record (Manual)");
                System.out.println("7. Delete Record (From File)");
                System.out.println("8. Clear All Data (Truncate)");
                System.out.println("9. Delete Entire Table");
                System.out.println("10. View Records (Paginated)");
                System.out.println("11. Exit");
                System.out.println("12. Create Log Table and Triggers for a Table");
                System.out.print("Enter your choice: ");
                String choice = scanner.nextLine();

                switch (choice) {
                    case "1": createTable(conn); break;
                    case "2": insertManual(conn); break;
                    case "3": insertFromFile(conn); break;
                    case "4": updateManual(conn); break;
                    case "5": updateFromFile(conn); break;
                    case "6": deleteManual(conn); break;
                    case "7": deleteFromFile(conn); break;
                    case "8": truncateTable(conn); break;
                    case "9": deleteTable(conn); break;
                    case "10": selectRecords(conn); break;
                    case "11": System.out.println("Exiting..."); return;
                    case "12": createLogTableAndTriggers(conn); break;
                    default: System.out.println("Invalid choice.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Database Error: " + e.getMessage());
        }
    }

    private static void insertManual(Connection conn) throws SQLException {
        System.out.print("Enter table name: ");
        String table = scanner.nextLine();
        List<ColumnInfo> columns = getTableColumnsWithPrecision(conn, table);
        if (columns.isEmpty()) {
            System.out.println("Invalid table or no columns found.");
            return;
        }

        StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" (");
        for (ColumnInfo col : columns) {
            sql.append(col.name).append(", ");
        }
        sql.setLength(sql.length() - 2);
        sql.append(") VALUES (").append("?, ".repeat(columns.size()));
        sql.setLength(sql.length() - 2);
        sql.append(")");

        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnInfo col = columns.get(i);
                while (true) {
                    System.out.print("Enter value for " + col.name + " (" + col.type + "): ");
                    String input = scanner.nextLine().trim();
                    if (input.equalsIgnoreCase("null") || input.isEmpty()) {
                        pstmt.setNull(i + 1, java.sql.Types.NULL);
                        break;
                    }
                    try {
                        if (col.type.toUpperCase().startsWith("NUMBER")) {
                            pstmt.setBigDecimal(i + 1, new BigDecimal(input));
                        } else if (col.type.toUpperCase().startsWith("VARCHAR") || col.type.equalsIgnoreCase("CHAR")) {
                            pstmt.setString(i + 1, input);
                        } else if (col.type.equalsIgnoreCase("DATE")) {
                            pstmt.setDate(i + 1, java.sql.Date.valueOf(input));
                        } else {
                            pstmt.setString(i + 1, input);
                        }
                        break;
                    } catch (Exception e) {
                        System.out.println("Invalid value. Try again.");
                    }
                }
            }
            pstmt.executeUpdate();
            System.out.println("Record inserted.");
        }
    }

    private static void insertFromFile(Connection conn) {
        try {
            System.out.print("Enter file path (SQL file with INSERT statements): ");
            String filePath = scanner.nextLine().replace("\"", "").trim();

            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                int count = 0;
                try (Statement stmt = conn.createStatement()) {
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (!line.isEmpty()) {
                            stmt.executeUpdate(line);
                            count++;
                        }
                    }
                }
                System.out.println(count + " SQL statement(s) executed from file.");
            }
        } catch (Exception e) {
            System.out.println("Error inserting from file: " + e.getMessage());
        }
    }

    private static void updateManual(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter column to update: ");
            String col = scanner.nextLine();
            System.out.print("Enter new value: ");
            String newVal = scanner.nextLine();
            System.out.print("Enter condition (e.g., id=1): ");
            String cond = scanner.nextLine();

            String sql = "UPDATE " + table + " SET " + col + " = ? WHERE " + cond;
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, newVal);
                int updated = pstmt.executeUpdate();
                System.out.println(updated + " record(s) updated.");
            }
        } catch (SQLException e) {
            System.out.println("Update error: " + e.getMessage());
        }
    }

    private static void updateFromFile(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter CSV file path: ");
            String filePath = scanner.nextLine();
            System.out.print("Enter condition column name (e.g., id): ");
            String conditionCol = scanner.nextLine();

            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String[] headers = br.readLine().split(",");
                List<String> updateCols = new ArrayList<>(Arrays.asList(headers));
                updateCols.remove(conditionCol);

                StringBuilder sql = new StringBuilder("UPDATE ").append(table).append(" SET ");
                for (String col : updateCols) sql.append(col).append(" = ?, ");
                sql.setLength(sql.length() - 2);
                sql.append(" WHERE ").append(conditionCol).append(" = ?");

                try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] values = line.split(",");
                        int i = 0;
                        for (; i < updateCols.size(); i++) {
                            pstmt.setString(i + 1, values[i + 1].trim());
                        }
                        pstmt.setString(i + 1, values[0].trim());
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                    System.out.println("Records updated from file.");
                }
            }
        } catch (Exception e) {
            System.out.println("Update from file error: " + e.getMessage());
        }
    }

    private static void deleteManual(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter condition (e.g., id=1): ");
            String cond = scanner.nextLine();
            String sql = "DELETE FROM " + table + " WHERE " + cond;
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                int deleted = pstmt.executeUpdate();
                System.out.println(deleted + " record(s) deleted.");
            }
        } catch (SQLException e) {
            System.out.println("Delete error: " + e.getMessage());
        }
    }

    private static void deleteFromFile(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter file path with keys to delete: ");
            String filePath = scanner.nextLine();
            System.out.print("Enter condition column name (e.g., id): ");
            String condCol = scanner.nextLine();

            String sql = "DELETE FROM " + table + " WHERE " + condCol + " = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql);
                 BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    pstmt.setString(1, line.trim());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                System.out.println("Records deleted from file.");
            }
        } catch (Exception e) {
            System.out.println("Delete from file error: " + e.getMessage());
        }
    }

    private static void truncateTable(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("TRUNCATE TABLE " + table);
                System.out.println("Table truncated.");
            }
        } catch (SQLException e) {
            System.out.println("Truncate error: " + e.getMessage());
        }
    }

    private static void deleteTable(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE " + table);
                System.out.println("Table deleted.");
            }
        } catch (SQLException e) {
            System.out.println("Delete table error: " + e.getMessage());
        }
    }

    private static void selectRecords(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter number of rows per page: ");
            int pageSize = Integer.parseInt(scanner.nextLine());

            int offset = 0;
            while (true) {
                String sql = "SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (SELECT * FROM " + table +
                        ") a WHERE ROWNUM <= ?) WHERE rnum > ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setInt(1, offset + pageSize);
                    pstmt.setInt(2, offset);
                    ResultSet rs = pstmt.executeQuery();
                    ResultSetMetaData meta = rs.getMetaData();
                    int colCount = meta.getColumnCount();

                    List<String[]> rows = new ArrayList<>();
                    int[] colWidths = new int[colCount];

                    for (int i = 0; i < colCount; i++) {
                        colWidths[i] = meta.getColumnName(i + 1).length();
                    }

                    while (rs.next()) {
                        String[] row = new String[colCount];
                        for (int i = 0; i < colCount; i++) {
                            String val = rs.getString(i + 1);
                            row[i] = (val == null) ? "null" : val;
                            colWidths[i] = Math.max(colWidths[i], row[i].length());
                        }
                        rows.add(row);
                    }

                    if (rows.isEmpty()) {
                        System.out.println("No more records.");
                        break;
                    }

                    for (int i = 0; i < colCount; i++) {
                        System.out.printf("%-" + (colWidths[i] + 2) + "s", meta.getColumnName(i + 1));
                    }
                    System.out.println();
                    for (int i = 0; i < Arrays.stream(colWidths).sum() + (2 * colCount); i++) {
                        System.out.print("-");
                    }
                    System.out.println();

                    for (String[] row : rows) {
                        for (int i = 0; i < colCount; i++) {
                            System.out.printf("%-" + (colWidths[i] + 2) + "s", row[i]);
                        }
                        System.out.println();
                    }

                    System.out.print("Next page? (y/n): ");
                    if (!scanner.nextLine().equalsIgnoreCase("y")) break;

                    offset += pageSize;
                }
            }
        } catch (Exception e) {
            System.out.println("Select error: " + e.getMessage());
        }
    }

    private static void createTable(Connection conn) throws SQLException {
        System.out.print("Enter table name: ");
        String table = scanner.nextLine();

        System.out.print("Enter number of columns: ");
        int colCount = Integer.parseInt(scanner.nextLine());
        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(table).append(" (");

        for (int i = 0; i < colCount; i++) {
            System.out.print("Enter column " + (i + 1) + " name: ");
            String colName = scanner.nextLine();
            System.out.print("Enter column " + (i + 1) + " type (e.g., VARCHAR2(100), NUMBER, DATE): ");
            String colType = scanner.nextLine();
            sb.append(colName).append(" ").append(colType);
            if (i != colCount - 1) sb.append(", ");
        }
        sb.append(")");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sb.toString());
            System.out.println("Table created successfully.");
        }
    }

    private static List<ColumnInfo> getTableColumnsWithPrecision(Connection conn, String table) {
        List<ColumnInfo> list = new ArrayList<>();
        try (PreparedStatement pstmt = conn.prepareStatement(
                "SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?")) {
            pstmt.setString(1, table.toUpperCase());
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString("COLUMN_NAME");
                String type = rs.getString("DATA_TYPE");
                int precision = rs.getInt("DATA_PRECISION");
                int scale = rs.getInt("DATA_SCALE");
                list.add(new ColumnInfo(name, type, precision, scale));
            }
        } catch (SQLException e) {
            System.out.println("Error fetching column metadata: " + e.getMessage());
        }
        return list;
    }

    static class ColumnInfo {
        String name, type;
        int precision, scale;

        ColumnInfo(String name, String type) {
            this.name = name;
            this.type = type;
        }

        ColumnInfo(String name, String type, int precision, int scale) {
            this.name = name;
            this.type = type;
            this.precision = precision;
            this.scale = scale;
        }
    }

    private static void createLogTableAndTriggers(Connection conn) {
        try {
            System.out.print("Enter the table name to create triggers for: ");
            String tableName = scanner.nextLine().toUpperCase();

            // Create log table if not exists
            String createLogTableSQL = "BEGIN\n" +
                    "   EXECUTE IMMEDIATE 'CREATE TABLE audit_log (\n" +
                    "       id NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY PRIMARY KEY,\n" +
                    "       action VARCHAR2(10),\n" +
                    "       table_name VARCHAR2(50),\n" +
                    "       record_id VARCHAR2(4000),\n" +
                    "       action_timestamp TIMESTAMP DEFAULT SYSTIMESTAMP\n" +
                    "   )';\n" +
                    "EXCEPTION\n" +
                    "   WHEN OTHERS THEN\n" +
                    "       IF SQLCODE != -955 THEN\n" + // ORA-00955: name is already used by an existing object
                    "           RAISE;\n" +
                    "       END IF;\n" +
                    "END;";

            try (CallableStatement cstmt = conn.prepareCall(createLogTableSQL)) {
                cstmt.execute();
                System.out.println("Audit log table created or already exists.");
            }

            // Drop existing triggers (if any), safely ignoring errors
            String[] triggerNames = {
                "trg_audit_log_insert_" + tableName,
                "trg_audit_log_update_" + tableName,
                "trg_audit_log_delete_" + tableName
            };
            for (String trgName : triggerNames) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate("DROP TRIGGER " + trgName);
                    System.out.println("Dropped existing trigger " + trgName);
                } catch (SQLException e) {
                    // Ignore errors about trigger doesn't exist
                    if (!e.getMessage().contains("ORA-04080")) { // ORA-04080: trigger does not exist
                        System.out.println("Error dropping trigger " + trgName + ": " + e.getMessage());
                    }
                }
            }

            // CREATE INSERT TRIGGER
            String insertTriggerSQL = 
                    "CREATE OR REPLACE TRIGGER trg_audit_log_insert_" + tableName + "\n" +
                    "BEFORE INSERT ON " + tableName + "\n" +
                    "FOR EACH ROW\n" +
                    "DECLARE\n" +
                    "   v_rec_id VARCHAR2(4000);\n" +
                    "BEGIN\n" +
                    "   BEGIN\n" +
                    "     v_rec_id := :NEW.ID;\n" +
                    "   EXCEPTION\n" +
                    "     WHEN OTHERS THEN\n" +
                    "       v_rec_id := NULL;\n" +
                    "   END;\n" +
                    "   INSERT INTO audit_log(action, table_name, record_id, action_timestamp)\n" +
                    "   VALUES ('INSERT', '" + tableName + "', NVL(TO_CHAR(v_rec_id), 'NULL'), SYSTIMESTAMP);\n" +
                    "END;";

            // CREATE UPDATE TRIGGER
            String updateTriggerSQL = 
                    "CREATE OR REPLACE TRIGGER trg_audit_log_update_" + tableName + "\n" +
                    "BEFORE UPDATE ON " + tableName + "\n" +
                    "FOR EACH ROW\n" +
                    "DECLARE\n" +
                    "   v_rec_id VARCHAR2(4000);\n" +
                    "BEGIN\n" +
                    "   BEGIN\n" +
                    "     v_rec_id := :NEW.ID;\n" +
                    "   EXCEPTION\n" +
                    "     WHEN OTHERS THEN\n" +
                    "       v_rec_id := NULL;\n" +
                    "   END;\n" +
                    "   INSERT INTO audit_log(action, table_name, record_id, action_timestamp)\n" +
                    "   VALUES ('UPDATE', '" + tableName + "', NVL(TO_CHAR(v_rec_id), 'NULL'), SYSTIMESTAMP);\n" +
                    "END;";

            // CREATE DELETE TRIGGER
            String deleteTriggerSQL = 
                    "CREATE OR REPLACE TRIGGER trg_audit_log_delete_" + tableName + "\n" +
                    "BEFORE DELETE ON " + tableName + "\n" +
                    "FOR EACH ROW\n" +
                    "DECLARE\n" +
                    "   v_rec_id VARCHAR2(4000);\n" +
                    "BEGIN\n" +
                    "   BEGIN\n" +
                    "     v_rec_id := :OLD.ID;\n" +
                    "   EXCEPTION\n" +
                    "     WHEN OTHERS THEN\n" +
                    "       v_rec_id := NULL;\n" +
                    "   END;\n" +
                    "   INSERT INTO audit_log(action, table_name, record_id, action_timestamp)\n" +
                    "   VALUES ('DELETE', '" + tableName + "', NVL(TO_CHAR(v_rec_id), 'NULL'), SYSTIMESTAMP);\n" +
                    "END;";

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(insertTriggerSQL);
                stmt.execute(updateTriggerSQL);
                stmt.execute(deleteTriggerSQL);
                System.out.println("Triggers created on table " + tableName + ".");
            } catch (SQLException e) {
                System.out.println("Error creating triggers: " + e.getMessage());
            }
        } catch (SQLException e) {
            System.out.println("Error in createLogTableAndTriggers: " + e.getMessage());
        }
    }
}