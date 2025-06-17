import javafx.application.Application;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleStringProperty;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.scene.Scene;
import javafx.scene.control.*;
import javafx.scene.control.cell.CheckBoxTableCell;
import javafx.scene.layout.*;
import javafx.stage.Stage;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class javafxmysql3 extends Application {

    String url = "jdbc:mysql://localhost:3306/sys?useSSL=false&serverTimezone=UTC";
    String user = "root";
    String password = "Akhila@7496"; // Replace with your actual MySQL password

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        Label message = new Label("SQL CRUD Operations (MySQL DB)");
        Button createBtn = new Button("Create Table");
        Button insertBtn = new Button("Insert Values");
        Button updateBtn = new Button("Update Table");
        Button deleteBtn = new Button("Delete Table");
        Button truncateBtn = new Button("Truncate Table");
        Button dropBtn = new Button("Drop Table");
        Button selectBtn = new Button("Select Records");

        createBtn.setOnAction(e -> showCreateTableWindow());
        insertBtn.setOnAction(e -> showInsertWindow());
        updateBtn.setOnAction(e -> showUpdateWindow());
        deleteBtn.setOnAction(e -> showSimpleTableActionWindow("Delete", "DELETE FROM "));
        truncateBtn.setOnAction(e -> showSimpleTableActionWindow("Truncate", "TRUNCATE TABLE "));
        dropBtn.setOnAction(e -> showSimpleTableActionWindow("Drop", "DROP TABLE "));
        selectBtn.setOnAction(e -> showSelectWindow());

        VBox root = new VBox(15, message, createBtn, insertBtn, updateBtn, deleteBtn, truncateBtn, dropBtn, selectBtn);
        root.setStyle("-fx-padding: 30; -fx-alignment: center;");
        Scene scene = new Scene(root, 400, 550);
        primaryStage.setScene(scene);
        primaryStage.setTitle("JavaFX MySQL DB Manager");
        primaryStage.show();
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    private void showAlert(Alert.AlertType type, String message) {
        Alert alert = new Alert(type, message);
        alert.showAndWait();
    }

    public static class RowData {
        SimpleBooleanProperty selected = new SimpleBooleanProperty(false);
        List<SimpleStringProperty> values;

        RowData(List<SimpleStringProperty> values) {
            this.values = values;
        }
    }

    private List<String> getPrimaryKeyColumns(Connection conn, String tableName) throws SQLException {
        List<String> pkCols = new ArrayList<>();
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet pkRs = metaData.getPrimaryKeys(conn.getCatalog(), null, tableName)) {
            while (pkRs.next()) {
                pkCols.add(pkRs.getString("COLUMN_NAME"));
            }
        }
        return pkCols;
    }

    private void showInsertWindow() {
        Stage stage = new Stage();
        TextField tableField = new TextField();
        Button loadBtn = new Button("Load Table Structure");

        VBox layout = new VBox(10, new Label("Table Name:"), tableField, loadBtn);
        layout.setStyle("-fx-padding: 20; -fx-alignment: center;");
        stage.setScene(new Scene(layout, 400, 150));
        stage.setTitle("Insert Values");
        stage.show();

        loadBtn.setOnAction(ev -> {
            String tableName = tableField.getText().trim();
            if (tableName.isEmpty()) {
                showAlert(Alert.AlertType.ERROR, "Table name is required.");
                return;
            }

            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 1")) {

                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                List<TextField> inputFields = new ArrayList<>();
                VBox formLayout = new VBox(10);
                formLayout.setStyle("-fx-padding: 20;");

                for (int i = 1; i <= columnCount; i++) {
                    String colName = meta.getColumnName(i);
                    String colType = meta.getColumnTypeName(i);
                    TextField input = new TextField();
                    input.setPromptText("Enter value for " + colName);
                    inputFields.add(input);

                    formLayout.getChildren().add(new HBox(10, new Label(colName + " (" + colType + "):"), input));
                }

                Button insertBtn = new Button("Insert");
                formLayout.getChildren().add(insertBtn);

                Stage insertStage = new Stage();
                insertStage.setScene(new Scene(new ScrollPane(formLayout), 400, 400));
                insertStage.setTitle("Insert into " + tableName);
                insertStage.show();

                insertBtn.setOnAction(ev2 -> {
                    try {
                        StringBuilder columns = new StringBuilder();
                        StringBuilder values = new StringBuilder();

                        for (int i = 0; i < columnCount; i++) {
                            String val = inputFields.get(i).getText();
                            if (val.isEmpty()) {
                                showAlert(Alert.AlertType.ERROR, "All fields are required.");
                                return;
                            }
                            if (columns.length() > 0) {
                                columns.append(", ");
                                values.append(", ");
                            }

                            columns.append(meta.getColumnName(i + 1));
                            int type = meta.getColumnType(i + 1);

                            if (type == Types.INTEGER || type == Types.BIGINT || type == Types.FLOAT || type == Types.DOUBLE) {
                                values.append(val);
                            } else {
                                values.append("'").append(val.replace("'", "''")).append("'");
                            }
                        }

                        String sql = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")";
                        try (Connection conn2 = getConnection();
                             Statement stmt2 = conn2.createStatement()) {
                            stmt2.executeUpdate(sql);
                            showAlert(Alert.AlertType.INFORMATION, "Record inserted successfully.");
                            insertStage.close();
                        }
                    } catch (SQLException ex) {
                        showAlert(Alert.AlertType.ERROR, ex.getMessage());
                    }
                });

            } catch (SQLException ex) {
                showAlert(Alert.AlertType.ERROR, ex.getMessage());
            }
        });
    }

    private void showCreateTableWindow() {
        Stage stage = new Stage();
        TextField tableNameField = new TextField();
        TextField fieldCountField = new TextField();
        Button nextBtn = new Button("Next");

        VBox layout = new VBox(10, new Label("Table Name:"), tableNameField,
                new Label("Number of Fields:"), fieldCountField, nextBtn);
        layout.setStyle("-fx-padding: 20; -fx-alignment: center;");
        stage.setScene(new Scene(layout, 300, 250));
        stage.setTitle("Create Table");
        stage.show();

        nextBtn.setOnAction(e -> {
            String tableName = tableNameField.getText().trim();
            int count;
            try {
                count = Integer.parseInt(fieldCountField.getText().trim());
            } catch (NumberFormatException ex) {
                showAlert(Alert.AlertType.ERROR, "Invalid field count.");
                return;
            }
            showFieldCountWindow(stage, tableName, count);
        });
    }

    private void showFieldCountWindow(Stage parent, String tableName, int count) {
        Stage stage = new Stage();
        VBox fieldsBox = new VBox(10);
        List<TextField> names = new ArrayList<>();
        List<TextField> types = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            TextField name = new TextField();
            TextField type = new TextField();
            names.add(name);
            types.add(type);
            fieldsBox.getChildren().add(new HBox(10, new Label("Field " + (i + 1) + ":"), name, type));
        }

        Button createBtn = new Button("Create Table");
        fieldsBox.getChildren().add(createBtn);
        fieldsBox.setStyle("-fx-padding: 20;");
        stage.setScene(new Scene(fieldsBox, 400, 300));
        stage.setTitle("Field Details");
        stage.show();

        createBtn.setOnAction(e -> {
            StringBuilder sb = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
            for (int i = 0; i < count; i++) {
                sb.append(names.get(i).getText()).append(" ").append(types.get(i).getText());
                if (i < count - 1) sb.append(", ");
            }
            sb.append(")");
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(sb.toString());
                showAlert(Alert.AlertType.INFORMATION, "Table created successfully.");
                stage.close();
                parent.close();
            } catch (SQLException ex) {
                showAlert(Alert.AlertType.ERROR, ex.getMessage());
            }
        });
    }

    private void showUpdateWindow() {
        showAlert(Alert.AlertType.INFORMATION, "Update functionality not implemented in this demo.");
    }

    private void showSimpleTableActionWindow(String actionName, String sqlPrefix) {
        Stage stage = new Stage();
        TextField tableField = new TextField();
        Button actionBtn = new Button(actionName);

        VBox layout = new VBox(10, new Label(actionName + " Table:"), tableField, actionBtn);
        layout.setStyle("-fx-padding: 20; -fx-alignment: center;");
        stage.setScene(new Scene(layout, 300, 150));
        stage.setTitle(actionName + " Table");
        stage.show();

        actionBtn.setOnAction(e -> {
            String tableName = tableField.getText().trim();
            if (tableName.isEmpty()) {
                showAlert(Alert.AlertType.ERROR, "Table name is required.");
                return;
            }
            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(sqlPrefix + tableName);
                showAlert(Alert.AlertType.INFORMATION, actionName + " operation successful.");
                stage.close();
            } catch (SQLException ex) {
                showAlert(Alert.AlertType.ERROR, ex.getMessage());
            }
        });
    }

    private void showSelectWindow() {
        showAlert(Alert.AlertType.INFORMATION, "Select window is not updated in this version.");
    }
}
