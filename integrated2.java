import java.io.File;
import java.io.IOException;
import java.util.Scanner;

public class integrated2 {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // === Update this path to match your system ===
        String mysqlpath="C:\\Users\\potth\\Downloads\\MYSQL[1]\\MYSQL\\mysql.jar";

        while (true) {
            System.out.println("\n=== Main Menu ===");
            System.out.println("1. Run Calculator Program");
            System.out.println("2. Run JDBC CRUD Operations Program");
            System.out.println("3. Run JavaFX Oracle Program");
            System.out.println("4. Exit");
            System.out.print("Enter your choice: ");
            String choice = scanner.nextLine();

            switch (choice) {
                case "1":
                    runJavaClass("calculator123");
                    break;
                case "2":
                    runJavaClassWithDependency("FileDBExecutor1", mysqlpath);
                    break;
                case "3":
                    runJavaFXWithDependency(
                        "MainApp",
                        "C:\\Users\\potth\\Downloads\\openjfx-21.0.7_windows-x64_bin-sdk\\javafx-sdk-21.0.7\\lib",
                        mysqlpath
                    );
                    break;
                case "4":
                    System.out.println("Exiting...");
                    scanner.close();
                    return;
                default:
                    System.out.println("Invalid choice. Try again.");
            }
        }
    }

    private static void runJavaClass(String className) {
        try {
            ProcessBuilder pb = new ProcessBuilder("java", className);
            pb.inheritIO();
            Process p = pb.start();
            p.waitFor();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error running class: " + e.getMessage());
        }
    }

    private static void runJavaClassWithDependency(String className, String ojdbcJarPath) {
        try {
            ProcessBuilder pb = new ProcessBuilder(
                "java",
                "-cp", ".;" + ojdbcJarPath,
                className
            );
            pb.inheritIO();
            Process p = pb.start();
            p.waitFor();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error running JDBC class: " + e.getMessage());
        }
    }

    private static void runJavaFXWithDependency(String className, String javafxLibPath, String ojdbcJarPath) {
        try {
            String classpath = ".;" + ojdbcJarPath;
            ProcessBuilder pb = new ProcessBuilder(
                "java",
                "--module-path", javafxLibPath,
                "--add-modules", "javafx.controls,javafx.fxml",
                "-cp", classpath,
                className
            );
            pb.inheritIO();
            Process p = pb.start();
            p.waitFor();
        } catch (IOException | InterruptedException e) {
            System.out.println("Error running JavaFX class: " + e.getMessage());
        }
    }
}