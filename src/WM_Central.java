import java.net.*;
import java.util.*;
import java.sql.DriverManager;
import java.sql.SQLException;

public class WM_Central{

    public static void createTable(){
        String url = "jdbc:sqlite:WM.db";
        String sql = "CREATE TABLE IF NOT EXISTS aspersores (" +
                    "id TEXT PRIMARY KEY, " +
                    "estado TEXT, " +
                    "ocupado INTEGER)";

        try (var conn = DriverManager.getConnection(url);
            var stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Tabla 'aspersores' lista.");
        } catch (SQLException e) {
            System.err.println("ERROR creando tabla: " + e.getMessage());
        }
    }

    public static void insertarDatosPrueba(){
        String url = "jdbc:sqlite:WM.db";
        String sql = "INSERT OR IGNORE INTO aspersores (id, estado, ocupado) VALUES (?, ?, ?)";

        String[][] datos = {
            {"WS-01", "DESCONECTADA", "0"},
            {"WS-02", "DESCONECTADA", "0"},
            {"WS-03", "DESCONECTADA", "0"},
            {"WS-04", "DESCONECTADA", "0"},
            {"WS-05", "DESCONECTADA", "0"}
        };

        try (var conn = DriverManager.getConnection(url);
            var pstmt = conn.prepareStatement(sql)) {

            for (String[] fila : datos) {
                pstmt.setString(1, fila[0]);
                pstmt.setString(2, fila[1]);
                pstmt.setInt(3, Integer.parseInt(fila[2]));
                pstmt.executeUpdate();
            }
            System.out.println("Datos de prueba insertados.");

        } catch (SQLException e) {
            System.err.println("ERROR insertando datos: " + e.getMessage());
        }
    }

    public static void main(String args[]){

        
        String puerto_Servidor = "";

        try{
            if(args.length < 1){
            System.out.println("Error. Número de argumentos inválidos");
            System.out.println("$./Servidor <Puerto_Servidor>");
            System.exit(1);
            }

            puerto_Servidor = args[0];


            createTable();
            insertarDatosPrueba();

            ServerSocket skServidor = new ServerSocket(Integer.parseInt(puerto_Servidor));
            System.out.println("Escucho el puerto " + puerto_Servidor);

            for(;;){
                Socket skCliente = skServidor.accept();
                System.out.println("Esperando cliente...");

                Thread t = new WM_Central_Thread(skCliente);
                t.start();
            }

        
        }catch(Exception e){
            System.out.println("Error: " + e.toString());
        }
        


    }
}