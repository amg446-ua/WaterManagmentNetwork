import java.net.*;
import java.util.*;
import java.sql.DriverManager;
import java.sql.SQLException;

public class WM_Central{

    public static void createTable(){
        String url = "jdbc:sqlite:/app/data/WM.db";
        String sql = "CREATE TABLE IF NOT EXISTS aspersores (" +
                    "id TEXT PRIMARY KEY, " +
                    "estado TEXT, " +
                    "operario_actual TEXT, " +
                    "caudal_actual REAL DEFAULT 0, " + 
                    "volumen_actual REAL DEFAULT 0, " +
                    "ubicacion TEXT)";

        try (var conn = DriverManager.getConnection(url);
            var stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Tabla 'aspersores' lista");
        } catch (SQLException e) {
            System.err.println("ERROR creando tabla: " + e.getMessage());
        }
    }

    public static void createOperatorTable(){
        String url = "jdbc:sqlite:/app/data/WM.db";
        String sql = "CREATE TABLE IF NOT EXISTS operarios (" + 
                    "id TEXT PRIMARY KEY, " + 
                    "nombre TEXT)";

        try(var conn = DriverManager.getConnection(url); var stmt = conn.createStatement()){
            stmt.execute(sql);
            System.out.println("Tabla 'operarios' lista");
        }catch(SQLException e){
            System.err.println("ERROR creanto tabla: " + e.getMessage());
        }
    }

    public static void insertartOperarios(){
        String url = "jdbc:sqlite:/app/data/WM.db";
        String sql = "INSERT OR IGNORE INTO operarios (id, nombre) VALUES (?, ?)";

        String[][] datos = {
            {"FO-01", "Adrián Muñoz"},
            {"FO-02", "Javier Rentero"},
            {"FO-03", "Fran Sol"}
        };

        try(var conn = DriverManager.getConnection(url); var pstmt = conn.prepareStatement(sql)){

            for(String[] dato : datos){
                pstmt.setString(1, dato[0]);
                pstmt.setString(2, dato[1]);
                pstmt.executeUpdate();
            }

            System.out.println("Datos de prueba de operarios insertados");

        }catch(SQLException e){
            System.err.println("ERROR insertando datos " + e.getMessage());
        }
    }

    public static void insertarDatosPrueba(){
        String url = "jdbc:sqlite:/app/data/WM.db";
        String sql = "INSERT OR IGNORE INTO aspersores (id, estado, operario_actual, caudal_actual, volumen_actual, ubicacion) VALUES (?, ?, ?, ?, ?, ?)";

        String[][] datos = {
            {"WS-01", "DESCONECTADA", "NULL", "-"},
            {"WS-02", "DESCONECTADA", "NULL", "-"},
            {"WS-03", "DESCONECTADA", "NULL", "-"},
            {"WS-04", "DESCONECTADA", "NULL", "-"},
            {"WS-05", "DESCONECTADA", "NULL", "-"}
        };

        try (var conn = DriverManager.getConnection(url);
            var pstmt = conn.prepareStatement(sql)) {

            for (String[] fila : datos) {
                pstmt.setString(1, fila[0]);
                pstmt.setString(2, fila[1]);
                if (fila[2].equals("NULL")) {
                    pstmt.setNull(3, java.sql.Types.VARCHAR);
                } else {
                    pstmt.setString(3, fila[2]);
                }
                pstmt.setDouble(4, 0.0);
                pstmt.setDouble(5, 0.0); 
                pstmt.setString(6, fila[3]); 
                pstmt.executeUpdate();
            }
            System.out.println("Datos de prueba insertados.");

        } catch (SQLException e) {
            System.err.println("ERROR insertando datos: " + e.getMessage());
        }
    }

    public static void inicializarEstados(){
        String url = "jdbc:sqlite:/app/data/WM.db";
        var sql = "UPDATE aspersores SET estado = 'DESCONECTADA'";
        try (var conn = DriverManager.getConnection(url);
            var pstmt = conn.prepareStatement(sql)) {
            pstmt.executeUpdate();
            //System.out.println(filas + " estaciones marcadas como DESCONECTADA al arrancar CENTRAL.");
        } catch (SQLException e) {
            System.err.println("Error inicializando estados: " + e.getMessage());
        }
    }


    public static void main(String args[]){

        String puerto_Servidor = "";
        String broker = "";

        try{
            if(args.length < 2){
                System.out.println("Error. Número de argumentos inválidos");
                System.out.println("$./Servidor <Puerto_Servidor> <Broker_Kafka>");
                System.exit(1);
            }

            puerto_Servidor = args[0];
            broker = args[1];

            createTable();
            insertarDatosPrueba();
            inicializarEstados();

            createOperatorTable();
            insertartOperarios();

            ServerSocket skServidor = new ServerSocket(Integer.parseInt(puerto_Servidor));
            System.out.println("Escucho el puerto " + puerto_Servidor);

            new KafkaConsumer_Thread(broker).start();
            new KafkaPeticiones_Thread(broker).start();
            new MenuOperador_Thread(broker).start();

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