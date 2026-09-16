import java.net.*;
import java.util.*;
import java.sql.DriverManager;
import java.sql.SQLException;

public class WM_Central{


    public static void createTable(){
        var url = "jdbc:sqlite://localhost:3306";

        var sql = "CREATE TABLE IF NOT EXISTS WateringStations ("
                + "	id INTEGER PRIMARY KEY,"
                + "	estado text NOT NULL,"
                + "	ocupado INTEGER"
                + ");";
        try (var conn = DriverManager.getConnection(url);
                var stmt = conn.createStatement()) {
            // create a new table
            stmt.execute(sql);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }



    public static void main(String args[]){

        String Cadena = "";
        String puerto_Servidor = "";

        try{
            if(args.length < 1){
            System.out.println("Error. Número de argumentos inválidos");
            System.out.println("$./Servidor <Puerto_Servidor>");
            System.exit(1);
            }

            puerto_Servidor = args[0];


            createTable();

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