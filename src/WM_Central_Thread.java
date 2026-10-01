import java.lang.Exception;
import java.net.Socket;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.io.*;
import java.util.*;

public class WM_Central_Thread extends Thread {

    private Socket skCliente;

    public WM_Central_Thread(Socket skCliente) {
        this.skCliente = skCliente;
    }

    public String leeSocket(Socket skCliente, String Cadena){
        try{
            InputStream aux = skCliente.getInputStream();
            DataInputStream flujo = new DataInputStream(aux);
            Cadena = flujo.readUTF();
        }catch(EOFException e){
            return null;
        }catch(Exception e){
            System.out.println("Error" + e.toString());
        }

        return Cadena;
    }

    public void escribirSocket(Socket skCliente, String respuesta){
        try{
            OutputStream aux = skCliente.getOutputStream();
            DataOutputStream flujo = new DataOutputStream(aux);
            flujo.writeUTF(respuesta);
        }catch(Exception e){
            System.out.println("Error" + e.toString());
        }
        return;
    }

    public boolean validarCadena(String[] partes){
        return partes.length == 3 && partes[0].equals("REGISTRO");
    }

    public String[] splitear(String Cadena){
        return Cadena.split("#");
    }

    public static String buscarEstadoID_BBDD(String id){
        var url = "jdbc:sqlite:/home/usuario/Documentos/Curso-26-27/SD/Practica/Water-Management/WM.db";
        String sql = "SELECT estado FROM aspersores WHERE id = ?";

        try (var conn = DriverManager.getConnection(url);
            var pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, id);

            try (var rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("estado"); // "DESCONECTADA", "CONECTADA", etc.
                } else {
                    return null; // el id no existe en la BD
                }
            }

        } catch (SQLException e) {
            System.err.println("ERROR consultando BD: " + e.getMessage());
            return null;
        }
    }

    public static void updateConexion_BBDD(String estado, String id){
        var url = "jdbc:sqlite:/home/usuario/Documentos/Curso-26-27/SD/Practica/Water-Management/WM.db";
        var sql = "UPDATE aspersores SET estado = ? WHERE id = ?";

        try(var conn = DriverManager.getConnection(url); var pstmt = conn.prepareStatement(sql)){
            pstmt.setString(1, estado);
            pstmt.setString(2, id);

            int filas = pstmt.executeUpdate();
            if (filas == 0) {
                System.out.println("Aviso: no se actualizó ninguna fila para id=" + id);
            }

        }catch (SQLException e) {
            System.err.println(e.getMessage());
        }
    }

    public static void setOperarioActual(String id, String operador){
        var url = "jdbc:sqlite:/home/usuario/Documentos/Curso-26-27/SD/Practica/Water-Management/WM.db";
        var sql = "UPDATE aspersores SET operario_actual = ? WHERE id = ?";

        try(var conn = DriverManager.getConnection(url); var pstmt = conn.prepareStatement(sql)){
            pstmt.setString(1, operador);
            pstmt.setString(2, id);
            pstmt.executeUpdate();
        }catch (SQLException e) {
            System.err.println("Error actualizando operario_actual: " + e.getMessage());
        }
    }

    public static String getOperarioActual(String id_estacion) {
        String url = "jdbc:sqlite:/home/usuario/Documentos/Curso-26-27/SD/Practica/Water-Management/WM.db";
        String sql = "SELECT operario_actual FROM aspersores WHERE id = ?";
        try (var conn = DriverManager.getConnection(url);
            var pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, id_estacion);
            try (var rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString("operario_actual");
                }
                return null;
            }
        } catch (SQLException e) {
            System.err.println("Error leyendo operario_actual: " + e.getMessage());
            return null;
        }
    }

    public static boolean validarOperator(String id_operario){

        String url = "jdbc:sqlite:/home/usuario/Documentos/Curso-26-27/SD/Practica/Water-Management/WM.db";
        String sql = "SELECT id from operarios where id = ?";

        try(var conn = DriverManager.getConnection(url); var pstmt = conn.prepareStatement(sql)){

            pstmt.setString(1, id_operario);

            try (var rs = pstmt.executeQuery()) {
                return rs.next();   // Si no hay fila devuelve false, si no trues
            }

        } catch (SQLException e) {
            System.err.println("Error leyendo operario_actual: " + e.getMessage());
            return false;
        }
    }

    /*
        public static void registrar_operario(String id_operario, String nombre){
            String url = "jdbc:sqlite:/home/usuario/Documentos/Curso-26-27/SD/Practica/Water-Management/WM.db";
            String sql = "INSERT INTO operarios (id, nombre) VALUES (?, ?)";

            try(var conn = DriverManager.getConnection(url); var pstmt = conn.prepareStatement(sql)){

                pstmt.setString(1, id_operario);
                pstmt.setString(2, nombre);
                pstmt.executeQuery();

            }catch (SQLException e) {
                System.err.println("Error leyendo operario_actual: " + e.getMessage());
            }
        }
    */
    

    public void run(){
        String Cadena = "";
        String respuesta = "";
        String registro = "";
        String id_estacion = "";
        String ubicacion = "";

        try{
            Cadena = this.leeSocket(skCliente, Cadena);

            String[] partes = this.splitear(Cadena);

            if(!validarCadena(partes)){
                respuesta = "STATUS#ERROR#Formato de trama invalido";
                System.out.println("Trama invalida recibida: " + Cadena);
                this.escribirSocket(skCliente, respuesta);
                skCliente.close();
                return;
            }

            String estado = buscarEstadoID_BBDD(partes[1]);

            if(estado == null){
                respuesta = "STATUS#ERROR#Ningún aspersor registrado en la bbdd con ese ID";
                this.escribirSocket(skCliente, respuesta); 
                skCliente.close();
            }
            else if(!estado.equals("DESCONECTADA")){
                respuesta = "STATUS#ERROR#El aspersor ya estaba conectado";
                this.escribirSocket(skCliente, respuesta); 
                skCliente.close();
            }
            else{
                registro = partes[0];
                id_estacion = partes[1];
                ubicacion = partes[2];
                System.out.println(registro + " ID_ESTACION: " + id_estacion + ", UBICACIÓN: " + ubicacion);
                respuesta = "STATUS#OK#Estacion: " + ubicacion + " registrada correctamente";
                escribirSocket(skCliente, respuesta);
                //estado = "DISPONIBLE";
                //updateConexion_BBDD(estado, id_estacion);
                
                for(;;){
                    Cadena = "";
                    Cadena = this.leeSocket(skCliente, Cadena);
                    System.out.println("Cadena: " + Cadena);
                    if(Cadena == null || Cadena.isEmpty()) {
                        System.out.println("La estación " + id_estacion + " se ha desconectado.");
                        updateConexion_BBDD("DESCONECTADA", id_estacion);
                        break;
                    }
                    else if(Cadena.contains("CONNECT")){
                        String[] partesAlerta = this.splitear(Cadena);
                        System.out.println(partesAlerta[2]);
                        if(partesAlerta[2].equals("DISPONIBLE")){
                            System.out.println("MODO DISPONIBLE " + id_estacion);
                            updateConexion_BBDD("DISPONIBLE", id_estacion);
                            this.escribirSocket(skCliente, "ACK#DISPONIBILIDAD_RECIBIDA");
                        }
                        else if(partesAlerta[2].equals("FUERA_DE_SERVICIO")){
                            System.out.println("MODO FUERA DE SERVICIO " + id_estacion);
                            updateConexion_BBDD("FUERA_DE_SERVICIO", id_estacion);
                            this.escribirSocket(skCliente, "ACK#BLOQUEO_RECIBIDO");
                        }
                    }
                    else if(Cadena.contains("ALERT")){
                        String[] partesAlerta = this.splitear(Cadena);
                        // Trama esperada = ALERT#id#FUGA
                            if(partesAlerta[2].equals("FUGA")){
                            System.out.println("ALERTA DE FUGA Estación: " + id_estacion);
                            updateConexion_BBDD("FUGA", id_estacion);
                            this.escribirSocket(skCliente, "ACK#FUGA_RECIBIDA");
                        }
                        else if(partesAlerta[2].equals("DESCONECTADA")){
                            System.out.println("ALERTA DE DESCONEXIÓN Estación: " + id_estacion);
                            updateConexion_BBDD("DESCONECTADA", id_estacion);
                        }
                    }

                }
                skCliente.close();

            }

            //this.escribirSocket(skCliente, respuesta);

            
        }catch(Exception e){
            System.out.println("Error: " + e.toString());
        }

    }

}