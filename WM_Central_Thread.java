import java.lang.Exception;
import java.net.Socket;
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
        String[] partes = Cadena.split("#");
        String op = partes.length > 0 ? partes[0] : "";
        String id_estacion = partes.length > 1 ? partes[1] : "";
        String ubicacion = partes.length > 2 ? partes[2] : "";
        return new String[]{op, id_estacion, ubicacion};
    }

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
            }
            else{
                registro = partes[0];
                id_estacion = partes[1];
                ubicacion = partes[2];
                System.out.println(registro + " ID_ESTACION: " + id_estacion + ", UBICACIÓN: " + ubicacion);
                respuesta = "STATUS#OK#Estacion: " + ubicacion + " registrada correctamente";
            }

            this.escribirSocket(skCliente, respuesta);

            skCliente.close();

        }catch(Exception e){
            System.out.println("Error" + e.toString());
        }

    }

}