import java.net.*;
import java.util.*;

public class WM_Central{


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