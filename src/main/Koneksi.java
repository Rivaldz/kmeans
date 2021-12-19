package main;

import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.SQLException;  

public class Koneksi {
  // public Koneksi() {
  // }
  private Connection connect;  
  private String driverName = "org.postgresql.Driver"; // Driver Untuk Koneksi Ke PostgreSQL  
  // private String driverName = "kluster"; // Driver Untuk Koneksi Ke PostgreSQL  
  private String jdbc = "jdbc:postgresql://";  
  private String host = "localhost:"; // Host ini Bisa Menggunakan IP Anda, Contoh : 192.168.100.100  
  private String port = "5432/"; // Port Default PostgreSQL  
  private String database = "dataset"; // Ini Database yang akan digunakan  
  private String url = jdbc + host + port + database;  
  private String username = ""; //  
  private String password = "";  

  public Connection getKoneksi() throws SQLException {  
    if (connect == null) {  
      try {  
        Class.forName(driverName);  
        System.out.println("Class Driver Ditemukan");  
        try {  
          connect = DriverManager.getConnection(url, username, password);  
          System.out.println("Koneksi Database Sukses");  
        } catch (SQLException se) {  
          System.out.println("Koneksi Database Gagal : " + se);  
          System.exit(0);  
        }  
      } catch (ClassNotFoundException cnfe) {  
        System.out.println("Class Driver Tidak Ditemukan, Terjadi Kesalahan Pada : " + cnfe);  
        System.exit(0);  
      }  
    }  
    return connect;  
  }  
}  