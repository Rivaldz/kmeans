package main;

import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;

public class Koneksi {
  // public Koneksi() {
  // }
  private Connection connect;
  private Statement stmt = null;
  private String driverName = "org.postgresql.Driver"; // Driver Untuk Koneksi Ke PostgreSQL  
  // private String driverName = "kluster"; // Driver Untuk Koneksi Ke PostgreSQL  
  private String jdbc = "jdbc:postgresql://";  
  private String host = "localhost:"; // Host ini Bisa Menggunakan IP Anda, Contoh : 192.168.100.100  
  private String port = "5432/"; // Port Default PostgreSQL  
  private String database = "dataset"; // Ini Database yang akan digunakan  
  private String url = jdbc + host + port + database;  
  private String username = "postgres"; //  
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
  public void queryData(){
    try {
      stmt = connect.createStatement();
      String sql = "SELECT * FROM vw_hasil_normalisasi_min_max_latihan";
      ResultSet rs = stmt.executeQuery(sql);
      while (rs.next()) {
        int id = rs.getInt("tbmm_id");
        Float frekuensi= rs.getFloat("normalisasi_frekuensi");
        Float total = rs.getFloat("normalisasi_total");
        // int age  = rs.getInt("age");
        // String  address = rs.getString("address");
        // float salary = rs.getFloat("salary");
        System.out.println( "ID = " + id );
        System.out.println( "NAME = " + frekuensi);
        System.out.println( "AGE = " + total );
        // System.out.println( "ADDRESS = " + address );
        // System.out.println( "SALARY = " + salary );
        System.out.println(); 
      }
      rs.close();
      stmt.close();
      connect.close();
    } catch (SQLException e) {
      System.err.println( e.getClass().getName()+": "+ e.getMessage() );
      System.exit(0);

    }
    System.out.println("Operation done successfully");


  }
}  