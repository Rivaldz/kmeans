package main;

import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.io.*;

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
  private String csvFilePath = "files/sample.csv";

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
  public void queryData() throws IOException{
    System.out.println("Mengambil Data Dari Database");  
    try {
      stmt = connect.createStatement();
      String sql = "SELECT * FROM vw_hasil_normalisasi_min_max_latihan";
      ResultSet rs = stmt.executeQuery(sql);
      BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFilePath));
      fileWriter.write("Class,normalisasi_frekuensi,normalisasi_total");

      while (rs.next()) {
        int id = rs.getInt("tbmm_id");
        Double frekuensi= rs.getDouble("normalisasi_frekuensi");
        Double total = rs.getDouble("normalisasi_total");

       
      String line = String.format("%d,%f,%f",
              id, frekuensi, total);
      fileWriter.newLine();
      fileWriter.write(line);

        System.out.println( "ID = " + id );
        System.out.println( "NAME = " + frekuensi);
        System.out.println( "AGE = " + total );
        System.out.println(); 
      }
      rs.close();
      stmt.close();
      connect.close();
      fileWriter.close();

    } catch (SQLException e) {
      System.err.println( e.getClass().getName()+": "+ e.getMessage() );
      System.exit(0);

    }
    System.out.println("Operation done successfully");

  }
}  