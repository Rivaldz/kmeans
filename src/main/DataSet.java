package main;

import java.io.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;

public class DataSet {

    static class Record{
        HashMap<String, Double> record;
        Integer clusterNo;

        public Record(HashMap<String, Double> record){
            this.record = record;
        }

        public void setClusterNo(Integer clusterNo) {
            this.clusterNo = clusterNo;
        }

        public HashMap<String, Double> getRecord() {
            return record;
        }
    }

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
    private String csvFilePathInsert = "files/sampleClustered.csv";
    private int batchSize = 20;
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

    private final LinkedList<String> attrNames = new LinkedList<>();
    private final LinkedList<Record> records = new LinkedList<>();
    private final LinkedList<Integer> indicesOfCentroids = new LinkedList<>();
    private final HashMap<String, Double> minimums = new HashMap<>();
    private final HashMap<String, Double> maximums = new HashMap<>();
    private static final Random random = new Random();

    public DataSet(String csvFileName) throws IOException, SQLException {
    getKoneksi();
    
    attrNames.push("normalisasi_total");
    attrNames.push("normalisasi_frekuensi");
    attrNames.push("Class");

    try {
      stmt = connect.createStatement();
      String sql = "SELECT * FROM vw_hasil_normalisasi_min_max_latihan";

      String sqlMinClass = "SELECT MIN (tbmm_id)FROM vw_hasil_normalisasi_min_max_latihan;";
      String sqlMinFrekuensi = "SELECT MIN (normalisasi_frekuensi)FROM vw_hasil_normalisasi_min_max_latihan;";
      String sqlMinTotal = "SELECT MIN (normalisasi_total)FROM vw_hasil_normalisasi_min_max_latihan;";

      String sqlMaxClass = "SELECT MAX (tbmm_id)FROM vw_hasil_normalisasi_min_max_latihan;";
      String sqlMaxFrekuensi = "SELECT MAX (normalisasi_frekuensi)FROM vw_hasil_normalisasi_min_max_latihan;";
      String sqlMaxTotal = "SELECT MAX (normalisasi_total)FROM vw_hasil_normalisasi_min_max_latihan;";

      ResultSet rs = stmt.executeQuery(sql);

    //   ResultSet rsMin = stmt.executeQuery(sqlMinClass);
    //   while (rsMin.next()) {
    //     minimums.put("Class", rsMin.getDouble("min"));
    //   }
    //   ResultSet rsMinFrek = stmt.executeQuery(sqlMinFrekuensi);
    //   while(rsMinFrek.next()){
    //     minimums.put("normalisasi_frekuensi", rsMinFrek.getDouble("min"));
    //   }
    //   ResultSet rsMinTot = stmt.executeQuery(sqlMinTotal);
    //   while (rsMinTot.next()) {
    //     minimums.put("normalisasi_total", rsMinTot.getDouble("min"));
    //   }

    //   ResultSet rsMax = stmt.executeQuery(sqlMaxClass);
    //   while(rsMax.next()){
    //     maximums.put("Class", rsMax.getDouble("max"));
    //   }
    //   ResultSet rsMaxFrek = stmt.executeQuery(sqlMaxFrekuensi);
    //   while (rsMaxFrek.next()) {
    //     maximums.put("normalisasi_frekuensi", rsMaxFrek.getDouble("max"));
    //   }
    //   ResultSet rsMaxTot = stmt.executeQuery(sqlMaxTotal);
    //   while (rsMaxTot.next()) {
    //     maximums.put("normalisasi_total",rsMaxTot.getDouble("max") );
    //   }
    //   BufferedWriter fileWriter = new BufferedWriter(new FileWriter(csvFilePath));
    //   fileWriter.write("Class,normalisasi_frekuensi,normalisasi_total");
       int size = 0;
      while (rs.next()) {
      HashMap<String, Double> record = new HashMap<>();
        //   rs.last();
        //   size = rs.getRow();
        //   System.out.println(" Panjang roww" + size);
        //   for (int i = 1; i < size ; i++) {
                 
        //   }
          int id = rs.getInt("tbmm_id");
          Double frekuensi= rs.getDouble("normalisasi_frekuensi");
          Double total = rs.getDouble("normalisasi_total");

        //   String line = String.format("%d,%f,%f",
        //                 id, frekuensi, total);
        //   fileWriter.newLine();
        //   fileWriter.write(line);

          System.out.println("Lihat Rs");

          System.out.println( "class = " + id );
          System.out.println( "normalisasi frekuensi  = " + frekuensi);
          System.out.println( "normalisasi total = " + total );
          System.out.println();
            
            Double idCast = (double) id;
            record.put("Class", idCast);
            record.put("normalisasi_frekuensi", frekuensi);
            record.put("normalisasi_total", total);
            
            updateMin("Class", idCast);
            updateMin("normalisasi_frekuensi", frekuensi);
            updateMin("normalisasi_total", total);

            updateMax("Class", idCast);
            updateMax("normalisasi_frekuensi", frekuensi);
            updateMax("normalisasi_total", total);

            // minimums.put("Class", idCast);
            // minimums.put("normalisasi_frekuensi", frekuensi);
            // minimums.put("normalisasi_total", total);

            // maximums.put("Class", idCast);
            // maximums.put("normalisasi_frekuensi", frekuensi);
            // maximums.put("normalisasi_total", total);
 
            // updateMin("Class", idCast);
            // updateMin("normalisasi_frekuensi", frekuensi);
            // updateMin("normalisasi_total", total);

            // updateMax("Class", idCast);
            // updateMax("normalisasi_frekuensi", frekuensi);
            // updateMax("normalisasi_total", total);
       // System.out.println("record --> " + name + "[]" + val );
        // updateMin(name, val);
        // System.out.println("update Min " + name + "[]" + val );
        // updateMax(name, val);
        // System.out.println("update Max " + name + "[]" + val );

        records.add(new Record(record));
        }

      // rs.close();
      // stmt.close();
      // connect.close();
    //   fileWriter.close();

    } catch (SQLException e) {
      System.err.println( e.getClass().getName()+": "+ e.getMessage() );
      System.exit(0);

    }

       /* String row;
       try(BufferedReader csvReader = new BufferedReader(new FileReader(csvFileName))) {
            if((row = csvReader.readLine()) != null){
                String[] data = row.split(",");
                Collections.addAll(attrNames, data);
            }

            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");

                HashMap<String, Double> record = new HashMap<>();
                System.out.println("----------------------------");

                if(attrNames.size() == data.length) {
                    for (int i = 0; i < attrNames.size(); i++) {
                        String name = attrNames.get(i);
                        double val = Double.parseDouble(data[i]);
                        record.put(name, val);
                        System.out.println("record --> " + name + "[]" + val );
                        updateMin(name, val);
                        System.out.println("update Min " + name + "[]" + val );
                        updateMax(name, val);
                        System.out.println("update Max " + name + "[]" + val );
                    }
                } else{
                    throw new IOException("Incorrectly formatted file.");
                }

                records.add(new Record(record));
            }

        }
        showHasilHash(); */
        // for (int i = 0; i < minimums.size(); i++) {
        //    System.out.println("Lihat MINIMUS mas brow " + minimums.get(i)); 
        // }
            

        showHasilHash();
    }

    public void createCsvOutput(String outputFileName){

        try(BufferedWriter csvWriter = new BufferedWriter(new FileWriter(outputFileName))) {
            for(int i=0; i<attrNames.size(); i++){
                csvWriter.write(attrNames.get(i));
                csvWriter.write(",");
            }

            csvWriter.write("ClusterId");
            csvWriter.write("\n");
            System.out.println("20. Cek ini"+records.getFirst().getRecord());
            for(var record : records){
                for(int i=0; i<attrNames.size(); i++){
                    csvWriter.write(String.valueOf(record.getRecord().get(attrNames.get(i))));
                    csvWriter.write(",");
                }
                csvWriter.write(String.valueOf(record.clusterNo));
                csvWriter.write("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void updateMin(String name, Double val){
        if(minimums.containsKey(name)){
            if(val < minimums.get(name)){
                minimums.put(name, val);
            }
        } else{
            minimums.put(name, val);
        }


    }

    private void updateMax(String name, Double val){
        if(maximums.containsKey(name)){
            if(val > maximums.get(name)){
                maximums.put(name, val);
            }
        } else{
            maximums.put(name, val);
        }
    }
    private void showHasilHash(){
        minimums.forEach((K,V) -> System.out.println(K + ", Minimus : " + V));
        maximums.forEach((K,V) -> System.out.println(K + ", Maximus : " + V));
        System.out.println("this of record RRRRRRR");
         for (int i = 0; i < records.size(); i++) {
           System.out.println("Lihat RECORD mas brow " + records.get(i).getRecord()); 
        }

        System.out.println("this of attrAAAAAAA");
        for (int i = 0; i < attrNames.size(); i++) {
           System.out.println("Lihat ATTR mas brow " + attrNames.get(i)); 
        }
        System.out.println("this of indicesSSSSS");
        for (int i = 0; i < indicesOfCentroids.size(); i++) {
           System.out.println("Lihat indicessss mas brow " + indicesOfCentroids.get(i)); 
        }
    }

    public Double meanOfAttr(String attrName, LinkedList<Integer> indices){
        Double sum = 0.0;
        for(int i : indices){
            if(i<records.size()){
                sum += records.get(i).getRecord().get(attrName);
            }
        }
        return sum / indices.size();
    }

    public HashMap<String, Double> calculateCentroid(int clusterNo){
        HashMap<String, Double> centroid = new HashMap<>();

        LinkedList<Integer> recsInCluster = new LinkedList<>();
        for(int i=0; i<records.size(); i++){
            var record = records.get(i);
            if(record.clusterNo == clusterNo){
                recsInCluster.add(i);
            }
        }

        for(String name : attrNames){
            centroid.put(name, meanOfAttr(name, recsInCluster));
        }
        return centroid;
    }

    public LinkedList<HashMap<String,Double>> recomputeCentroids(int K){
        LinkedList<HashMap<String,Double>> centroids = new LinkedList<>();
        for(int i=0; i<K; i++){
            centroids.add(calculateCentroid(i));
        }
        return centroids;
    }

    public void removeAttr(String attrName){
        if(attrNames.contains(attrName)){
            attrNames.remove(attrName);

            for(var record : records){
                record.getRecord().remove(attrName);
            }

            minimums.remove(attrName);

            maximums.remove(attrName);
        }

    }

    public HashMap<String, Double> randomDataPoint(){
        HashMap<String, Double> res = new HashMap<>();

        for(String name : attrNames){
            Double min = minimums.get(name);
            Double max = maximums.get(name);
            res.put(name, min + (max-min) * random.nextDouble());
        }

        return res;
    }

    public HashMap<String, Double> randomFromDataSet(){
        int index = random.nextInt(records.size());
        return records.get(index).getRecord();
    }

    public static Double euclideanDistance(HashMap<String, Double> a, HashMap<String, Double> b){
        if(!a.keySet().equals(b.keySet())){
            return Double.POSITIVE_INFINITY;
        }

        double sum = 0.0;

        for(String attrName : a.keySet()){
            sum += Math.pow(a.get(attrName) - b.get(attrName), 2);
        }
        System.out.println("14. Math.sqrt(sum)=="+Math.sqrt(sum));

        return Math.sqrt(sum);
    }

    public Double calculateClusterSSE(HashMap<String, Double> centroid, int clusterNo){
        double SSE = 0.0;
        for(int i=0; i<records.size(); i++){
            if(records.get(i).clusterNo == clusterNo){
                SSE += Math.pow(euclideanDistance(centroid, records.get(i).getRecord()), 2);
            }
        }
        return SSE;
    }

    public Double calculateTotalSSE(LinkedList<HashMap<String,Double>> centroids){
        Double SSE = 0.0;
        for(int i=0; i<centroids.size(); i++) {
            SSE += calculateClusterSSE(centroids.get(i), i);
            //System.out.println("13. SSE ====="+i+"." +SSE);
        }
        return SSE;
    }

    public HashMap<String,Double> calculateWeighedCentroid(){
        double sum = 0.0;

        for(int i=0; i<records.size(); i++){
            if(!indicesOfCentroids.contains(i)){
                double minDist = Double.MAX_VALUE;
                for(int ind : indicesOfCentroids){
                    double dist = euclideanDistance(records.get(i).getRecord(), records.get(ind).getRecord());
                    if(dist<minDist)
                        minDist = dist;
                }
                if(indicesOfCentroids.isEmpty())
                    sum = 0.0;
                sum += minDist;
            }
        }

        double threshold = sum * random.nextDouble();

        for(int i=0; i<records.size(); i++){
            if(!indicesOfCentroids.contains(i)){
                double minDist = Double.MAX_VALUE;
                for(int ind : indicesOfCentroids){
                    double dist = euclideanDistance(records.get(i).getRecord(), records.get(ind).getRecord());
                    if(dist<minDist)
                        minDist = dist;
                }
                sum += minDist;

                if(sum > threshold){
                    indicesOfCentroids.add(i);
                    return records.get(i).getRecord();
                }
            }
        }

        return new HashMap<>();
    }

    public LinkedList<String> getAttrNames() {
        return attrNames;
    }

    public LinkedList<Record> getRecords() {
        return records;
    }

    public Double getMin(String attrName){
        return minimums.get(attrName);
    }

    public Double getMax(String attrName){
        return maximums.get(attrName);
    }
}
