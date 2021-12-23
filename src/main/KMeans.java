package main;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Scanner; 

import main.DataSet.Record;

public class KMeans {

    static final Double PRECISION = 0.0;

    /* K-Means++ implementation, initializes K centroids from data */
    static LinkedList<HashMap<String, Double>> kmeanspp(DataSet data, int K){
        LinkedList<HashMap<String,Double>> centroids = new LinkedList<>();

        centroids.add(data.randomFromDataSet());
        System.out.println("3. Cek nilai k"+K);
        System.out.println("4. Cek Centroid add==="+centroids);
        for(int i=1; i<K; i++){
            centroids.add(data.calculateWeighedCentroid());
            System.out.println("5. Cek Centroid add dalam for"+centroids);
        }

        return centroids;
    }

    /* K-Means itself, it takes a dataset and a number K and adds class numbers
    * to records in the dataset */
    static void kmeans(DataSet data, int K){
        // select K initial centroids
        LinkedList<HashMap<String,Double>> centroids = kmeanspp(data, K);
        System.out.println("1. Cek isi centroids==="+centroids);

        // initialize Sum of Squared Errors to max, we'll lower it at each iteration
        Double SSE = Double.MAX_VALUE;

        while (true) {
        	 //System.out.println("2. Masuk while===");
            // assign observations to centroids

            LinkedList<Record> records = data.getRecords();
            // for each record
            for(var record : records){
            	//System.out.println("6. liat get data==="+record.getRecord());//keluarin data yg di excel
                Double minDist = Double.MAX_VALUE;
                //System.out.println("7. Keluarin mindist==="+minDist);
                // find the centroid at a minimum distance from it and add the record to its cluster
                for(int i=0; i<centroids.size(); i++){
                	//System.out.println("8. get centroid i"+centroids.get(i));
                    Double dist = DataSet.euclideanDistance(centroids.get(i), record.getRecord());
                   // System.out.println("9. Liat ecludience ==="+dist);
                    if(dist<minDist){
                        minDist = dist;
                        record.setClusterNo(i);
                        //System.out.println("10. iiii"+record.getRecord());//Hasilnya sama seperti di excel/data source cuma duplicate dan yg puluhan satuan
                        //SAMPE SINI
                    }
                }

            }

            // recompute centroids   ;] to new cluster assignments
            centroids = data.recomputeCentroids(K);
            System.out.println("11. LIhat recompute centroid"+centroids);
            

            // exit condition, SSE changed less than PRECISION parameter
            Double newSSE = data.calculateTotalSSE(centroids);
            if(SSE-newSSE <= PRECISION){
                break;
            }
            SSE = newSSE;
            System.out.println("12. LIhat SSE===="+SSE);
        }
    }

    public static void main(String[] args) throws SQLException, IOException {
        Scanner myObj = new Scanner(System.in);  // Create a Scanner object
        System.out.print("Masukan Nilai K = ");
    
        int nilaiK = myObj.nextInt();  // Read user input
        System.out.println("Hasil Input Nilai K: " + nilaiK);  // Output user input

        Koneksi koneksi = new Koneksi();
        koneksi.getKoneksi();
        koneksi.queryData();

        try {
            // read data
            DataSet data = new DataSet("files/sample.csv");//buat baca nama file -> ini diubah jadi baca ke postgresql

            // remove prior classification attr if it exists (input any irrelevant attributes)
            data.removeAttr("Class");

            // cluster
            kmeans(data, nilaiK);//cluster 2 masih manual bisa dibuat inputan 2 3 4 5 dst.

            // output into a csv
            data.createCsvOutput("files/sampleClustered.csv");//output-> outputnya jadi tulis ke postgresql

        } catch (IOException e){
            e.printStackTrace();
        }

        koneksi.insertData();
    }

}
