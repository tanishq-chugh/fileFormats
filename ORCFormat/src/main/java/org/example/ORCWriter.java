package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.OrcFile;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class ORCWriter {

    public static void main(String[] args) throws IOException {

        Configuration config = new Configuration();
        TypeDescription Tableschema = TypeDescription.fromString("struct<ID:int,Name:string,City:string,Company:string>");
        Writer writer = OrcFile.createWriter(new Path("output2.orc"),
                OrcFile.writerOptions(config).setSchema(Tableschema));

        VectorizedRowBatch batch = Tableschema.createRowBatch();
        LongColumnVector id_col = (LongColumnVector) batch.cols[0];
        BytesColumnVector name_col = (BytesColumnVector) batch.cols[1];
        BytesColumnVector city_col = (BytesColumnVector) batch.cols[2];
        BytesColumnVector cmp_col = (BytesColumnVector) batch.cols[3];


        String[] names = new String[]{"Tanishq","Aniket","Sanjana","Inayat","Raunak","Amruth"};
        String[] cities = new String[]{"JPR","GGN","BLR","PAT","DEL","BLR"};
        String[] companies = new String[]{"Cloudera","Cloudera","Cloudera","Cloudera","Cloudera","Cloudera"};



        for(int i=0;i<names.length;i++){
            int row = batch.size++;
            id_col.vector[row]=i+1;
            name_col.setVal(row,names[i].getBytes(StandardCharsets.UTF_8));
            city_col.setVal(row,cities[i].getBytes(StandardCharsets.UTF_8));
            cmp_col.setVal(row,companies[i].getBytes(StandardCharsets.UTF_8));
            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }

        if (batch.size != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
        writer.close();
        System.out.println("Done");
    }
}
