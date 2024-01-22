package org.example;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class ParquetFileWriter{

    public static void main(String[] args) {

        String SchemaInAvro = "{\"type\": \"record\", \"name\": \"Interns\"," +
                "\"fields\": [" +
                "{\"name\": \"ID\", \"type\": \"int\"}," +
                "{\"name\": \"Name\", \"type\": \"string\"}," +
                "{\"name\": \"City\",\"type\": \"string\"}," +
                "{\"name\": \"Company\",\"type\": \"string\"}" +
                "]"+
                "}";

        Schema avroSchema = new Schema.Parser().parse(SchemaInAvro);
        String outputFilePath = "Output3.parquet";


        try {
            AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<>(
                    new Path(outputFilePath),
                    avroSchema,
                    CompressionCodecName.SNAPPY,
                    AvroParquetWriter.DEFAULT_BLOCK_SIZE,
                    AvroParquetWriter.DEFAULT_PAGE_SIZE
            );

            String[] names = new String[]{"Tanishq","Aniket","Sanjana","Inayat","Raunak","Amruth"};
            String[] cities = new String[]{"JPR","GGN","BLR","PAT","DEL","BLR"};
            String[] companies = new String[]{"Cloudera","Cloudera","Cloudera","Cloudera","Cloudera","Cloudera"};

            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
            for(int i=0;i<names.length;i++){
                recordBuilder.set("ID",i+1);
                recordBuilder.set("Name", names[i]);
                recordBuilder.set("City",cities[i]);
                recordBuilder.set("Company",companies[i]);
                GenericRecord curr = recordBuilder.build();
                writer.write(curr);
            }

            writer.close();
            System.out.println("Parquet File saved.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
