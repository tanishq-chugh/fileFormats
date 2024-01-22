package org.example;
import avroformats.Intern;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class AvroTableWriter {
    public static void main(String[] args) {

        String[] names = new String[]{"Tanishq","Aniket","Sanjana","Inayat","Raunak","Amruth"};
        String[] cities = new String[]{"JPR","GGN","BLR","PAT","DEL","BLR"};
        String[] companies = new String[]{"Cloudera","Cloudera","Cloudera","Cloudera","Cloudera","Cloudera"};

        ArrayList<Intern> allInterns = new ArrayList<>();
        for(int i=0;i<names.length;i++){
            Intern curr = new Intern(i+1,names[i],cities[i],companies[i]);
            allInterns.add(curr);
        }

        DatumWriter<Intern> internDatumWriter = new SpecificDatumWriter<>(Intern.class);

        try (
                DataFileWriter<Intern> dfw = new DataFileWriter<Intern>(internDatumWriter)) {
                dfw.create(allInterns.get(0).getSchema(), new File("output2.avro"));
                for(int i=0;i<allInterns.size();i++){
                    dfw.append(allInterns.get(i));
                }
                System.out.println("Done");
        } catch (IOException e) {

            e.printStackTrace();

        }
    }
}
