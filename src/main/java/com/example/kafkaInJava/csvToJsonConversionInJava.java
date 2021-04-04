package com.example.kafkaInJava;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class csvToJsonConversionInJava {

    public static void main(String[] args) {

        //     readcsvusingScanner("C://Users//roshan_singh//IdeaProjects//Java_kafka_project//Data//demoname.csv");
        opencsvconversion("C://Users//roshan_singh//IdeaProjects//Java_kafka_project//Data//demoname.csv");
    }
    private static  void opencsvconversion(String filepath)  {


        try {
            CSVReader reader= new CSVReader(new FileReader(filepath),',','\'',1);
            String[] nextLine;

            while((nextLine= reader.readNext()) != null){
                Names name= new Names(nextLine[0],nextLine[1],Integer.valueOf(nextLine[2]));
//                for(String value : nextLine){
//                    System.out.print(value + "\t");
//                }
                System.out.println(new GsonBuilder().setPrettyPrinting().create().toJson(name));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
