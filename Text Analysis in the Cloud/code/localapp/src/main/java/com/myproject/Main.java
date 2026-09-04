package com.myproject;

import com.myproject.common.AWS;
import com.myproject.common.s3_debuge;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello, Local Application!");

        AWS aws = AWS.getInstance();
        aws.createBucketIfNotExists(aws.bucketName);
        new s3_debuge(aws.getS3Client(), aws.bucketName, "logs/app-log.txt").createFileIfNotExists();
        new s3_debuge(aws.getS3Client(), aws.bucketName, "logs/app-log.txt").log("Main", "main start testing");
        LocalApplication.main(args);

        // String[] p = new String[4];
        // p[0] = "input-sample.txt";//
        // p[1] = "outLastTest2.html";
        // p[2] = "1";
        // p[3] = "terminate";
        // LocalApplication.main(p);
        
        
        
    }
}
