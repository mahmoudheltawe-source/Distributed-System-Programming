package com.myproject.common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class myLogger {

    private String className;
    private static final DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public myLogger(Class<?> clazz) {
        this.className = clazz.getSimpleName();
    }

    private void log(String level, String message) {
        String time = LocalDateTime.now().format(formatter);
        System.out.println("[" + time + "] [" + level + "] [" + className + "] " + message);
        new  s3_debuge(AWS.getInstance().getS3Client(), AWS.getInstance().bucketName, "logs/app-log.txt").log(className, message);
    }    private void logE(String level, String message) {
        String time = LocalDateTime.now().format(formatter);
        System.err.println("[" + time + "] [" + level + "] [" + className + "] " + message);
                new  s3_debuge(AWS.getInstance().getS3Client(), AWS.getInstance().bucketName, "logs/app-log.txt").log(className, message);

    }

    public void info(String message) {
        log("INFO", message);
    }

    public void debug(String message) {
        log("DEBUG", message);
        
    }

    public void error(String message) {
        log("ERROR", message);
    }

    public void error(String message, Exception e) {
        logE("ERROR", message + " | Exception: " + e.getMessage());
        e.printStackTrace();
    }
    
}
