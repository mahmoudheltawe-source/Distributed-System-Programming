package com.dsp.collocation.util;

import com.amazonaws.regions.Regions;

public class util_class {

    static String bucketName = "heb-output-bucket-s3-mahmoud-with-final-test41";
    public static String getDecade(int year) {
        return (year / 10) * 10 + "s";

    }

    public static boolean isStopWord(String word) {
        return StopWords.isStopWord(word);
    }

    public static double compute(long c12, long c1, long c2, long N) {
       return LLRCalculator.compute(c12,c1,c2,N);
    }
    

    public static String getbucketName() {
        return bucketName;
    }

    public static Regions getRegion() {
        return Regions.US_EAST_1;
    }


}