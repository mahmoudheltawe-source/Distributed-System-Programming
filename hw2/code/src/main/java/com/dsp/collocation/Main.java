package com.dsp.collocation;


public class Main {

    public static void main(String[] args) throws Exception {

        // args:
        // 0 - bigram input
        // 1 - unigram input
        // 2 - output base path

        //step1
        runBigramJob(args[0], args[2] + "/bigram");
        //step 2
        runUnigramJob(args[1], args[2] + "/unigram");
        //step 3  
        runJoinJob(args[2] + "/bigram", args[2] + "/unigram", args[2] + "/llr");
        //step 4
        runTopJob(args[2] + "/llr", args[2] + "/top100");
    }

    private static void runTopJob(String string, String string2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'runTopJob'");
    }

    private static void runUnigramJob(String string, String string2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'runUnigramJob'");
    }

    private static void runJoinJob(String string, String string2, String string3) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'runJoinJob'");
    }

    private static void runBigramJob(String string, String string2) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'runBigramJob'");
    }
}