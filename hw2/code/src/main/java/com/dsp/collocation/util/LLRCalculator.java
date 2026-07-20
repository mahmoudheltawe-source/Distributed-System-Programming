package com.dsp.collocation.util;

public class LLRCalculator {

    public static double compute(double c12, double c1, double c2, double n) {
        double k11 = c12;
        double k12 = c1 - c12;
        double k21 = c2 - c12;
        double k22 = n - c1 - c2 + c12;

        if (k12 < 0){
            k12 = 0;
        } 
        if (k21 < 0){
            k21 = 0;
        } 
        if (k22 < 0){
            k22 = 0;
        } 

        double row1 = k11 + k12;
        double row2 = k21 + k22;
        double col1 = k11 + k21;
        double col2 = k12 + k22;

        if (row1 == 0 || row2 == 0 || col1 == 0 || col2 == 0) {
            return 0.0;
        }

        double m11 = (row1 * col1) / n;
        double m12 = (row1 * col2) / n;
        double m21 = (row2 * col1) / n;
        double m22 = (row2 * col2) / n;

        double llr = 0;

        llr += safeTerm(k11, m11);
        llr += safeTerm(k12, m12);
        llr += safeTerm(k21, m21);
        llr += safeTerm(k22, m22);

        return 2.0 * llr;
        
    }

    private static double safeTerm(double observed, double expected) {
        if (observed == 0) return 0;
        return observed * Math.log(observed / expected);
    }
    private static double xlogx(long x) {
        if (x <= 0) return 0.0;
        return x * Math.log(x);
    }

    
    public static void main(String[] args) {
        System.out.println(compute(20, 5000, 2000, 10000));
    }
}