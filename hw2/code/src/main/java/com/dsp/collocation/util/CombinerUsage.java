package com.dsp.collocation.util;

public enum CombinerUsage {
    WITH,
    WITHOUT;
    
    public static CombinerUsage fromString(String s) {
        if (s == null) return WITHOUT;
        s = s.trim().toUpperCase();
        if (s.equals("WITH") || s.equals("TRUE") || s.equals("YES") || s.equals("1")) return WITH;
        return WITHOUT;
    }

    public boolean enabled() {
        return this == WITH;
    }
}
