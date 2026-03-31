package com.clougence.cloudcanal.es6.sink;

public class CcEs6ReplayResult {

    private final boolean skipped;

    private final String message;

    private CcEs6ReplayResult(boolean skipped, String message) {
        this.skipped = skipped;
        this.message = message;
    }

    public static CcEs6ReplayResult applied() {
        return new CcEs6ReplayResult(false, null);
    }

    public static CcEs6ReplayResult skipped(String message) {
        return new CcEs6ReplayResult(true, message);
    }

    public boolean isSkipped() {
        return skipped;
    }

    public String getMessage() {
        return message;
    }
}
