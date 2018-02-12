package org.zeroref.jpgstreamstore;

public class AppendResult {
    private int currentVersion;

    public AppendResult(int currentVersion) {
        this.currentVersion = currentVersion;
    }

    public int getCurrentVersion() {
        return currentVersion;
    }
}
