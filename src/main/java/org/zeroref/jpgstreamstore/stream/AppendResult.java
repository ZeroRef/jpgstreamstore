package org.zeroref.jpgstreamstore.stream;

public class AppendResult {
    private int currentVersion = 0;

    public AppendResult(int currentVersion) {
        this.currentVersion = currentVersion;
    }

    public int getCurrentVersion() {
        return currentVersion;
    }
}
