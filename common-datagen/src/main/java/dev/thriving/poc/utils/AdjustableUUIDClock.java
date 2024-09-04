package dev.thriving.poc.utils;

import com.fasterxml.uuid.UUIDClock;

import java.time.Duration;

public class AdjustableUUIDClock extends UUIDClock {

    private long baseTimeMillis;

    // Constructor with system time initialization
    public AdjustableUUIDClock() {
        this.baseTimeMillis = System.currentTimeMillis();
    }

    // Constructor with provided epoch time
    public AdjustableUUIDClock(long epochMillis) {
        this.baseTimeMillis = epochMillis;
    }

    // Override currentTimeMillis to return the current base time
    @Override
    public long currentTimeMillis() {
        return baseTimeMillis;
    }

    // Method to set the time directly using epoch milliseconds
    public void setEpochMillis(long epochMillis) {
        this.baseTimeMillis = epochMillis;
    }

    // Method to shift the current time directly using a java.time.Duration
    public void shiftTime(Duration duration) {
        this.baseTimeMillis += duration.toMillis();
    }
}
