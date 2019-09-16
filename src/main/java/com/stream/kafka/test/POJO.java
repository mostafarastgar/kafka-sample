package com.stream.kafka.test;

public class POJO {
    private String message;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "POJO{" +
                "message='" + message + '\'' +
                '}';
    }
}
