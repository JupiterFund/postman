package com.nodeunify.jupiter.postman;

import java.util.Calendar;

public class CalendarTest {
    public static void main(String[] args) {
        Calendar cal = Calendar.getInstance();
        int ctpYear = cal.get(Calendar.YEAR);
        int ctpMonth = cal.get(Calendar.MONTH);
        int ctpDay = cal.get(Calendar.DAY_OF_MONTH);
        System.out.println(ctpYear + " - " + ctpMonth + " - " + ctpDay);
    }
}
