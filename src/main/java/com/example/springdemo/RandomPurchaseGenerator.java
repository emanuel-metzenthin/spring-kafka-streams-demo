package com.example.springdemo;

import com.sun.tools.javac.util.List;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class RandomPurchaseGenerator {
    public Purchase generate() {

        List<String> names = List.of("iPhone", "TV", "Laptop", "Monitor", "Earphones");
        List<Integer> prices = List.of(1000, 700, 1500, 500, 150);

        Random r = new Random();
        int i = r.nextInt(names.size());

        return new Purchase(names.get(i), prices.get(i));
    }
}