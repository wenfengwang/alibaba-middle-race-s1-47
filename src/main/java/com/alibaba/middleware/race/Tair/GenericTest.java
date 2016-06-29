package com.alibaba.middleware.race.Tair;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Created by wangwenfeng on 6/29/16.
 */
public class GenericTest<T> {
    public static void main(String[] args) {
        GenericTest<String> test = new GenericTest<String>(){}; // 匿名内部类的声明在编译时进行，实例化在运行时进行

        Type typeClass1 = test.getClass().getGenericSuperclass();
        System.out.println(typeClass1);

        if (typeClass1 instanceof ParameterizedType) {
            Type actualType1 = ((ParameterizedType)typeClass1).getActualTypeArguments()[0];

            System.out.println(actualType1);
        } else {
            System.out.println(typeClass1 + " is Not ParameterizedType");
        }

        System.out.println(" ==================================== ");

        GenericTest<String> test2 = new GenericTest<String>(); // 所有的泛型类型在运行时都是Object类型
        Type typeClass2 = test2.getClass().getGenericSuperclass();
        System.out.println(typeClass2);

        if (typeClass2 instanceof ParameterizedType) {
            Type actualType2 = ((ParameterizedType)typeClass2).getActualTypeArguments()[0];

            System.out.println(actualType2);
        } else {
            System.out.println(typeClass2 + " is Not ParameterizedType");
        }

    }
}
