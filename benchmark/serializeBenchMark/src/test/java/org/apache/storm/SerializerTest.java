package org.apache.storm;

import org.apache.storm.serializer.PersonSingleton;
import org.junit.Test;

import java.io.*;

/**
 * locate org.apache.storm
 * Created by mastertj on 2018/3/26.
 */
public class SerializerTest {

    public static final String FilePath="D:\\ProgramProjects\\IntelJIDEAProject\\IntelliJIDEA15.0.2\\BigDataProject\\StormSource\\storm\\benchmark\\serializeBenchMark\\data\\person.dat";
    @Test
    public void serialize() throws Exception {
        File file = new File(FilePath);
        if(!file.exists())
            file.createNewFile();
        FileOutputStream out = new FileOutputStream(file);
        ObjectOutputStream op = new ObjectOutputStream(out);
        op.writeObject(PersonSingleton.getInstance());
        op.close();
    }

    @Test
    public void deserialize() throws Exception {
        FileInputStream in = new FileInputStream(new File(FilePath));
        ObjectInputStream oi = new ObjectInputStream(in);
        Object person = oi.readObject();
        in = new FileInputStream(new File(FilePath));
        oi = new ObjectInputStream(in);
        PersonSingleton person1 = (PersonSingleton) oi.readObject();

        System.out.println("sington person hashcode:" + person.hashCode());
        System.out.println("sington person1 hashcode:" + person1.hashCode());
        System.out.println("singleton getInstance hashcode:" + PersonSingleton.getInstance().hashCode());
        System.out.println("singleton person equals:" + (person == PersonSingleton.getInstance()));
        System.out.println("person equals1:" + (person1 == person));
    }
}
