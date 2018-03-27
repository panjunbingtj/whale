package org.apache.storm.serializer;

import java.io.*;

public class PersonSingleton implements Serializable {
	private static final long serialVersionUID = 1L;
    private String name;
    private PersonSingleton(String name) {
        this.name = name;
    };
    private static PersonSingleton person = null;

    public static synchronized PersonSingleton getInstance() {
        if (person == null)
            return person = new PersonSingleton("cgl");
        return person;
    }

    private Object writeReplace() throws ObjectStreamException {
        System.out.println("1 write replace start");
        return this;//可修改为其他对象
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        System.out.println("2 write object start");
        out.defaultWriteObject();
       //out.writeInt(1);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        System.out.println("3 read object start");
        in.defaultReadObject();
       //int i=in.readInt();
    }

    private Object readResolve() throws ObjectStreamException {
        System.out.println("4 read resolve start");
        return PersonSingleton.getInstance();//不管序列化的操作是什么，返回的都是本地的单例对象
    }
}
