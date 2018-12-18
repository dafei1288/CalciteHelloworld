package com.dafei1288.calcite.function;

public class StringFunction {
    public String concat(Object o1,Object o2){
        return "["+o1.toString()+" , "+o2.toString()+"] => "+this.toString();
    }
    public String parseString(Object o){
        return o.toString();
    }
}
