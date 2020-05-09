package org.qcri.rheem.core.arrow.utils;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class VectorUtils {


    /**
     * 根据arrow的数据类型，返回对应的vector
     *
     * @param dataType 数据类型
     * @return vector
     */
    public static ValueVector creativeVector(ArrowType dataType, String fieldName, BufferAllocator allocator) {
        switch (dataType.getTypeID()) {
            case Bool: return new BitVector(fieldName, allocator);
            case Int: return new IntVector(fieldName, allocator);
            case FloatingPoint: return new Float8Vector(fieldName, allocator);
            case Utf8: return new VarCharVector(fieldName, allocator);
        }
        return null;
    }

    /**
     * 设置vector的值在指定位置
     *
     * @param vector 存放的vector
     * @param value 值
     * @param index 存放位置
     */
    public static void setVectorValue(ValueVector vector, Object value, int index) {
        if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(index, (int)value);
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(index, (int)value);
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(index, (int)value);
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setSafe(index, ((String)value).getBytes());
        }
    }
}
