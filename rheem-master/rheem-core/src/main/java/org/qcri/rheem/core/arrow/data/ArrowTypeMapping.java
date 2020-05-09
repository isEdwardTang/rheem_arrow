package org.qcri.rheem.core.arrow.data;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * 实现从arrow的数据类型到内部使用的数据类型之间的相互转换
 */
public interface ArrowTypeMapping {

    /**
     * 从内部各个平台的数据类型到arrow的数据类型的映射
     * TODO: 还需要支持tuple类型
     *
     * @param internalType 内部数据类型
     * @return Arrow数据类型
     */
    default ArrowType toArrowType(Object internalType) {
        // 默认实现，平台数据类型使用泛型，Object是java基本类型的引用类型
        if (internalType instanceof Boolean) {
            return ArrowType.Bool.INSTANCE;
        } else if (internalType instanceof Byte) {
            return new ArrowType.Int(8, true);
        } else if (internalType instanceof Short) {
            return new ArrowType.Int(8 * 2, true);
        } else if (internalType instanceof Integer) {
            return new ArrowType.Int(8 * 4, true);
        } else if (internalType instanceof Long) {
            return new ArrowType.Int(8 * 8, true);
        } else if(internalType instanceof Float) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
        } else if (internalType instanceof Double) {
            return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        } else if (internalType instanceof String) {
            return ArrowType.Utf8.INSTANCE;
        } else {
            throw new UnsupportedOperationException(internalType.getClass() + " is not support!");
        }
    }

//    /**
//     * 从arrow的数据类型到内部数据类型的映射
//     * @param arrowType arrow的数据类型
//     * @return 内部数据类型
//     */
//    Object fromArrowType(ArrowType arrowType);
}
