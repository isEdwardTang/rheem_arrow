package org.qcri.rheem.core.arrow.data;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.qcri.rheem.core.arrow.utils.VectorUtils;

import java.util.Iterator;

/**
 * 底层使用arrow的数据，各个平台的数据均继承这个类
 */
public class BasicArrowData<Type> implements AutoCloseable {

    private ArrowType dataType;    // rheem的数据类型定义

    private ArrowTypeMapping typeMapping; // 从平台的数据类型到arrow数据类型的映射

    /*
        TODO: schema不支持，目前仅考虑类似JavaStream或者RDD的行的结构(Iterator)，后续添加schema支持表的结构，如Flink、SparkSql、Jdbc
        TODO: 后续改成 直接使用 field + schema
     */
    // 实际存储的数据，暂时只考虑一维情况，也就是Type是个基本类型，不考虑嵌套
    private ValueVector vector;
    private RootAllocator allocator;

    public BasicArrowData(ArrowTypeMapping typeMapping, Iterator<Type> data) {
        this.typeMapping = typeMapping;

        // 通过平台的数据data初始化vector
        this.allocator = new RootAllocator();
        int index = 0;
        while (data.hasNext()) {
            // 初次时判断类型
            Object value = data.next();
            if (index == 0) {
                this.dataType = this.typeMapping.toArrowType(value);
                this.vector = VectorUtils.creativeVector(this.dataType, "BasicArrowData", this.allocator);
            }
            VectorUtils.setVectorValue(this.vector, value, index);
            index += 1;
        }

    }



    @Override
    public void close() throws Exception {
        if (allocator != null) {
            allocator.close();
        }
    }
}
