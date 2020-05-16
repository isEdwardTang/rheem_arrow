package org.qcri.rheem.core.arrow.utils;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.qcri.rheem.core.arrow.data.ArrowTypeMapping;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class VectorUtils {


    /**
     * 根据arrow的数据类型，返回对应的vector
     *
     * @param dataType 数据类型
     * @return vector
     */
    public static FieldVector creativeVector(ArrowType dataType, String fieldName, BufferAllocator allocator) {
        FieldVector vector = null;
        switch (dataType.getTypeID()) {
            case Bool:
                vector = new BitVector(fieldName, allocator);
                break;
            case Int:
                vector = new IntVector(fieldName, allocator);
                break;
            case FloatingPoint:
                vector = new Float8Vector(fieldName, allocator);
                break;
            case Utf8:
                vector = new VarCharVector(fieldName, allocator);
                break;
        }
        if (vector != null) {
            vector.allocateNew();
        }
        return vector;
    }

    /**
     * 设置vector的值在指定位置
     *
     * @param vector 存放的vector
     * @param value  值
     * @param index  存放位置
     */
    public static void setVectorValue(ValueVector vector, Object value, int index) {
        if (vector instanceof BitVector) {
            ((BitVector) vector).setSafe(index, (int) value);
        } else if (vector instanceof IntVector) {
            ((IntVector) vector).setSafe(index, (int) value);
        } else if (vector instanceof Float8Vector) {
            ((Float8Vector) vector).setSafe(index, (Double) value);
        } else if (vector instanceof VarCharVector) {
            ((VarCharVector) vector).setSafe(index, ((String) value).getBytes());
        }
    }

    /**
     * 获取vector指定位置的值
     *
     * @param vector 存放vector
     * @param index 下标
     * @return value
     */
    public static Object getVectorValue(FieldVector vector, int index) {
        if (vector instanceof BitVector) {
            return ((BitVector) vector).get(index);
        } else if (vector instanceof IntVector) {
            return ((IntVector) vector).get(index);
        } else if (vector instanceof Float8Vector) {
            return((Float8Vector) vector).get(index);
        } else if (vector instanceof VarCharVector) {
            return ((VarCharVector) vector).get(index);
        }
        return null;
    }

    /**
     * 将迭代器的数据存储到vector中
     *
     * @param data 数据
     * @return vector
     */
    public static FieldVector creativeVectorByIterator(BufferAllocator allocator, ArrowTypeMapping mapping,
                                                       Iterator<?> data) {
        FieldVector vector = null;
        int index = 0;
        while (data.hasNext()) {
            // 初次时判断类型
            Object value = data.next();
            if (index == 0) {
                ArrowType type = mapping.toArrowType(value);
                vector = VectorUtils.creativeVector(type, "BasicArrowData", allocator);
            }
            VectorUtils.setVectorValue(vector, value, index);
            index += 1;
        }
        assert vector != null;
        vector.setValueCount(index);
        return vector;
    }

    /**
     * 获取迭代器对应的用于flight的producer，目前只考虑一行的情况
     *
     * @param allocator vector空间管理
     * @param mapping   数据映射
     * @param data      数据
     * @return producer
     */
    public static FlightProducer getFlightProducer(BufferAllocator allocator,
                                                   ArrowTypeMapping mapping, Iterator<?> data) {

        return new FlightProducer() {
            FieldVector vector; // 数据
            ArrowType type; // 数据类型

            @Override
            public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
                // 最重要的方法，读取数据
                String ticketString = new String(ticket.getBytes());
                if (ticketString.equals("get list data")) {
                    // 直接把所有的数据返回
                    // schema信息
                    VectorSchemaRoot root = VectorSchemaRoot.of(vector);
                    listener.start(root);
                    // 数据
                    root.allocateNew();
                    int index = 0;
                    while (data.hasNext()) {
                        // 初次时判断类型
                        Object value = data.next();
                        if (index == 0) {
                            type = mapping.toArrowType(value);
                            vector = VectorUtils.creativeVector(type, "BasicArrowData", allocator);
                        }
                        VectorUtils.setVectorValue(vector, value, index);
                        index += 1;
                    }
                    vector.setValueCount(index);
                    root.setRowCount(1);    // 目前只考虑简单的一行的情况
                    listener.putNext();
                    root.close();
                    listener.completed();
                }
            }

            @Override
            public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
                // 列出可用的datastream
            }

            @Override
            public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
                return null;
            }

            @Override
            public Runnable acceptPut(CallContext context, FlightStream flightStream, StreamListener<PutResult> ackStream) {
                return null;
            }

            @Override
            public void doAction(CallContext context, Action action, StreamListener<Result> listener) {

            }

            @Override
            public void listActions(CallContext context, StreamListener<ActionType> listener) {

            }

        };
    }

    /**
     * 将vector转换成List保存
     * @param vector 数据
     * @return list
     */
    public static List<Object> convertVectorToList(FieldVector vector) {
        List<Object> result = new ArrayList<>();
        int counts = vector.getValueCount();
        for (int i = 0; i < counts; i++) {
            result.add(VectorUtils.getVectorValue(vector, i));
        }
        return result;
    }

}
