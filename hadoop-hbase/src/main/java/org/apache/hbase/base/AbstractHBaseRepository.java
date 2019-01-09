package org.apache.hbase.base;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;


import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public abstract class AbstractHBaseRepository<Entity> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractHBaseRepository.class);

    @Autowired
    private Connection connection;

    private String tableName;

    private String family;

    private Class<Entity> entityClass;

    public AbstractHBaseRepository(){
    }
    public AbstractHBaseRepository(String tableName,String family){
        this.tableName = tableName;
        this.family = family;
    }


    @PostConstruct
    public void init() {
        //this指代的是子类的对象，this.getClass()返回代表子类的Class对象，
        //再接着调用getGenericSuperclass()方法，可以返回代表子类的直接
        //超类（也就是AbstractBaseDao类）的Type对象。因为它是泛型，因此可强制类型
        //转换为ParameterizedType类型，再调用getActualTypeArguments()
        //方法，可获得子类给泛型指定的实际类型的数组。因为这里只有一个泛型
        //参数，所以取数组的第1个元素，即为entity的类型。entityClass代表entity的类型。
        @SuppressWarnings("unchecked")
//        Class<T> entityType = (Class<T>) ((ParameterizedType) this.getClass()
//                .getGenericSuperclass()).getActualTypeArguments()[1];
                Type[] typeArray = ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments();
        this.entityClass = (Class<Entity>)typeArray[0];
        logger.info("PostConstruct");
    }


    public Entity get(String rowkey) throws Exception {
        Table table = null;
        Entity entity = null;
        try {
            table = connection.getTable(TableName.valueOf( tableName));
            Get get = new Get(Bytes.toBytes(rowkey));

            Result result = table.get(get);
            entity = createEntity(result);
        } finally {
            if (table != null) {
                table.close();
            }
        }
        return entity;
    }

    /**
     * 同步写
     * @return boolean 是否插入成功
     * @throws Exception
     */
    public List<Entity> scanByPreRowkey(String preRowkey) throws Exception {
        Table table = null;
        List<Entity> list = new LinkedList<>();
        ResultScanner scanner = null;
        byte[] preRowkeyBytes= Bytes.toBytes(preRowkey);
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();

            Filter filter = new PrefixFilter(preRowkeyBytes);
            scan.setFilter(filter);
            scanner = table.getScanner(scan);
            for(Result result : scanner){
                logger.info("rowkey is {}",Bytes.toString(result.getRow()));
                Entity newInstance = createEntity(result);
                list.add(newInstance);
            }
        } finally {
            if (table != null) {
                table.close();
            }
        }
        return list;
    }


    /**
     * 根据qualifier包含的字符串范围扫描
     * @param startStr 起始字符串
     * @param endStr 结束字符串
     * @return
     * @throws Exception
     */
    public List<Entity> scanBySingleColumnValue(String qualifier, String startStr, String endStr) throws Exception {
        Table table = null;
        List<Entity> list = new LinkedList<>();
        ResultScanner scanner = null;
        try {

            table = connection.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            FilterList filterList = new FilterList();
            if(StringUtils.isNotBlank(startStr) && StringUtils.isNotBlank(endStr)){
                SingleColumnValueFilter filterOne = new SingleColumnValueFilter(Bytes.toBytes(family),
                        Bytes.toBytes(qualifier), CompareOperator.GREATER, Bytes.toBytes(startStr));
                SingleColumnValueFilter filterTwo = new SingleColumnValueFilter(Bytes.toBytes(family),
                        Bytes.toBytes(qualifier), CompareOperator.LESS_OR_EQUAL, Bytes.toBytes(endStr));
                filterList.addFilter(filterOne);
                filterList.addFilter(filterTwo);
            }else if(StringUtils.isNotBlank(startStr) && StringUtils.isBlank(endStr)){
                SingleColumnValueFilter filterOne = new SingleColumnValueFilter(Bytes.toBytes(family),
                        Bytes.toBytes(qualifier), CompareOperator.GREATER_OR_EQUAL, Bytes.toBytes(startStr));
                filterList.addFilter(filterOne);
            }else if(StringUtils.isBlank(startStr) && StringUtils.isNotBlank(endStr)){
                SingleColumnValueFilter filterTwo = new SingleColumnValueFilter(Bytes.toBytes(family),
                        Bytes.toBytes(qualifier), CompareOperator.LESS_OR_EQUAL, Bytes.toBytes(endStr));
                filterList.addFilter(filterTwo);
            }
            scan.setFilter(filterList);
            scanner = table.getScanner(scan);
            for(Result result : scanner){
                logger.info("rowkey is {}",Bytes.toString(result.getRow()));
                Entity newInstance = createEntity(result);
                list.add(newInstance);
            }
        } finally {
            if (table != null) {
                table.close();
            }
        }
        return list;
    }

    private Entity createEntity(Result result) throws Exception{
        Entity newInstance = entityClass.newInstance();
        for(Cell cell : result.listCells()){
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            Field field = entityClass.getDeclaredField(qualifier);
            field.setAccessible(true);
            Class<?> filedType = field.getType();
            if(filedType.equals(Long.class)){
                field.set(newInstance,Bytes.toLong(CellUtil.cloneValue(cell)));
            }else if(filedType.equals(Double.class)){
                field.set(newInstance,Bytes.toDouble(CellUtil.cloneValue(cell)));
            }else if(filedType.equals(Integer.class)){
                field.set(newInstance,Bytes.toInt(CellUtil.cloneValue(cell)));
            }else if(filedType.equals(BigDecimal.class)){
                field.set(newInstance,Bytes.toBigDecimal(CellUtil.cloneValue(cell)));
            }else if(filedType.equals(Date.class)){
                String dateString = Bytes.toString(CellUtil.cloneValue(cell));
                DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
                field.set(newInstance,DateTime.parse(dateString,dateTimeFormatter).toDate());
            }else{
                field.set(newInstance,Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        return newInstance;
    }


}
