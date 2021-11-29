import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class mycbiao {
    public static Configuration configuration; //HBase 配置信息
    public static Connection connection; //HBase 连接
    public static Admin admin;

    //建立连接
    public void init () {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","localhost");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        try{
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    //关闭连接 public static void createTable(String tableName, String familyNames[]) throws IOException {
    //        //如果表存在退出
    //        if (admin.tableExists(TableName.valueOf(tableName))) {
    //            System.out.println("Table exists!");
    //            return;
    //        }
    //        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
    //        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
    //        for (String familyName : familyNames) {
    //            tableDescriptor.addFamily(new HColumnDescriptor(familyName));
    //        }
    //        //tableDescriptor.addFamily(new HColumnDescriptor(familyName));
    //        admin.createTable(tableDescriptor);
    //        System.out.println("createtable success!");
    //    }
    //
    //    /**
    //     * 删除表
    //     * @param tableName 表名
    //     * */
    //    public static void dropTable(String tableName) throws IOException {
    //        //如果表不存在报异常
    //        if (!admin.tableExists(TableName.valueOf(tableName))) {
    //            System.out.println(tableName+"不存在");
    //            return;
    //        }
    //
    //        //删除之前要将表disable
    //        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
    //            admin.disableTable(TableName.valueOf(tableName));
    //        }
    //        admin.deleteTable(TableName.valueOf(tableName));
    //        System.out.println("deletetable " + tableName + "ok.");
    //    }
    //
    //    /**
    //     * 指定行/列中插入数据
    //     * @param tableName 表名
    //     * @param rowKey 主键rowkey
    //     * @param family 列族
    //     * @param column 列
    //     * @param value 值
    //     * TODO: 批量PUT
    //     */
    //    public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
    //        //3.2获得Table接口,需要传入表名
    //        Table table =connection.getTable(TableName.valueOf(tableName));
    //        Put put = new Put(Bytes.toBytes(rowKey));
    //        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
    //        table.put(put);
    //        System.out.println("insertrecored " + rowKey + " to table " + tableName + "ok.");
    //    }
    //
    //    /**
    //     * 删除表中的指定行
    //     * @param tableName 表名
    //     * @param rowKey rowkey
    //     * TODO: 批量删除
    //     */
    //    public static void delete(String tableName, String rowKey) throws IOException {
    //        //3.2获得Table接口,需要传入表名
    //        Table table = connection.getTable(TableName.valueOf(tableName));
    //        Delete delete = new Delete(Bytes.toBytes(rowKey));
    //        table.delete(delete);
    //    }
    //}

    public void close() {
        try{
            if(admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    //检查现在所有的表
    public void listTables() throws IOException {
        //Getting all the list of tables using HBaseAdmin object
        HTableDescriptor[] tableDescriptor =admin.listTables();
        // printing all the table names.
        if(tableDescriptor.length == 0){
            System.out.println("no table.");
        }
        else{
            System.out.println("Tables:");
            for (int i=0; i<tableDescriptor.length; i++ ){
                System.out.println(tableDescriptor[i].getNameAsString());
            }
        }

    }

    /**
     * 创建 HBase 数据库表的时候，首先需要定义表的模型，包括表的名称、行键和列族的名称。
     * @param myTableName 表名
     * @param colFamily 列族
     * @throws IOException
     */
    public void createTable(String myTableName, String[] colFamily) throws IOException {
        TableName tableName = TableName.valueOf(myTableName);
        if(admin.tableExists(tableName)){
            System.out.println("table "+ myTableName +" exists!");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for(String str:colFamily){
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(str);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            System.out.println("Successfully create table: "+ myTableName +"!");
        }
    }

    /**
     * 扫描全表
     * @param myTableName
     * @throws IOException
     */
    public void scanTable(String myTableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(myTableName)); //获取table
        Scan scan = new Scan();
        ResultScanner scanResult = table.getScanner(scan);


        //遍历每一行
        for (Result result : scanResult) {
            String row = new String(result.getRow()); //获取row key
            List<Cell> cells = result.listCells(); //将cell的内容放到list中
            //行键 column=列族名:列族限定, value=xxx
            for (Cell c:cells) {
                System.out.println(row+"\t"+new String(CellUtil.cloneFamily(c))+":"+
                        new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
            }
        }
    }

    //按照列来扫描
    public void scanTableByColumn(String myTableName, String colFamily, String col) throws IOException {
        Table table=connection.getTable(TableName.valueOf(myTableName));

        ResultScanner scanResult = table.getScanner(colFamily.getBytes(), col.getBytes()); //getScanner方法中设置列族和列名即可获得某列的scanner

        for (Result result : scanResult) {
            String row = new String(result.getRow());
            List<Cell> cells = result.listCells();
            for (Cell c:cells) {
                System.out.println(row+"\t"+new String(CellUtil.cloneFamily(c))+":"+
                        new String(CellUtil.cloneQualifier(c))+", value="+new String(CellUtil.cloneValue(c)));
            }
        }
    }

    //添加新的列族
    public void addFamily(String myTableName, String colFamily) throws IOException {
        HTableDescriptor tableDescriptor =  admin.getTableDescriptor(TableName.valueOf(myTableName)); //获得原来表的定义信息
        HColumnDescriptor nColumnDescriptor = new HColumnDescriptor(colFamily); //define a column family
        tableDescriptor.addFamily(nColumnDescriptor); //add new column family into table
        admin.modifyTable(TableName.valueOf(myTableName), tableDescriptor); //commit it to admin
        System.out.println("Add column family: "+colFamily+" successfully!");
    }

    /**
     * 插入一行数据
     * @param myTableName
     * @param rowKey
     * @param colFamily
     * @param col
     * @param val
     * @throws IOException
     */
    public void insertData(String myTableName, String rowKey, String colFamily, String col, String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(myTableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(colFamily.getBytes(),col.getBytes(),val.getBytes());
        table.put(put);
        table.close();
    }

    public void deleteByCell(String myTableName, String rowKey, String colFamily, String col) throws IOException {
        Table table = connection.getTable(TableName.valueOf(myTableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除指定列
        delete.addColumns(Bytes.toBytes(colFamily), Bytes.toBytes(col));
        table.delete(delete);
    }
    //删除表
    public void dropTable(String myTableName) throws IOException {
        if (admin.tableExists(TableName.valueOf(myTableName))) {
            //如果表存在，则先disable表，然后才能删除表
            admin.disableTable(TableName.valueOf(myTableName));
            admin.deleteTable(TableName.valueOf(myTableName));
            System.out.println("Drop table "+myTableName+" successfully!");
        } else {
            //如果表不存在，输出提示
            System.out.println("There is no table "+myTableName);
        }
    }

    public static void main (String [] args) throws IOException{
        //设置数据库所在的主机名称
        String masterName = "myc";
        String myTableName = "mycbiao";
        String[] families= new String[] {"stu_xinxi", "lesson1", "lesson2", "lesson3"};  //设置表的列族
        mycbiao operation=new mycbiao(); //创建operation对象
        operation.init(); //建立连接
        operation.createTable(myTableName, families);//建表
        operation.insertData(myTableName,"2015001","stu_xinxi","S_Name", "Li Lei");
        operation.insertData(myTableName,"2015001","stu_xinxi","S_Sex", "male");
        operation.insertData(myTableName,"2015001","stu_xinxi","S_Age", "23");
        operation.insertData(myTableName,"2015002","stu_xinxi","S_Name", "Han Meimei");
        operation.insertData(myTableName,"2015002","stu_xinxi","S_Sex", "female");
        operation.insertData(myTableName,"2015002","stu_xinxi","S_Age", "22");
        operation.insertData(myTableName,"2015003","stu_xinxi","S_Name", "Li Lei");
        operation.insertData(myTableName,"2015003","stu_xinxi","S_Sex", "male");
        operation.insertData(myTableName,"2015003","stu_xinxi","S_Age", "24");

        operation.insertData(myTableName,"2015001","lesson1","SC_Cno", "123001");
        operation.insertData(myTableName,"2015001","lesson1","C_Name", "Math");
        operation.insertData(myTableName,"2015001","lesson1","C_Credit", "2.0");
        operation.insertData(myTableName,"2015001","lesson1","SC_Score", "86");
        operation.insertData(myTableName,"2015001","lesson3","SC_Cno", "123003");
        operation.insertData(myTableName,"2015001","lesson3","C_Name", "English");
        operation.insertData(myTableName,"2015001","lesson3","C_Credit", "3.0");
        operation.insertData(myTableName,"2015001","lesson3","SC_Score", "69");
        operation.insertData(myTableName,"2015002","lesson2","SC_Cno", "123002");
        operation.insertData(myTableName,"2015002","lesson2","C_Name", "Computer Science");
        operation.insertData(myTableName,"2015002","lesson2","C_Credit", "5.0");
        operation.insertData(myTableName,"2015002","lesson2","SC_Score", "77");
        operation.insertData(myTableName,"2015002","lesson3","SC_Cno", "123003");
        operation.insertData(myTableName,"2015002","lesson3","C_Name", "English");
        operation.insertData(myTableName,"2015002","lesson3","C_Credit", "3.0");
        operation.insertData(myTableName,"2015002","lesson3","SC_Score", "99");
        operation.insertData(myTableName,"2015003","lesson1","SC_Cno", "123001");
        operation.insertData(myTableName,"2015003","lesson1","C_Name", "Math");
        operation.insertData(myTableName,"2015003","lesson1","C_Credit", "2.0");
        operation.insertData(myTableName,"2015003","lesson1","SC_Score", "98");
        operation.insertData(myTableName,"2015003","lesson2","SC_Cno", "123002");
        operation.insertData(myTableName,"2015003","lesson2","C_Name", "Computer Science");
        operation.insertData(myTableName,"2015003","lesson2","C_Credit", "5.0");
        operation.insertData(myTableName,"2015003","lesson2","SC_Score", "95");
        operation.scanTable(myTableName);
        operation.scanTableByColumn(myTableName, "lesson2", "SC_Score");
        operation.deleteByCell(myTableName, "2015003","lesson1", "SC_Cno");
        operation.deleteByCell(myTableName, "2015003","lesson1", "C_Name");
        operation.deleteByCell(myTableName, "2015003","lesson1", "C_Credit");
        operation.deleteByCell(myTableName, "2015003","lesson1", "SC_Score");
        operation.deleteByCell(myTableName, "2015003","lesson2", "SC_Cno");
        operation.deleteByCell(myTableName, "2015003","lesson2", "C_Name");
        operation.deleteByCell(myTableName, "2015003","lesson2", "C_Credit");
        operation.deleteByCell(myTableName, "2015003","lesson2", "SC_Score");
        operation.deleteByCell(myTableName, "2015003","lesson3", "SC_Cno");
        operation.deleteByCell(myTableName, "2015003","lesson3", "C_Name");
        operation.deleteByCell(myTableName, "2015003","lesson3", "C_Credit");
        operation.deleteByCell(myTableName, "2015003","lesson3", "SC_Score");
        operation.scanTable(myTableName);
        operation.listTables();
        operation.dropTable(myTableName);
        operation.listTables();


        operation.close();
    }
}
