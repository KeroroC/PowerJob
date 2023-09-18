package tech.powerjob.server.persistence.storage.impl;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.env.Environment;
import tech.powerjob.common.serialize.JsonUtils;
import tech.powerjob.common.utils.CommonUtils;
import tech.powerjob.common.utils.NetUtils;
import tech.powerjob.server.common.constants.SwitchableStatus;
import tech.powerjob.server.common.spring.condition.PropertyAndOneBeanCondition;
import tech.powerjob.server.extension.dfs.*;
import tech.powerjob.server.persistence.storage.AbstractDFsService;

import javax.annotation.Priority;
import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Oracle 特性类似的数据库存储
 * PS1. 大文件上传可能会报 max_allowed_packet 不足，可根据参数放开数据库限制 set global max_allowed_packet = 500*1024*1024
 * PS2. 数据库并不适合大规模的文件存储，该扩展仅适用于简单业务，大型业务场景请选择其他存储方案（OSS、MongoDB等）
 * ********************* 配置项 *********************
 *  oms.storage.dfs.oracle.driver
 *  oms.storage.dfs.oracle.url
 *  oms.storage.dfs.oracle.username
 *  oms.storage.dfs.oracle.password
 *  oms.storage.dfs.oracle.auto_create_table
 *  oms.storage.dfs.oracle.table_name
 *
 * @author wangpeng
 * @since 2023/9/15
 */
@Slf4j
@Priority(value = Integer.MAX_VALUE - 4)
@Conditional(OracleDfsService.OracleCondition.class)
public class OracleDfsService extends AbstractDFsService {

    private DataSource dataSource;

    private static final String TYPE_ORACLE = "oracle";

    /**
     * 数据库驱动，oracle 为 oracle.jdbc.OracleDriver
     */
    private static final String KEY_DRIVER_NAME = "driver";
    /**
     * 数据库地址，比如 jdbc:oracle:thin:@127.0.0.1:1521:orcl
     */
    private static final String KEY_URL = "url";
    /**
     * 用户名，比如 DB_POWER_JOB
     */
    private static final String KEY_USERNAME = "username";
    /**
     * 用户密码
     */
    private static final String KEY_PASSWORD = "password";
    /**
     * 是否自动建表
     */
    private static final String KEY_AUTO_CREATE_TABLE = "auto_create_table";
    /**
     * 表名
     */
    private static final String KEY_TABLE_NAME = "table_name";

    /* ********************* SQL region ********************* */

    private static final String DEFAULT_TABLE_NAME = "powerjob_files";

    private static final String CREATE_TABLE_SQL = "declare num number;\n" +
            "begin\n" +
            "    select count(1) into num from user_tables where table_name = upper('%1$s');\n" +
            "    if num = 0\n" +
            "    then\n" +
            "        execute immediate 'CREATE TABLE %1$s (\n" +
            "            id NUMBER(19, 0) NOT NULL,\n" +
            "            bucket VARCHAR2(255) NOT NULL,\n" +
            "            name VARCHAR2(255) NOT NULL,\n" +
            "            version VARCHAR2(255) NOT NULL,\n" +
            "            meta VARCHAR2(255),\n" +
            "            length NUMBER(19, 0) NOT NULL,\n" +
            "            status INT NOT NULL,\n" +
            "            data BLOB NOT NULL,\n" +
            "            extra VARCHAR2(255),\n" +
            "            gmt_create DATE NOT NULL,\n" +
            "            gmt_modified DATE,\n" +
            "            constraint PK_ID primary key (id))';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.id IS ''ID''';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.bucket IS ''分桶''';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.name IS ''文件名称''';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.version IS ''版本''';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.meta IS ''元数据''';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.length IS ''长度''';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.status IS ''状态''';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.data IS ''文件内容''';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.extra IS ''其他信息''';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.gmt_create IS ''创建时间''';\n" +
            "        execute immediate 'COMMENT ON COLUMN %1$s.gmt_modified IS ''更新时间''';\n" +
            "        execute immediate 'create sequence POWERJOB_FILES_INCREMENT_ID minvalue 1 nomaxvalue increment by 1 start with 1 nocache';\n" +
            "        execute immediate 'create trigger POWERJOB_FILES_INCREMENT_TRIG before insert on %1$s for each row when (new.id is null)\n" +
            "                           begin\n" +
            "                               select POWERJOB_FILES_INCREMENT_ID.nextval into:new.id from dual;\n" +
            "                           end;';\n" +
            "        commit;\n" +
            "    end if;\n" +
            "end;";

    private static final String INSERT_SQL = "insert into %s(bucket, name, version, meta, length, status, data, extra, gmt_create, gmt_modified) values (?,?,?,?,?,?,?,?,?,?);";

    private static final String DELETE_SQL = "DELETE FROM %s ";

    private static final String QUERY_FULL_SQL = "select * from %s";

    private static final String QUERY_META_SQL = "select bucket, name, version, meta, length, status, extra, gmt_create, gmt_modified from %s";


    private void deleteByLocation(FileLocation fileLocation) {
        String dSQLPrefix = fullSQL(DELETE_SQL);
        String dSQL = dSQLPrefix.concat(whereSQL(fileLocation));
        executeDelete(dSQL);
    }

    private void executeDelete(String sql) {
        try (Connection con = dataSource.getConnection()) {
            con.createStatement().executeUpdate(sql);
        }  catch (Exception e) {
            log.error("[OracleDfsService] executeDelete failed, sql: {}", sql);
        }
    }

    @Override
    public void store(StoreRequest storeRequest) throws IOException {

        Stopwatch sw = Stopwatch.createStarted();
        String insertSQL = fullSQL(INSERT_SQL);

        FileLocation fileLocation = storeRequest.getFileLocation();

        // 覆盖写，写之前先删除
        deleteByLocation(fileLocation);

        Map<String, Object> meta = Maps.newHashMap();
        meta.put("_server_", NetUtils.getLocalHost());
        meta.put("_local_file_path_", storeRequest.getLocalFile().getAbsolutePath());

        Date date = new Date(System.currentTimeMillis());

        try (Connection con = dataSource.getConnection()) {
            PreparedStatement pst = con.prepareStatement(insertSQL);

            pst.setString(1, fileLocation.getBucket());
            pst.setString(2, fileLocation.getName());
            pst.setString(3, "mu");
            pst.setString(4, JsonUtils.toJSONString(meta));
            pst.setLong(5, storeRequest.getLocalFile().length());
            pst.setInt(6, SwitchableStatus.ENABLE.getV());
            pst.setBlob(7, new BufferedInputStream(Files.newInputStream(storeRequest.getLocalFile().toPath())));
            pst.setString(8, null);
            pst.setDate(9, date);
            pst.setDate(10, date);

            pst.execute();

            log.info("[OracleDfsService] store [{}] successfully, cost: {}", fileLocation, sw);

        } catch (Exception e) {
            log.error("[OracleDfsService] store [{}] failed!", fileLocation);
            ExceptionUtils.rethrow(e);
        }
    }

    @Override
    public void download(DownloadRequest downloadRequest) throws IOException {

        Stopwatch sw = Stopwatch.createStarted();
        String querySQL = fullSQL(QUERY_FULL_SQL);

        FileLocation fileLocation = downloadRequest.getFileLocation();

        FileUtils.forceMkdirParent(downloadRequest.getTarget());

        try (Connection con = dataSource.getConnection()) {

            ResultSet resultSet = con.createStatement().executeQuery(querySQL.concat(whereSQL(fileLocation)));

            boolean exist = resultSet.next();

            if (!exist) {
                log.warn("[OracleDfsService] download file[{}] failed due to not exits!", fileLocation);
                return;
            }

            Blob dataBlob = resultSet.getBlob("data");
            FileUtils.copyInputStreamToFile(new BufferedInputStream(dataBlob.getBinaryStream()), downloadRequest.getTarget());

            log.info("[OracleDfsService] download [{}] successfully, cost: {}", fileLocation, sw);

        }  catch (Exception e) {
            log.error("[OracleDfsService] download file [{}] failed!", fileLocation, e);
            ExceptionUtils.rethrow(e);
        }

    }

    @Override
    public Optional<FileMeta> fetchFileMeta(FileLocation fileLocation) throws IOException {

        String querySQL = fullSQL(QUERY_META_SQL);

        try (Connection con = dataSource.getConnection()) {

            ResultSet resultSet = con.createStatement().executeQuery(querySQL.concat(whereSQL(fileLocation)));

            boolean exist = resultSet.next();

            if (!exist) {
                return Optional.empty();
            }

            FileMeta fileMeta = new FileMeta()
                    .setLength(resultSet.getLong("length"))
                    .setLastModifiedTime(resultSet.getDate("gmt_modified"))
                    .setMetaInfo(JsonUtils.parseMap(resultSet.getString("meta")));
            return Optional.of(fileMeta);

        }  catch (Exception e) {
            log.error("[OracleDfsService] fetchFileMeta [{}] failed!", fileLocation);
            ExceptionUtils.rethrow(e);
        }

        return Optional.empty();
    }

    @Override
    public void cleanExpiredFiles(String bucket, int days) {

        // 虽然官方提供了服务端删除的能力，依然强烈建议用户直接在数据库层面配置清理事件！！！

        String dSQLPrefix = fullSQL(DELETE_SQL);
        final long targetTs = DateUtils.addDays(new Date(System.currentTimeMillis()), -days).getTime();
        final String targetDeleteTime = CommonUtils.formatTime(targetTs);
        log.info("[OracleDfsService] start to cleanExpiredFiles, targetDeleteTime: {}", targetDeleteTime);
        String fSQL = dSQLPrefix.concat(String.format(" where gmt_modified < '%s'", targetDeleteTime));
        log.info("[OracleDfsService] cleanExpiredFiles SQL: {}", fSQL);
        executeDelete(fSQL);
    }

    @Override
    protected void init(ApplicationContext applicationContext) {

        Environment env = applicationContext.getEnvironment();

        OracleProperty oracleProperty = new OracleProperty()
                .setDriver(fetchProperty(env, TYPE_ORACLE, KEY_DRIVER_NAME))
                .setUrl(fetchProperty(env, TYPE_ORACLE, KEY_URL))
                .setUsername(fetchProperty(env, TYPE_ORACLE, KEY_USERNAME))
                .setPassword(fetchProperty(env, TYPE_ORACLE, KEY_PASSWORD))
                .setAutoCreateTable(Boolean.TRUE.toString().equalsIgnoreCase(fetchProperty(env, TYPE_ORACLE, KEY_AUTO_CREATE_TABLE)))
                ;

        try {
            initDatabase(oracleProperty);
            initTable(oracleProperty);
        } catch (Exception e) {
            log.error("[OracleDfsService] init datasource failed!", e);
            ExceptionUtils.rethrow(e);
        }

        log.info("[OracleDfsService] initialize successfully, THIS_WILL_BE_THE_STORAGE_LAYER.");
    }

    void initDatabase(OracleProperty property) {

        log.info("[OracleDfsService] init datasource by config: {}", property);

        HikariConfig config = new HikariConfig();

        config.setDriverClassName(property.driver);
        config.setJdbcUrl(property.url);
        config.setUsername(property.username);
        config.setPassword(property.password);

        config.setAutoCommit(true);
        // 池中最小空闲连接数量
        config.setMinimumIdle(2);
        // 池中最大连接数量
        config.setMaximumPoolSize(32);

        dataSource = new HikariDataSource(config);
    }

    void initTable(OracleProperty property) throws Exception {

        if (property.autoCreateTable) {

            String createTableSQL = fullSQL(CREATE_TABLE_SQL);

            log.info("[OracleDfsService] use create table SQL: {}", createTableSQL);
            try (Connection connection = dataSource.getConnection()) {
                connection.createStatement().execute(createTableSQL);
                log.info("[OracleDfsService] auto create table successfully!");
            }
        }
    }

    private String fullSQL(String sql) {
        return String.format(sql, parseTableName());
    }

    private String parseTableName() {
        // 误删，兼容本地 unit test
        if (applicationContext == null) {
            return DEFAULT_TABLE_NAME;
        }
        String tableName = fetchProperty(applicationContext.getEnvironment(), TYPE_ORACLE, KEY_TABLE_NAME);
        return StringUtils.isEmpty(tableName) ? DEFAULT_TABLE_NAME : tableName;
    }

    private static String whereSQL(FileLocation fileLocation) {
        return String.format(" where bucket='%s' AND name='%s' ", fileLocation.getBucket(), fileLocation.getName());
    }

    @Override
    public void destroy() throws Exception {
    }

    @Data
    @Accessors(chain = true)
    static class OracleProperty {
        private String driver;
        private String url;
        private String username;
        private String password;

        private boolean autoCreateTable;
    }

    public static class OracleCondition extends PropertyAndOneBeanCondition {
        @Override
        protected List<String> anyConfigKey() {
            return Lists.newArrayList("oms.storage.dfs.oracle.url");
        }

        @Override
        protected Class<?> beanType() {
            return DFsService.class;
        }
    }
}
