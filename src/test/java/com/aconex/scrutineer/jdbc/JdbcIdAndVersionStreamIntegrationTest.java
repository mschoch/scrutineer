package com.aconex.scrutineer.jdbc;

import static com.aconex.scrutineer.HasIdAndVersionMatcher.hasIdAndVersion;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Iterator;

import javax.sql.DataSource;

import org.dbunit.DataSourceBasedDBTestCase;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.XmlDataSet;

import com.aconex.scrutineer.IdAndVersion;


public class JdbcIdAndVersionStreamIntegrationTest extends DataSourceBasedDBTestCase {

    private static final String SQL = "Select id, version from test";
    private final HSQLHelper HSQLHelper = new HSQLHelper();


    public void testShouldReturnTuplesInCorrectOrder() throws SQLException {

        JdbcIdAndVersionStream jdbcIdAndVersionStream = new JdbcIdAndVersionStream(getDataSource().getConnection(), SQL);

        jdbcIdAndVersionStream.open();

        Iterator<IdAndVersion> iterator = jdbcIdAndVersionStream.iterator();

        assertThat(iterator.next(), hasIdAndVersion("1", "10"));
        assertThat(iterator.next(), hasIdAndVersion("2", "20"));
        assertThat(iterator.next(), hasIdAndVersion("3", "30"));

        jdbcIdAndVersionStream.close();

    }

    @Override
    protected void setUp() throws Exception {
        HSQLHelper.createHsqldbTables(getDataSet(), getDataSource().getConnection());
        super.setUp();

    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        getDataSource().getConnection().createStatement().execute("SHUTDOWN");
    }

    @Override
    protected DataSource getDataSource() {
        return HSQLHelper.setupHSQLDBDataSource();
    }


    @Override
    protected IDataSet getDataSet() throws Exception {
        InputStream resourceAsStream = this.getClass().getResourceAsStream("dataset.xml");
        return new XmlDataSet(resourceAsStream);
    }


    private DataSource setupHSQLDBDataSource() {
        return HSQLHelper.setupHSQLDBDataSource();
    }

}
