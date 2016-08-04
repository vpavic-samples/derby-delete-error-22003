package sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PlainJdbcTest {

	private Connection conn;

	@Before
	public void setUp() throws Exception {
		Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
		this.conn = DriverManager.getConnection("jdbc:derby:memory:test;create=true");
		createTable();
	}

	@After
	public void tearDown() throws Exception {
		dropTable();
		if (this.conn != null) {
			this.conn.close();
		}
	}

	@Test
	public void delete() throws Exception {
		PreparedStatement delete = this.conn.prepareStatement(
				"delete from test " +
				"where big_number < ? - small_number * 1000");
		delete.setLong(1, System.currentTimeMillis());
		delete.executeUpdate();
		delete.close();
	}

	private void createTable() throws SQLException {
		PreparedStatement createTable = this.conn.prepareStatement(
				"create table test (" +
				"  id bigint primary key, " +
				"  big_number bigint not null, " +
				"  small_number int not null " +
				")");
		createTable.execute();
		createTable.close();
	}

	private void dropTable() throws SQLException {
		PreparedStatement dropTable = this.conn.prepareStatement(
				"drop table test");
		dropTable.execute();
		dropTable.close();
	}

}
