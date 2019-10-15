
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class MainConverter {

	public static void main(String[] args) throws Exception {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:mysql://"+args[0]+":3306/lportal");
		config.setUsername(args[1]);
		config.setPassword(args[2]);
		config.addDataSourceProperty("cachePrepStmts", "true");
		config.addDataSourceProperty("prepStmtCacheSize", "250");
		config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

		HikariDataSource ds = new HikariDataSource(config);
		Connection connection = ds.getConnection();
		DatabaseMetaData databaseMetaData = connection.getMetaData();

		ResultSet rsTables = databaseMetaData.getTables(null, null, null, new String[] { "Table" });

		while (rsTables.next()) {
			String tableName = rsTables.getString("TABLE_NAME");
			String originalTableName = tableName;
			String lowercaseTableName = originalTableName.toLowerCase();

			if (lowercaseTableName.equals(originalTableName)) {
				System.out.println("Skipping the processing of the table: " + originalTableName);
				continue;
			}

			boolean exist = true;
			try {
				Statement statement = connection.createStatement();
				statement.execute("select count(*) from " + lowercaseTableName);
			} catch (Exception e) {
				System.out.println(e.getMessage());
				exist = false;
			}

			if (!exist) {
				System.out.println("Skipping the processing of the table: " + originalTableName + " because it was already imported!");
				continue;
			}

			Statement statement = connection.createStatement();
			int rows = statement.executeUpdate("delete from " + originalTableName);

			System.out.println("Deleted " + rows + " rows from table " + originalTableName);

			StringBuilder builder1 = new StringBuilder();
			StringBuilder builder2 = new StringBuilder();
			ResultSet rsColumns = databaseMetaData.getColumns(null, null, tableName, null);

			builder1.append(" (");

			while (rsColumns.next()) {
				String columnName = rsColumns.getString("COLUMN_NAME");

				builder1.append(columnName);
				builder2.append(columnName);

				if (!rsColumns.isLast()) {
					builder1.append(",");
					builder2.append(",");
				}

			}

			builder1.append(") ");

			String sql = "insert ignore into lportal." + originalTableName + builder1.toString() + " select "
					+ builder2.toString() + " from lportal." + lowercaseTableName;

			statement = connection.createStatement();
			rows = statement.executeUpdate(sql);

			System.out.println("Copied " + rows + " rows from table " + tableName);

			statement = connection.createStatement();
			statement.executeUpdate("drop table " + lowercaseTableName);

		}

		ds.close();
	}

}
