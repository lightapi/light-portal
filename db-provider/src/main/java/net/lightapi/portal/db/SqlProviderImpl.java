package net.lightapi.portal.db;

import com.networknt.monad.Result;

public class PostgresProviderImpl implements DbProvider {


    @Override
    public Result<String> queryUserByEmail(String email) {
        Result<String> result = null;
        String sql =
                "SELECT c.sid, c.pkey, c.pvalue, c.porder, c.source, c.scope FROM \n" +
                        "(\n" +
                        "SELECT s.sid, p.pkey AS pkey, g.pvalue AS pvalue, p.porder AS porder, 'global' AS source, p.scope AS scope FROM prop p LEFT JOIN glob g ON p.pkey = g.pkey LEFT JOIN serv s \n" +
                        " ON s.host = g.host \n" +
                        " AND s.host = g.host \n" +
                        " AND s.cap = g.cap \n" +
                        " AND (s.project = g.project OR g.project = '') \n" +
                        " AND (s.projver = g.projver OR g.projver = '') \n" +
                        " AND (s.service = g.service OR g.service = '')\n" +
                        " AND (s.servver = g.servver OR g.servver = '')\n" +
                        " AND (s.env = g.env OR g.env = '')\n" +
                        " AND s.sid = ? \n" +
                        "UNION\n" +
                        "SELECT sp.sid, sp.pkey AS pkey, sp.pvalue AS pvalue, p.porder AS porder, 'custom' AS source, p.scope AS scope FROM serv_prop sp, prop p WHERE sp.pkey = p.pkey AND sp.sid = ?\n" +
                        ") AS c\n" +
                        "ORDER BY c.scope, c.porder\n";
        try (final Connection conn = ds.getConnection()) {
            List<Map<String, Object>> list = new ArrayList<>();
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setInt(1, sid);
                statement.setInt(2, sid);
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Map<String, Object> map = new HashMap();
                        map.put("sid", resultSet.getInt("sid"));
                        map.put("key", resultSet.getString("pkey"));
                        map.put("value", resultSet.getString("pvalue"));
                        map.put("order", resultSet.getInt("porder"));
                        map.put("source", resultSet.getString("source"));
                        map.put("scope", resultSet.getString("scope"));
                        list.add(map);
                    }
                }
            }
            result = Success.of(JsonMapper.toJson(list));
        } catch (SQLException e) {
            logger.error("SQLException:", e);
            result = Failure.of(new Status(SQL_EXCEPTION, e.getMessage()));
        } catch (Exception e) {
            logger.error("Exception:", e);
            result = Failure.of(new Status(GENERIC_EXCEPTION, e.getMessage()));
        }
        return result;
    }

    @Override
    public Result<String> queryUserById(String id) {
        return null;
    }
}
